/**
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
 */

package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"

	"gorm.io/gorm"

	"github.com/apache/airavata/internal/httpx"
	"github.com/apache/airavata/internal/objectstore"
	"github.com/apache/airavata/internal/orchestration"

	credmodel "github.com/apache/airavata/api/credentials/model"
	dto "github.com/apache/airavata/api/data/dto"
	model "github.com/apache/airavata/api/data/model"
	"github.com/apache/airavata/api/data/repository"
	iamrepo "github.com/apache/airavata/api/iam/repository"
	sharingmodel "github.com/apache/airavata/api/sharing/model"
	sharingrepo "github.com/apache/airavata/api/sharing/repository"
	sharingsvc "github.com/apache/airavata/api/sharing/service"
)

// Endpoint is the host and account one transfer runs against.
type Endpoint struct {
	Host      string
	Port      int
	LoginUser string
	Key       credmodel.SSHKey
}

// Transport moves a single file between this server and the host an Endpoint names.
type Transport interface {
	Download(ctx context.Context, ep Endpoint, remotePath, localPath string) error
	Upload(ctx context.Context, ep Endpoint, localPath, remotePath string) error
	MakeDirectory(ctx context.Context, ep Endpoint, remoteDir string) error
}

// ObjectStoreOpener opens the bucket a configuration names. It is the S3 counterpart of
// Transport: the server opens it with the storage's credential, and the bytes pass
// through here, so a caller never holds the credential or a presigned URL.
type ObjectStoreOpener func(objectstore.Config) (objectstore.Store, error)

// Download is the handle a caller streams a product's bytes from.
type Download struct {
	FileName string
	Size     int64

	io.ReadCloser
}

type stagedFile struct {
	*os.File
	dir string
}

func (f *stagedFile) Close() error {
	err := f.File.Close()
	if rmErr := os.RemoveAll(f.dir); err == nil {
		err = rmErr
	}
	return err
}

// location is one place a product's bytes are read from or written to.
type location struct {
	storageType model.DataStorageType
	storageID   string

	// path is a file path on an SCP storage and an object key on an S3 one.
	path string
}

type DataTransferService struct {
	productAccess
	db         *gorm.DB
	storages   *repository.SCPDataStorageRepository
	s3Storages *repository.S3DataStorageRepository
	transport  Transport
	openStore  ObjectStoreOpener
}

// NewDataTransferService returns a transfer service moving bytes through transport for
// SCP storages and through the stores openStore opens for S3 ones.
func NewDataTransferService(
	db *gorm.DB,
	products *repository.DataProductRepository,
	sharing *sharingrepo.Repository,
	storages *repository.SCPDataStorageRepository,
	s3Storages *repository.S3DataStorageRepository,
	members *iamrepo.GroupMemberRepository,
	transport Transport,
	openStore ObjectStoreOpener,
) *DataTransferService {
	return &DataTransferService{
		productAccess: productAccess{
			access:   sharingsvc.NewAccess(members),
			products: products,
			sharing:  sharing,
		},
		db:         db,
		storages:   storages,
		s3Storages: s3Storages,
		transport:  transport,
		openStore:  openStore,
	}
}

// Download stages the product's file locally and returns a handle to it.
func (s *DataTransferService) Download(ctx context.Context, id string) (*Download, error) {
	product, err := s.requireProduct(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := s.require(ctx, product, sharingmodel.AccessPermissionRead); err != nil {
		return nil, err
	}
	from, err := downloadLocation(product)
	if err != nil {
		return nil, err
	}

	dir, err := os.MkdirTemp("", "airavata-download-")
	if err != nil {
		return nil, fmt.Errorf("creating staging directory for data product %s: %w", product.ID, err)
	}
	// Everything from here to the handed-back handle has to clean the directory up
	// itself: nothing else knows about it yet.
	staged := filepath.Join(dir, "content")

	if err := s.fetch(ctx, product, from, staged); err != nil {
		os.RemoveAll(dir)
		return nil, err
	}

	file, err := os.Open(staged)
	if err != nil {
		os.RemoveAll(dir)
		return nil, fmt.Errorf("opening staged copy of data product %s: %w", product.ID, err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		os.RemoveAll(dir)
		return nil, fmt.Errorf("sizing staged copy of data product %s: %w", product.ID, err)
	}

	return &Download{
		FileName:   path.Base(from.path),
		Size:       info.Size(),
		ReadCloser: &stagedFile{File: file, dir: dir},
	}, nil
}

// Upload writes content to the product's primary location, replacing whatever was there.
func (s *DataTransferService) Upload(ctx context.Context, id string, content io.Reader) (*dto.DataProductResponse, error) {
	product, err := s.requireProduct(ctx, id)
	if err != nil {
		return nil, err
	}
	held, _, err := s.require(ctx, product, sharingmodel.AccessPermissionWrite)
	if err != nil {
		return nil, err
	}
	to, err := uploadLocation(product)
	if err != nil {
		return nil, err
	}

	staged, cleanup, err := stage(content, product.ID)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	if err := s.deliver(ctx, product, to, staged); err != nil {
		return nil, err
	}

	provisioned := model.ProvisionStatusProvisioned
	product.ProvisionStatus = &provisioned
	if err := s.products.Save(ctx, product); err != nil {
		return nil, err
	}

	out := dto.ToDataProductResponseWith(product, string(held))
	return &out, nil
}

// fetch copies the file at from to localPath.
func (s *DataTransferService) fetch(ctx context.Context, product *model.DataProduct, from location, localPath string) error {
	switch from.storageType {
	case model.DataStorageTypeSCP:
		ep, err := s.scpEndpoint(ctx, from.storageID)
		if err != nil {
			return err
		}
		if err := s.transport.Download(ctx, ep, from.path, localPath); err != nil {
			return upstream(err, "Could not read data product %s from %s", product.ID, ep.Host)
		}
		return nil

	case model.DataStorageTypeS3:
		store, err := s.objectStore(ctx, from.storageID)
		if err != nil {
			return err
		}
		if err := store.Download(ctx, from.path, localPath); err != nil {
			// Unlike SCP, S3 says plainly when the object is missing: the product was
			// registered but nothing has been written to it yet.
			if errors.Is(err, objectstore.ErrNotFound) {
				return httpx.NotFound("Data product %s has no content at its S3 location yet", product.ID)
			}
			return upstream(err, "Could not read data product %s from S3 data storage %s", product.ID, from.storageID)
		}
		return nil

	default:
		return unsupportedStorage(product, from.storageType)
	}
}

// deliver copies localPath to the file at to.
func (s *DataTransferService) deliver(ctx context.Context, product *model.DataProduct, to location, localPath string) error {
	switch to.storageType {
	case model.DataStorageTypeSCP:
		ep, err := s.scpEndpoint(ctx, to.storageID)
		if err != nil {
			return err
		}
		// The parent directory is created first for the same reason a staging task
		// creates it: a product may name a path under a directory that no run has made
		// yet, and scp will not make it.
		if remoteDir := path.Dir(to.path); remoteDir != "." && remoteDir != "/" {
			if err := s.transport.MakeDirectory(ctx, ep, remoteDir); err != nil {
				return upstream(err, "Could not create %s on %s for data product %s", remoteDir, ep.Host, product.ID)
			}
		}
		if err := s.transport.Upload(ctx, ep, localPath, to.path); err != nil {
			return upstream(err, "Could not write data product %s to %s", product.ID, ep.Host)
		}
		return nil

	case model.DataStorageTypeS3:
		// A bucket has no directories to make: the key's prefix is part of its name.
		store, err := s.objectStore(ctx, to.storageID)
		if err != nil {
			return err
		}
		if err := store.Upload(ctx, localPath, to.path); err != nil {
			return upstream(err, "Could not write data product %s to S3 data storage %s", product.ID, to.storageID)
		}
		return nil

	default:
		return unsupportedStorage(product, to.storageType)
	}
}

// stage writes content to a temporary file and returns its path with the function that
// removes it.
func stage(content io.Reader, productID string) (string, func(), error) {
	file, err := os.CreateTemp("", "airavata-upload-")
	if err != nil {
		return "", nil, fmt.Errorf("creating staging file for data product %s: %w", productID, err)
	}
	name := file.Name()
	cleanup := func() {
		file.Close()
		os.Remove(name)
	}

	if _, err := io.Copy(file, content); err != nil {
		cleanup()
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return "", nil, &httpx.Error{
				Status:  http.StatusRequestEntityTooLarge,
				Message: fmt.Sprintf("Uploaded content exceeds the %d byte limit", tooLarge.Limit),
				Err:     err,
			}
		}
		return "", nil, httpx.BadRequest("Could not read the uploaded content: %v", err)
	}
	if err := file.Close(); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("closing staging file for data product %s: %w", productID, err)
	}
	return name, cleanup, nil
}

// requireFile refuses a directory product. A directory moves as a tree, which neither
// single-file SCP nor a single S3 object does, and refusing it beats transferring the
// one file a recursive copy would have started with.
func requireFile(product *model.DataProduct) error {
	if !product.IsFile {
		return httpx.BadRequest(
			"Data product %s is a directory: only a file data product can be uploaded or downloaded", product.ID)
	}
	return nil
}

// downloadLocation is where a product's bytes are read from: its first replica, which is
// the primary when there is one and the oldest other replica when there is not. It is
// the same choice a job launch makes when it stages the product as an input.
func downloadLocation(product *model.DataProduct) (location, error) {
	if err := requireFile(product); err != nil {
		return location{}, err
	}
	if len(product.Replicas) == 0 {
		return location{}, httpx.Conflict("Data product %s names no location to transfer from", product.ID)
	}
	replica := product.Replicas[0]
	return location{storageType: replica.DataStorageType, storageID: replica.DataStorageID, path: replica.Path}, nil
}

// uploadLocation is where new bytes for a product are written: its primary location
// only. Other replicas are copies the caller registered, and are not rewritten here.
func uploadLocation(product *model.DataProduct) (location, error) {
	if err := requireFile(product); err != nil {
		return location{}, err
	}
	if product.Path == nil || *product.Path == "" {
		return location{}, httpx.Conflict("Data product %s names no path to transfer", product.ID)
	}
	if product.DataStorageID == nil {
		return location{}, httpx.Conflict("Data product %s names no storage to transfer through", product.ID)
	}
	// A product registered before the type was recorded is on SCP, the only kind there was.
	storageType := product.DataStorageType
	if storageType == "" {
		storageType = model.DataStorageTypeSCP
	}
	return location{storageType: storageType, storageID: *product.DataStorageID, path: *product.Path}, nil
}

func unsupportedStorage(product *model.DataProduct, storageType model.DataStorageType) error {
	return httpx.BadRequest(
		"Data product %s lives on %s storage, which cannot be transferred through this API",
		product.ID, storageType)
}

// scpEndpoint is the host and account an SCP storage is reached through.
func (s *DataTransferService) scpEndpoint(ctx context.Context, storageID string) (Endpoint, error) {
	var ep Endpoint
	storage, err := s.storages.FindByID(ctx, storageID)
	if err != nil {
		return ep, notFoundAs(err, "SCP data storage not found: %s", storageID)
	}
	if storage.HostName == nil || storage.LoginUser == nil || storage.SSHKey == nil {
		return ep, httpx.Conflict(
			"SCP data storage %s is missing the host, login user or key a transfer needs", storage.ID)
	}

	port := 22
	if storage.Port != nil && *storage.Port != 0 {
		port = *storage.Port
	}
	return Endpoint{
		Host:      *storage.HostName,
		Port:      port,
		LoginUser: *storage.LoginUser,
		Key:       *storage.SSHKey,
	}, nil
}

// objectStore opens the bucket an S3 storage names, under the credential it presents.
// As with an SCP storage's key, a share on the product is enough: the caller is never
// given the storage or its credential, only the one object the product names.
func (s *DataTransferService) objectStore(ctx context.Context, storageID string) (objectstore.Store, error) {
	storage, err := s.s3Storages.FindByID(ctx, storageID)
	if err != nil {
		return nil, notFoundAs(err, "S3 data storage not found: %s", storageID)
	}
	return openS3Storage(s.openStore, storage)
}

// openS3Storage opens a loaded storage's bucket under its credential. The credential
// goes no further than the client open returns.
func openS3Storage(open ObjectStoreOpener, storage *model.S3DataStorage) (objectstore.Store, error) {
	cfg, err := orchestration.S3StoreConfig(storage)
	if err != nil {
		return nil, httpx.Conflict("S3 data storage %s is missing the credential it needs", storage.ID)
	}
	store, err := open(cfg)
	if err != nil {
		return nil, upstream(err, "Could not open S3 data storage %s", storage.ID)
	}
	return store, nil
}

// upstream reports a failure of the host a transfer ran against rather than of the
// request. The cause is wrapped rather than written into the message: an SSH or S3
// failure names internal state, and the handler logs anything at 500 or above.
func upstream(err error, format string, args ...any) *httpx.Error {
	return &httpx.Error{
		Status:  http.StatusBadGateway,
		Message: fmt.Sprintf(format, args...),
		Err:     err,
	}
}
