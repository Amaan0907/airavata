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

	"github.com/apache/airavata/internal/objectstore"

	dto "github.com/apache/airavata/api/data/dto"
	"github.com/apache/airavata/api/data/repository"
	iamrepo "github.com/apache/airavata/api/iam/repository"
	sharingmodel "github.com/apache/airavata/api/sharing/model"
	sharingrepo "github.com/apache/airavata/api/sharing/repository"
	sharingsvc "github.com/apache/airavata/api/sharing/service"
)

// S3ObjectService browses what is in an S3 data storage's bucket, one level at a time.
//
// Nothing is copied into the database: a listing is read from the bucket when asked
// for, through this server and under the storage's credential. An object the caller
// wants to keep track of is registered as a data product at its key, with the data
// product API, and from then on is a product like any other.
type S3ObjectService struct {
	s3StorageAccess
	openStore ObjectStoreOpener
}

// NewS3ObjectService returns a service listing buckets through the stores openStore
// opens.
func NewS3ObjectService(
	storages *repository.S3DataStorageRepository,
	sharing *sharingrepo.Repository,
	members *iamrepo.GroupMemberRepository,
	openStore ObjectStoreOpener,
) *S3ObjectService {
	return &S3ObjectService{
		s3StorageAccess: s3StorageAccess{
			access:   sharingsvc.NewAccess(members),
			storages: storages,
			sharing:  sharing,
		},
		openStore: openStore,
	}
}

// List returns one page of the folders and files directly under q.Prefix.
//
// It takes READ on the storage: seeing what a bucket holds is reading the storage,
// and a share on one product in it is not a share on everything beside it.
func (s *S3ObjectService) List(ctx context.Context, storageID string, q dto.S3ObjectListQuery) (*dto.S3ObjectListingResponse, error) {
	storage, err := s.requireStorage(ctx, storageID)
	if err != nil {
		return nil, err
	}
	if _, err := s.require(ctx, storage, sharingmodel.AccessPermissionRead); err != nil {
		return nil, err
	}
	store, err := openS3Storage(s.openStore, storage)
	if err != nil {
		return nil, err
	}

	page, err := store.List(ctx, objectstore.ListOptions{
		Prefix:            q.Prefix,
		Delimiter:         dto.S3FolderDelimiter,
		ContinuationToken: q.ContinuationToken,
		MaxKeys:           q.MaxKeys,
	})
	if err != nil {
		return nil, upstream(err, "Could not list S3 data storage %s", storage.ID)
	}
	out := dto.ToS3ObjectListingResponse(storage.ID, q.Prefix, page)
	return &out, nil
}
