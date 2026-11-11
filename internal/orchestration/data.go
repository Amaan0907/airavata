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

package orchestration

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/google/uuid"

	"github.com/apache/airavata/internal/objectstore"
	internalptr "github.com/apache/airavata/internal/ptr"

	datamodel "github.com/apache/airavata/api/data/model"
	processmodel "github.com/apache/airavata/api/process/model"
)

type FileMetadata struct {
	Path string
	Size int64
	Mode os.FileMode
}

// copyData carries out one staging task: it fetches the source into a local temporary
// file and delivers that file to the destination.
//
// Every copy goes through this server, whatever the two ends are. That is what keeps an
// S3 credential away from the cluster: S3 data reaches a cluster as an SCP upload from
// here, and a cluster's output reaches S3 the same way in reverse.
func (a *ExecutionEngine) copyData(ctx context.Context, executionContext *ExecutionContext, processId string, dataStagingTaskId string) (*ExecutionContext, error) {
	dataStagingTask, err := a.dataStagingTasks.FindByIDAndProcessID(ctx, dataStagingTaskId, processId)
	if err != nil {
		return nil, err
	}
	if dataStagingTask.SourceDataStorageType == nil || dataStagingTask.SourceDataStorageID == nil || dataStagingTask.SourcePath == nil ||
		dataStagingTask.DestinationDataStorageType == nil || dataStagingTask.DestinationDataStorageID == nil || dataStagingTask.DestinationPath == nil {
		return nil, fmt.Errorf("data staging task %s does not name a storage and path at both ends", dataStagingTask.ID)
	}
	slog.Info("Starting data staging task", "taskId", dataStagingTask.ID, "processId", dataStagingTask.ProcessID)

	// os.TempDir rather than a literal /tmp, so TMPDIR is honoured and the path exists
	// on every platform.
	localTempDataPath := filepath.Join(os.TempDir(), dataStagingTask.ID+"-"+uuid.New().String())
	defer os.RemoveAll(localTempDataPath) // Make sure to clean up the temporary data path after the function completes

	if err := a.fetchSource(ctx, dataStagingTask, localTempDataPath); err != nil {
		return nil, err
	}
	if err := a.deliverDestination(ctx, dataStagingTask, localTempDataPath); err != nil {
		return nil, err
	}
	return executionContext, nil
}

// fetchSource copies a staging task's source to localPath.
func (a *ExecutionEngine) fetchSource(ctx context.Context, dataStagingTask *processmodel.DataStagingTask, localTempDataPath string) error {
	sourceID, sourcePath := *dataStagingTask.SourceDataStorageID, *dataStagingTask.SourcePath

	switch *dataStagingTask.SourceDataStorageType {
	case datamodel.DataStorageTypeSCP:
		s, err := a.scpStorages.FindByID(ctx, sourceID)
		if err != nil {
			return err
		}

		err = DownloadFileFromSCP(ctx, *s.HostName, *s.Port, *s.LoginUser, *s.SSHKey,
			sourcePath, localTempDataPath)
		if err != nil {
			slog.Error("Failed to download file through SCP", "taskId", dataStagingTask.ID, "processId", *dataStagingTask.ProcessID,
				"sourcePath", sourcePath, "destinationPath", localTempDataPath, "error", err)
			return err
		}
		slog.Info("Successfully downloaded file through SCP", "taskId", dataStagingTask.ID, "processId", *dataStagingTask.ProcessID,
			"sourcePath", sourcePath, "destinationPath", localTempDataPath)

	case datamodel.DataStorageTypeHPC:
		s, err := a.slurmClusterConfigs.FindByID(ctx, sourceID)
		if err != nil {
			return err
		}

		err = DownloadFileFromSCP(ctx, s.SlurmCluster.HeadnodeHost, s.SlurmCluster.HeadnodePort, s.LoginUser,
			*s.SSHKey, sourcePath, localTempDataPath)
		if err != nil {
			slog.Error("Failed to download file from HPC", "taskId", dataStagingTask.ID, "processId", *dataStagingTask.ProcessID,
				"sourcePath", sourcePath, "destinationPath", localTempDataPath, "error", err)
			return err
		}
		slog.Info("Successfully downloaded file from HPC", "taskId", dataStagingTask.ID, "processId", *dataStagingTask.ProcessID,
			"sourcePath", sourcePath, "destinationPath", localTempDataPath)

	case datamodel.DataStorageTypeS3:
		store, err := a.objectStoreFor(ctx, sourceID)
		if err != nil {
			return err
		}
		if err := store.Download(ctx, sourcePath, localTempDataPath); err != nil {
			slog.Error("Failed to download object from S3", "taskId", dataStagingTask.ID, "processId", *dataStagingTask.ProcessID,
				"storageId", sourceID, "key", sourcePath, "error", err)
			return err
		}
		slog.Info("Successfully downloaded object from S3", "taskId", dataStagingTask.ID, "processId", *dataStagingTask.ProcessID,
			"storageId", sourceID, "key", sourcePath, "destinationPath", localTempDataPath)

	default:
		return fmt.Errorf("data staging task %s: cannot stage from %q storage", dataStagingTask.ID, *dataStagingTask.SourceDataStorageType)
	}
	return nil
}

// deliverDestination copies localPath to a staging task's destination.
func (a *ExecutionEngine) deliverDestination(ctx context.Context, dataStagingTask *processmodel.DataStagingTask, localTempDataPath string) error {
	destinationID, destinationPath := *dataStagingTask.DestinationDataStorageID, *dataStagingTask.DestinationPath

	switch *dataStagingTask.DestinationDataStorageType {
	case datamodel.DataStorageTypeSCP:
		s, err := a.scpStorages.FindByID(ctx, destinationID)
		if err != nil {
			return err
		}

		// Ensure the destination directory exists before attempting the SCP upload.
		if err := CreateDirectorySSH(ctx, *s.HostName, *s.Port, *s.LoginUser, *s.SSHKey, filepath.Dir(destinationPath)); err != nil {
			slog.Error("Failed to create destination directory through SSH", "taskId", dataStagingTask.ID, "processId", *dataStagingTask.ProcessID,
				"destinationDirectory", filepath.Dir(destinationPath), "error", err)
			return err
		}

		err = UploadFileToSCP(ctx, *s.HostName, *s.Port, *s.LoginUser, *s.SSHKey, localTempDataPath, destinationPath)
		if err != nil {
			slog.Error("Failed to upload file through SCP", "taskId", dataStagingTask.ID, "processId", *dataStagingTask.ProcessID,
				"sourcePath", localTempDataPath, "destinationPath", destinationPath, "error", err)
			return err
		}
		slog.Info("Successfully uploaded file through SCP", "taskId", dataStagingTask.ID, "processId", *dataStagingTask.ProcessID,
			"sourcePath", localTempDataPath, "destinationPath", destinationPath)

	case datamodel.DataStorageTypeHPC:
		s, err := a.slurmClusterConfigs.FindByID(ctx, destinationID)
		if err != nil {
			return err
		}

		// Ensure the destination directory exists before attempting the SCP upload.
		if err := CreateDirectorySSH(ctx, s.SlurmCluster.HeadnodeHost, s.SlurmCluster.HeadnodePort, s.LoginUser, *s.SSHKey,
			filepath.Dir(destinationPath)); err != nil {
			slog.Error("Failed to create destination directory on HPC through SSH", "taskId", dataStagingTask.ID, "processId", *dataStagingTask.ProcessID,
				"destinationDirectory", filepath.Dir(destinationPath), "error", err)
			return err
		}

		err = UploadFileToSCP(ctx, s.SlurmCluster.HeadnodeHost, s.SlurmCluster.HeadnodePort, s.LoginUser,
			*s.SSHKey, localTempDataPath, destinationPath)
		if err != nil {
			slog.Error("Failed to upload file to HPC", "taskId", dataStagingTask.ID, "processId", *dataStagingTask.ProcessID,
				"sourcePath", localTempDataPath, "destinationPath", destinationPath, "error", err)
			return err
		}
		slog.Info("Successfully uploaded file to HPC", "taskId", dataStagingTask.ID, "processId", *dataStagingTask.ProcessID,
			"sourcePath", localTempDataPath, "destinationPath", destinationPath)

	case datamodel.DataStorageTypeS3:
		// An object key has no directories to create: the key is the whole name.
		store, err := a.objectStoreFor(ctx, destinationID)
		if err != nil {
			return err
		}
		if err := store.Upload(ctx, localTempDataPath, destinationPath); err != nil {
			slog.Error("Failed to upload object to S3", "taskId", dataStagingTask.ID, "processId", *dataStagingTask.ProcessID,
				"storageId", destinationID, "key", destinationPath, "error", err)
			return err
		}
		slog.Info("Successfully uploaded object to S3", "taskId", dataStagingTask.ID, "processId", *dataStagingTask.ProcessID,
			"storageId", destinationID, "key", destinationPath)

	default:
		return fmt.Errorf("data staging task %s: cannot stage to %q storage", dataStagingTask.ID, *dataStagingTask.DestinationDataStorageType)
	}
	return nil
}

// objectStoreFor opens the bucket an S3 data storage names, under the credential it
// presents. The credential is read here, on the server, and goes no further than the
// client: nothing logged above names more than the storage id and the key.
func (a *ExecutionEngine) objectStoreFor(ctx context.Context, storageID string) (objectstore.Store, error) {
	storage, err := a.s3Storages.FindByID(ctx, storageID)
	if err != nil {
		return nil, fmt.Errorf("loading S3 data storage %s: %w", storageID, err)
	}
	cfg, err := s3StoreConfig(storage)
	if err != nil {
		return nil, err
	}
	return a.openObjectStore(cfg)
}

// s3StoreConfig is the client configuration for a storage and its credential.
func s3StoreConfig(storage *datamodel.S3DataStorage) (objectstore.Config, error) {
	cred := storage.S3Credential
	if cred == nil {
		return objectstore.Config{}, fmt.Errorf("S3 data storage %s names no credential", storage.ID)
	}
	return objectstore.Config{
		EndpointURL:     internalptr.From(storage.EndpointURL),
		Region:          storage.Region,
		Bucket:          storage.BucketName,
		PathStyle:       storage.PathStyle,
		AccessKeyID:     cred.AccessKeyID,
		SecretAccessKey: cred.SecretAccessKey,
		SessionToken:    internalptr.From(cred.SessionToken),
	}, nil
}

// openS3Store is the store an engine opens unless a test replaces it.
func openS3Store(cfg objectstore.Config) (objectstore.Store, error) {
	return objectstore.New(cfg)
}
