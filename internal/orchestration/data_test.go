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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/apache/airavata/internal/db"
	"github.com/apache/airavata/internal/objectstore"

	credmodel "github.com/apache/airavata/api/credentials/model"
	datamodel "github.com/apache/airavata/api/data/model"
	datarepo "github.com/apache/airavata/api/data/repository"
	iammodel "github.com/apache/airavata/api/iam/model"
	processmodel "github.com/apache/airavata/api/process/model"
	processrepo "github.com/apache/airavata/api/process/repository"
)

// fakeStores is an in-memory object store per bucket. It stands in for S3 behind the
// engine's openObjectStore, so copyData can be run end to end without a network.
type fakeStores struct {
	mu      sync.Mutex
	buckets map[string]map[string][]byte
	opened  []objectstore.Config
}

func newFakeStores() *fakeStores {
	return &fakeStores{buckets: map[string]map[string][]byte{}}
}

func (f *fakeStores) open(cfg objectstore.Config) (objectstore.Store, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opened = append(f.opened, cfg)
	if f.buckets[cfg.Bucket] == nil {
		f.buckets[cfg.Bucket] = map[string][]byte{}
	}
	return &fakeBucket{f: f, bucket: cfg.Bucket}, nil
}

type fakeBucket struct {
	f      *fakeStores
	bucket string
}

func (b *fakeBucket) Stat(_ context.Context, key string) (*objectstore.ObjectInfo, error) {
	b.f.mu.Lock()
	defer b.f.mu.Unlock()
	content, ok := b.f.buckets[b.bucket][key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return &objectstore.ObjectInfo{Key: key, Size: int64(len(content))}, nil
}

func (b *fakeBucket) Download(_ context.Context, key, localPath string) error {
	b.f.mu.Lock()
	content, ok := b.f.buckets[b.bucket][key]
	b.f.mu.Unlock()
	if !ok {
		return fmt.Errorf("reading %s: %w", key, objectstore.ErrNotFound)
	}
	return os.WriteFile(localPath, content, 0o600)
}

func (b *fakeBucket) Upload(_ context.Context, localPath, key string) error {
	content, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}
	b.f.mu.Lock()
	defer b.f.mu.Unlock()
	b.f.buckets[b.bucket][key] = content
	return nil
}

func (b *fakeBucket) List(context.Context, objectstore.ListOptions) (*objectstore.Listing, error) {
	return &objectstore.Listing{}, nil
}

type stagingFixture struct {
	gdb    *gorm.DB
	engine *ExecutionEngine
	stores *fakeStores
}

func newStagingFixture(t *testing.T) *stagingFixture {
	t.Helper()
	gdb, err := gorm.Open(sqlite.Open("file::memory:?_pragma=foreign_keys(1)"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(gdb); err != nil {
		t.Fatalf("automigrate: %v", err)
	}

	token := "SESSION-TOKEN"
	endpoint := "https://minio.example.edu:9000"
	for _, row := range []any{
		&iammodel.User{ID: "alice", CreatedAt: 1},
		&credmodel.S3Credential{ID: "cred", Name: "c", AccessKeyID: "AKIA-TEST", SecretAccessKey: "SECRET", SessionToken: &token, OwnerID: "alice"},
		&datamodel.S3DataStorage{ID: "src", EndpointURL: &endpoint, Region: "eu-west-2", BucketName: "source-bucket", PathStyle: true, S3CredentialID: strPtr("cred")},
		&datamodel.S3DataStorage{ID: "dst", Region: "us-east-1", BucketName: "dest-bucket", S3CredentialID: strPtr("cred")},
		&datamodel.S3DataStorage{ID: "no-cred", Region: "us-east-1", BucketName: "orphan-bucket"},
		&processmodel.Process{ID: "p1"},
	} {
		if err := gdb.Create(row).Error; err != nil {
			t.Fatalf("seed %T: %v", row, err)
		}
	}

	stores := newFakeStores()
	return &stagingFixture{
		gdb:    gdb,
		stores: stores,
		engine: &ExecutionEngine{
			dataStagingTasks: processrepo.NewDataStagingTaskRepository(gdb),
			s3Storages:       datarepo.NewS3DataStorageRepository(gdb),
			openObjectStore:  stores.open,
		},
	}
}

func strPtr(s string) *string { return &s }

func (f *stagingFixture) task(t *testing.T, id string, srcType datamodel.DataStorageType, srcID, srcPath string, dstType datamodel.DataStorageType, dstID, dstPath string) {
	t.Helper()
	task := &processmodel.DataStagingTask{
		ID: id, ProcessID: strPtr("p1"),
		SourceDataStorageType: &srcType, SourceDataStorageID: &srcID, SourcePath: &srcPath,
		DestinationDataStorageType: &dstType, DestinationDataStorageID: &dstID, DestinationPath: &dstPath,
	}
	if err := f.gdb.Create(task).Error; err != nil {
		t.Fatalf("seed task: %v", err)
	}
}

// tempLeftovers lists what a task left in the temporary directory.
func tempLeftovers(t *testing.T, taskID string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), taskID+"-*"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	return matches
}

// S3 to S3 runs both new branches and the local hop between them, with no SSH involved.
func TestCopyDataBetweenS3Storages(t *testing.T) {
	f := newStagingFixture(t)
	f.stores.buckets["source-bucket"] = map[string][]byte{"in/data.bin": []byte("payload")}
	f.task(t, "s3-to-s3", datamodel.DataStorageTypeS3, "src", "in/data.bin", datamodel.DataStorageTypeS3, "dst", "out/data.bin")

	ec := &ExecutionContext{}
	got, err := f.engine.copyData(context.Background(), ec, "p1", "s3-to-s3")
	if err != nil {
		t.Fatalf("copyData: %v", err)
	}
	if got != ec {
		t.Error("copyData did not hand back the execution context")
	}
	if content := f.stores.buckets["dest-bucket"]["out/data.bin"]; string(content) != "payload" {
		t.Errorf("destination holds %q, want the source bytes", content)
	}
	if left := tempLeftovers(t, "s3-to-s3"); len(left) != 0 {
		t.Errorf("temporary files left behind: %v", left)
	}

	// Each end is opened as its own storage, under the credential it presents.
	if len(f.stores.opened) != 2 {
		t.Fatalf("opened %d stores, want 2", len(f.stores.opened))
	}
	want := objectstore.Config{
		EndpointURL: "https://minio.example.edu:9000", Region: "eu-west-2", Bucket: "source-bucket", PathStyle: true,
		AccessKeyID: "AKIA-TEST", SecretAccessKey: "SECRET", SessionToken: "SESSION-TOKEN",
	}
	if f.stores.opened[0] != want {
		t.Errorf("source config = %+v, want %+v", f.stores.opened[0], want)
	}
	if dst := f.stores.opened[1]; dst.EndpointURL != "" || dst.Bucket != "dest-bucket" || dst.PathStyle {
		t.Errorf("destination config = %+v, want AWS and the destination bucket", dst)
	}
}

// A missing object fails the task, writes nothing at the destination, and leaves no
// temporary file.
func TestCopyDataReportsAMissingObject(t *testing.T) {
	f := newStagingFixture(t)
	f.task(t, "missing", datamodel.DataStorageTypeS3, "src", "in/nothing.bin", datamodel.DataStorageTypeS3, "dst", "out/x.bin")

	_, err := f.engine.copyData(context.Background(), &ExecutionContext{}, "p1", "missing")
	if !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("copyData: err = %v, want ErrNotFound", err)
	}
	if len(f.stores.buckets["dest-bucket"]) != 0 {
		t.Errorf("destination written after a failed fetch: %v", f.stores.buckets["dest-bucket"])
	}
	if left := tempLeftovers(t, "missing"); len(left) != 0 {
		t.Errorf("temporary files left behind: %v", left)
	}
}

// Before this, a type copyData did not handle was skipped and the task reported
// success. Now it fails, at either end.
func TestCopyDataRefusesAnUnknownStorageType(t *testing.T) {
	f := newStagingFixture(t)
	f.stores.buckets["source-bucket"] = map[string][]byte{"in/a": []byte("a")}
	f.task(t, "bad-source", "FTP", "x", "/a", datamodel.DataStorageTypeS3, "dst", "out/a")
	f.task(t, "bad-dest", datamodel.DataStorageTypeS3, "src", "in/a", "FTP", "x", "/a")

	for _, id := range []string{"bad-source", "bad-dest"} {
		_, err := f.engine.copyData(context.Background(), &ExecutionContext{}, "p1", id)
		if err == nil || !strings.Contains(err.Error(), `"FTP"`) {
			t.Errorf("%s: err = %v, want a refusal naming the type", id, err)
		}
	}
}

func TestCopyDataRefusesAStorageWithNoCredential(t *testing.T) {
	f := newStagingFixture(t)
	f.task(t, "no-cred", datamodel.DataStorageTypeS3, "no-cred", "in/a", datamodel.DataStorageTypeS3, "dst", "out/a")
	if _, err := f.engine.copyData(context.Background(), &ExecutionContext{}, "p1", "no-cred"); err == nil || !strings.Contains(err.Error(), "names no credential") {
		t.Errorf("err = %v, want a missing-credential error", err)
	}
	if len(f.stores.opened) != 0 {
		t.Errorf("opened %d stores without a credential", len(f.stores.opened))
	}
}

func TestCopyDataRefusesAnIncompleteTask(t *testing.T) {
	f := newStagingFixture(t)
	srcType := datamodel.DataStorageTypeS3
	task := &processmodel.DataStagingTask{
		ID: "half", ProcessID: strPtr("p1"),
		SourceDataStorageType: &srcType, SourceDataStorageID: strPtr("src"), SourcePath: strPtr("in/a"),
	}
	if err := f.gdb.Create(task).Error; err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := f.engine.copyData(context.Background(), &ExecutionContext{}, "p1", "half"); err == nil {
		t.Error("copyData of a task with no destination succeeded")
	}
}
