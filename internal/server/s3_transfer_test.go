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

package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/apache/airavata/internal/objectstore"
)

// fakeBuckets stands in for S3 behind the content endpoint. It keeps objects in memory
// and records every bucket it was asked to open, with the configuration it was opened
// under, which is how a test asserts a refusal happened before any credential was used.
type fakeBuckets struct {
	mu      sync.Mutex
	objects map[string]map[string]string
	opened  []objectstore.Config

	// err, when set, fails every transfer, standing for an unreachable store.
	err error
}

func newFakeBuckets() *fakeBuckets {
	return &fakeBuckets{objects: map[string]map[string]string{}}
}

func (f *fakeBuckets) open(cfg objectstore.Config) (objectstore.Store, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opened = append(f.opened, cfg)
	if f.objects[cfg.Bucket] == nil {
		f.objects[cfg.Bucket] = map[string]string{}
	}
	return &fakeBucket{f: f, bucket: cfg.Bucket}, nil
}

func (f *fakeBuckets) put(bucket, key, content string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.objects[bucket] == nil {
		f.objects[bucket] = map[string]string{}
	}
	f.objects[bucket][key] = content
}

func (f *fakeBuckets) get(bucket, key string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	content, ok := f.objects[bucket][key]
	return content, ok
}

type fakeBucket struct {
	f      *fakeBuckets
	bucket string
}

func (b *fakeBucket) Stat(_ context.Context, key string) (*objectstore.ObjectInfo, error) {
	content, ok := b.f.get(b.bucket, key)
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return &objectstore.ObjectInfo{Key: key, Size: int64(len(content))}, nil
}

func (b *fakeBucket) Download(_ context.Context, key, localPath string) error {
	if b.f.err != nil {
		return b.f.err
	}
	content, ok := b.f.get(b.bucket, key)
	if !ok {
		return fmt.Errorf("reading %s: %w", key, objectstore.ErrNotFound)
	}
	return os.WriteFile(localPath, []byte(content), 0o600)
}

func (b *fakeBucket) Upload(_ context.Context, localPath, key string) error {
	if b.f.err != nil {
		return b.f.err
	}
	content, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}
	b.f.put(b.bucket, key, string(content))
	return nil
}

func (b *fakeBucket) List(context.Context, objectstore.ListOptions) (*objectstore.Listing, error) {
	return &objectstore.Listing{}, nil
}

// An S3 product's content goes up to the object its path names and comes back from it,
// through this server, under the storage's credential.
func TestS3ProductContentRoundTrip(t *testing.T) {
	h := newHarness(t)
	fake, buckets := h.withFakeTransfers()
	_, productID := h.seedS3Product("roundtrip", tokenAlice)

	rec := h.upload(productID, tokenAlice, "ACGT")
	if rec.Code != http.StatusOK {
		t.Fatalf("upload: status = %d, want 200\nbody: %s", rec.Code, rec.Body.String())
	}
	var uploaded map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &uploaded); err != nil {
		t.Fatalf("decode upload response: %v", err)
	}
	if uploaded["provisionStatus"] != "PROVISIONED" {
		t.Errorf("status after upload = %v, want PROVISIONED", uploaded["provisionStatus"])
	}
	if got, _ := buckets.get("roundtrip-bucket", "runs/roundtrip.dat"); got != "ACGT" {
		t.Errorf("object content = %q, want ACGT", got)
	}

	// The bucket is opened as the storage describes it, under its credential.
	want := objectstore.Config{
		EndpointURL: "https://minio.example.edu:9000", Region: "us-east-1", Bucket: "roundtrip-bucket", PathStyle: true,
		AccessKeyID: "AKIA-roundtrip", SecretAccessKey: "secret-roundtrip",
	}
	if len(buckets.opened) == 0 || buckets.opened[0] != want {
		t.Errorf("opened = %+v, want %+v", buckets.opened, want)
	}

	rec = h.download(productID, tokenAlice)
	if rec.Code != http.StatusOK {
		t.Fatalf("download: status = %d, want 200\nbody: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "ACGT" {
		t.Errorf("downloaded %q, want ACGT", got)
	}
	if got := rec.Header().Get("Content-Length"); got != "4" {
		t.Errorf("Content-Length = %q, want 4", got)
	}
	if got := rec.Header().Get("Content-Disposition"); got != `attachment; filename=roundtrip.dat` {
		t.Errorf("Content-Disposition = %q, want the key's last segment", got)
	}
	if fake.touched() {
		t.Errorf("an S3 transfer reached the SCP transport: %+v", fake)
	}
}

// The same shares govern an S3 product as an SCP one, and a share on the product is
// enough: the caller never needs the storage, or sees its credential.
func TestS3ProductContentRespectsShares(t *testing.T) {
	h := newHarness(t)
	_, buckets := h.withFakeTransfers()
	storageID, productID := h.seedS3Product("shared", tokenAlice)
	buckets.put("shared-bucket", "runs/shared.dat", "ACGT")

	if rec := h.download(productID, tokenBob); rec.Code != http.StatusForbidden {
		t.Errorf("stranger download: status = %d, want 403", rec.Code)
	}
	if rec := h.upload(productID, tokenBob, "nope"); rec.Code != http.StatusForbidden {
		t.Errorf("stranger upload: status = %d, want 403", rec.Code)
	}
	if len(buckets.opened) != 0 {
		t.Errorf("a refused request still opened the bucket: %+v", buckets.opened)
	}

	shareID := h.shareProductWithUser(productID, "bob", "READ", tokenAlice)
	h.mustDo(http.MethodGet, "/api/v1/s3-data-storages/"+storageID, tokenBob, nil, http.StatusForbidden)
	if rec := h.download(productID, tokenBob); rec.Code != http.StatusOK {
		t.Errorf("READ download: status = %d, want 200\nbody: %s", rec.Code, rec.Body.String())
	}
	if rec := h.upload(productID, tokenBob, "nope"); rec.Code != http.StatusForbidden {
		t.Errorf("READ upload: status = %d, want 403", rec.Code)
	}

	h.widenShare(productID, shareID, "WRITE", tokenAlice)
	if rec := h.upload(productID, tokenBob, "TTTT"); rec.Code != http.StatusOK {
		t.Errorf("WRITE upload: status = %d, want 200", rec.Code)
	}
	if got, _ := buckets.get("shared-bucket", "runs/shared.dat"); got != "TTTT" {
		t.Errorf("object content = %q, want TTTT", got)
	}
}

// S3 says when an object is missing, so a product registered before anything was
// written to it is a 404 rather than a failure of the store.
func TestS3ProductDownloadOfAMissingObject(t *testing.T) {
	h := newHarness(t)
	h.withFakeTransfers()
	_, productID := h.seedS3Product("empty", tokenAlice)

	if rec := h.download(productID, tokenAlice); rec.Code != http.StatusNotFound {
		t.Errorf("download of an unwritten object: status = %d, want 404\nbody: %s", rec.Code, rec.Body.String())
	}
}

// A store that cannot be reached is the upstream's failure, the product is not marked
// provisioned, and the response names neither the credential nor the cause.
func TestS3ProductContentReportsUnreachableStore(t *testing.T) {
	h := newHarness(t)
	_, buckets := h.withFakeTransfers()
	buckets.err = errors.New("dial tcp: connection refused")
	_, productID := h.seedS3Product("down", tokenAlice)

	for name, rec := range map[string]*httptest.ResponseRecorder{
		"download": h.download(productID, tokenAlice),
		"upload":   h.upload(productID, tokenAlice, "ACGT"),
	} {
		if rec.Code != http.StatusBadGateway {
			t.Errorf("%s: status = %d, want 502", name, rec.Code)
		}
		if body := rec.Body.String(); strings.Contains(body, "secret-down") || strings.Contains(body, "connection refused") {
			t.Errorf("%s: response leaks internal state: %s", name, body)
		}
	}

	out := h.mustDo(http.MethodGet, "/api/v1/data-products/"+productID, tokenAlice, nil, http.StatusOK)
	if out["provisionStatus"] != "REGISTERD" {
		t.Errorf("status after a failed upload = %v, want REGISTERD", out["provisionStatus"])
	}
}

// A product with no primary location is downloaded from its oldest replica, as a job
// launch would stage it. An upload has nowhere to go: it writes the primary only.
func TestContentDownloadReadsAReplicaWhenThereIsNoPrimary(t *testing.T) {
	h := newHarness(t)
	_, buckets := h.withFakeTransfers()
	_, productID := h.seedProduct("replica-dl", tokenAlice)
	_, s3Storage := h.seedS3Storage("replica-dl", tokenAlice)
	h.mustDo(http.MethodPost, "/api/v1/data-products/"+productID+"/replicas", tokenAlice,
		s3Replica(s3Storage, "inputs/seq.fa"), http.StatusCreated)
	h.dropPrimaryLocation(productID)
	buckets.put("replica-dl-bucket", "inputs/seq.fa", "GATTACA")

	rec := h.download(productID, tokenAlice)
	if rec.Code != http.StatusOK {
		t.Fatalf("download: status = %d, want 200\nbody: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "GATTACA" {
		t.Errorf("downloaded %q, want the replica's content", got)
	}
	if got := rec.Header().Get("Content-Disposition"); got != `attachment; filename=seq.fa` {
		t.Errorf("Content-Disposition = %q, want the replica's key", got)
	}

	if rec := h.upload(productID, tokenAlice, "ACGT"); rec.Code != http.StatusConflict {
		t.Errorf("upload with no primary: status = %d, want 409", rec.Code)
	}
}

// Downloads read the primary when there is one, and uploads replace the primary alone:
// a replica on S3 is the caller's copy, not something this endpoint rewrites.
func TestContentPrefersThePrimaryOverReplicas(t *testing.T) {
	h := newHarness(t)
	fake, buckets := h.withFakeTransfers()
	_, productID := h.seedProduct("primary-first", tokenAlice)
	_, s3Storage := h.seedS3Storage("primary-first", tokenAlice)
	h.mustDo(http.MethodPost, "/api/v1/data-products/"+productID+"/replicas", tokenAlice,
		s3Replica(s3Storage, "copies/primary-first"), http.StatusCreated)
	fake.content["/scratch/primary-first"] = "SCP"
	buckets.put("primary-first-bucket", "copies/primary-first", "S3")

	if rec := h.download(productID, tokenAlice); rec.Body.String() != "SCP" {
		t.Errorf("downloaded %q (status %d), want the primary's content", rec.Body.String(), rec.Code)
	}
	if rec := h.upload(productID, tokenAlice, "NEW"); rec.Code != http.StatusOK {
		t.Fatalf("upload: status = %d, want 200", rec.Code)
	}
	if fake.content["/scratch/primary-first"] != "NEW" {
		t.Errorf("primary content = %q, want NEW", fake.content["/scratch/primary-first"])
	}
	if got, _ := buckets.get("primary-first-bucket", "copies/primary-first"); got != "S3" {
		t.Errorf("replica content = %q, want it untouched", got)
	}
	if len(buckets.opened) != 0 {
		t.Errorf("opened %d buckets for an SCP product's primary", len(buckets.opened))
	}
}
