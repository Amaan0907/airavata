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
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

func objectsPath(storageID string, query url.Values) string {
	path := "/api/v1/s3-data-storages/" + storageID + "/objects"
	if len(query) > 0 {
		path += "?" + query.Encode()
	}
	return path
}

// names returns the "name" of each entry in a listing's folders or files.
func names(listing map[string]any, field string) []string {
	out := []string{}
	for _, entry := range listing[field].([]any) {
		out = append(out, entry.(map[string]any)["name"].(string))
	}
	return out
}

// seedBrowsableBucket fills a storage's bucket with a small tree, including the folder
// markers an S3 console leaves behind.
func seedBrowsableBucket(buckets *fakeBuckets, bucket string) {
	for key, content := range map[string]string{
		"top.txt":            "t",
		"runs/":              "",
		"runs/a.dat":         "aaaa",
		"runs/b.dat":         "bb",
		"runs/sub/c.dat":     "c",
		"runs/sub/deep/d.fa": "d",
		"runs/empty/":        "",
	} {
		buckets.put(bucket, key, content)
	}
}

// A bucket browses as a tree: one level per request, folders from the common prefixes
// and files from the objects, with folder markers left out.
func TestS3ObjectListingBrowsesOneLevel(t *testing.T) {
	h := newHarness(t)
	_, buckets := h.withFakeTransfers()
	_, storageID := h.seedS3Storage("browse", tokenAlice)
	seedBrowsableBucket(buckets, "browse-bucket")

	top := h.mustDo(http.MethodGet, objectsPath(storageID, nil), tokenAlice, nil, http.StatusOK)
	if got := names(top, "folders"); !reflect.DeepEqual(got, []string{"runs"}) {
		t.Errorf("top folders = %v, want [runs]", got)
	}
	if got := names(top, "files"); !reflect.DeepEqual(got, []string{"top.txt"}) {
		t.Errorf("top files = %v, want [top.txt]", got)
	}
	if top["prefix"] != "" || top["dataStorageId"] != storageID || top["nextContinuationToken"] != nil {
		t.Errorf("top listing = %v", top)
	}

	// Opening a folder is listing its prefix, exactly as the folder entry gives it.
	next := top["folders"].([]any)[0].(map[string]any)["prefix"].(string)
	runs := h.mustDo(http.MethodGet, objectsPath(storageID, url.Values{"prefix": {next}}), tokenAlice, nil, http.StatusOK)
	if got := names(runs, "folders"); !reflect.DeepEqual(got, []string{"empty", "sub"}) {
		t.Errorf("runs/ folders = %v, want [empty sub]", got)
	}
	// "runs/" itself is a folder marker and is not listed as a file of runs/.
	if got := names(runs, "files"); !reflect.DeepEqual(got, []string{"a.dat", "b.dat"}) {
		t.Errorf("runs/ files = %v, want [a.dat b.dat]", got)
	}
	file := runs["files"].([]any)[0].(map[string]any)
	if file["key"] != "runs/a.dat" || file["size"] != float64(4) || file["etag"] != "etag-runs/a.dat" ||
		file["lastModified"] != float64(fakeModified.UnixMilli()) {
		t.Errorf("file entry = %v", file)
	}

	// An empty folder exists only through its marker, and lists as empty arrays.
	empty := h.mustDo(http.MethodGet, objectsPath(storageID, url.Values{"prefix": {"runs/empty/"}}), tokenAlice, nil, http.StatusOK)
	if len(empty["folders"].([]any)) != 0 || len(empty["files"].([]any)) != 0 {
		t.Errorf("empty folder = %v, want no entries", empty)
	}

	// A prefix that is not a whole folder matches the start of names, as S3 does.
	partial := h.mustDo(http.MethodGet, objectsPath(storageID, url.Values{"prefix": {"runs/s"}}), tokenAlice, nil, http.StatusOK)
	if got := names(partial, "folders"); !reflect.DeepEqual(got, []string{"sub"}) {
		t.Errorf("runs/s folders = %v, want [sub]", got)
	}
}

// A long level comes back a page at a time, and following the tokens visits every
// entry once.
func TestS3ObjectListingPages(t *testing.T) {
	h := newHarness(t)
	_, buckets := h.withFakeTransfers()
	_, storageID := h.seedS3Storage("pages", tokenAlice)
	seedBrowsableBucket(buckets, "pages-bucket")

	var seen []string
	query := url.Values{"prefix": {"runs/"}, "maxKeys": {"2"}}
	for pages := 0; ; pages++ {
		if pages > 5 {
			t.Fatal("listing did not end")
		}
		page := h.mustDo(http.MethodGet, objectsPath(storageID, query), tokenAlice, nil, http.StatusOK)
		seen = append(seen, names(page, "folders")...)
		seen = append(seen, names(page, "files")...)
		token, more := page["nextContinuationToken"].(string)
		if !more {
			break
		}
		query.Set("continuationToken", token)
	}
	if want := []string{"a.dat", "b.dat", "empty", "sub"}; !sameSet(seen, want) {
		t.Errorf("entries across pages = %v, want %v once each", seen, want)
	}
}

func sameSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	count := map[string]int{}
	for _, s := range got {
		count[s]++
	}
	for _, s := range want {
		if count[s] != 1 {
			return false
		}
	}
	return true
}

func TestS3ObjectListingValidation(t *testing.T) {
	h := newHarness(t)
	_, buckets := h.withFakeTransfers()
	_, storageID := h.seedS3Storage("checks", tokenAlice)

	for name, query := range map[string]url.Values{
		"zero maxKeys":      {"maxKeys": {"0"}},
		"maxKeys over 1000": {"maxKeys": {"1001"}},
		"non-numeric":       {"maxKeys": {"ten"}},
		"prefix too long":   {"prefix": {strings.Repeat("a", 1025)}},
		"control in prefix": {"prefix": {"runs/\x00"}},
		"enormous token":    {"continuationToken": {strings.Repeat("t", 4097)}},
	} {
		if rec := h.do(http.MethodGet, objectsPath(storageID, query), tokenAlice, nil); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", name, rec.Code)
		}
	}
	if len(buckets.opened) != 0 {
		t.Errorf("an invalid query still opened the bucket: %+v", buckets.opened)
	}
	if rec := h.do(http.MethodGet, objectsPath(storageID, url.Values{"maxKeys": {"1000"}}), tokenAlice, nil); rec.Code != http.StatusOK {
		t.Errorf("maxKeys 1000: status = %d, want 200", rec.Code)
	}
}

// Listing a bucket is reading the storage: it takes READ on the storage itself, and a
// share on one product in the bucket does not open the rest of it.
func TestS3ObjectListingNeedsReadOnTheStorage(t *testing.T) {
	h := newHarness(t)
	_, buckets := h.withFakeTransfers()
	storageID, productID := h.seedS3Product("guarded", tokenAlice)
	buckets.put("guarded-bucket", "runs/guarded.dat", "x")

	if rec := h.do(http.MethodGet, objectsPath(storageID, nil), "", nil); rec.Code != http.StatusUnauthorized {
		t.Errorf("anonymous: status = %d, want 401", rec.Code)
	}
	if rec := h.do(http.MethodGet, objectsPath(storageID, nil), tokenBob, nil); rec.Code != http.StatusForbidden {
		t.Errorf("stranger: status = %d, want 403", rec.Code)
	}
	h.shareProductWithUser(productID, "bob", "READ", tokenAlice)
	if rec := h.do(http.MethodGet, objectsPath(storageID, nil), tokenBob, nil); rec.Code != http.StatusForbidden {
		t.Errorf("product share only: status = %d, want 403", rec.Code)
	}
	if len(buckets.opened) != 0 {
		t.Errorf("a refused listing still opened the bucket: %+v", buckets.opened)
	}

	h.shareS3StorageWithUser(storageID, "bob", "READ", tokenAlice)
	if rec := h.do(http.MethodGet, objectsPath(storageID, nil), tokenBob, nil); rec.Code != http.StatusOK {
		t.Errorf("READ on the storage: status = %d, want 200\nbody: %s", rec.Code, rec.Body.String())
	}
	if rec := h.do(http.MethodGet, objectsPath("no-such-storage", nil), tokenAlice, nil); rec.Code != http.StatusNotFound {
		t.Errorf("unknown storage: status = %d, want 404", rec.Code)
	}
}

func TestS3ObjectListingReportsUnreachableStore(t *testing.T) {
	h := newHarness(t)
	_, buckets := h.withFakeTransfers()
	buckets.err = errors.New("dial tcp: connection refused")
	_, storageID := h.seedS3Storage("offline", tokenAlice)

	rec := h.do(http.MethodGet, objectsPath(storageID, nil), tokenAlice, nil)
	if rec.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", rec.Code)
	}
	if body := rec.Body.String(); strings.Contains(body, "secret-offline") || strings.Contains(body, "connection refused") {
		t.Errorf("response leaks internal state: %s", body)
	}
}

// Browsing and registering fit together: an object found by listing is registered as a
// product at its key, and its content is then read through the product.
func TestABrowsedObjectCanBeRegisteredAsAProduct(t *testing.T) {
	h := newHarness(t)
	_, buckets := h.withFakeTransfers()
	_, storageID := h.seedS3Storage("register", tokenAlice)
	seedBrowsableBucket(buckets, "register-bucket")

	listing := h.mustDo(http.MethodGet, objectsPath(storageID, url.Values{"prefix": {"runs/sub/deep/"}}), tokenAlice, nil, http.StatusOK)
	found := listing["files"].([]any)[0].(map[string]any)

	product := h.mustDo(http.MethodPost, "/api/v1/data-products", tokenAlice, map[string]any{
		"dataName": found["name"], "isFile": true, "path": found["key"],
		"dataStorageId": storageID, "dataStorageType": "S3",
	}, http.StatusCreated)

	rec := h.download(product["dataId"].(string), tokenAlice)
	if rec.Code != http.StatusOK || rec.Body.String() != "d" {
		t.Errorf("download of the registered object: status %d, body %q", rec.Code, rec.Body.String())
	}
}
