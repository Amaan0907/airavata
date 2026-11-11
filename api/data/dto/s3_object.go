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

package dto

import (
	"net/url"
	"strconv"
	"strings"
	"unicode"

	"github.com/apache/airavata/internal/httpx"
	"github.com/apache/airavata/internal/objectstore"
)

// S3FolderDelimiter is what separates the levels of a bucket when it is browsed as a
// tree. S3 itself has no folders: a folder is every key that shares a prefix up to here.
const S3FolderDelimiter = "/"

// maxContinuationTokenLength bounds a token passed back from a previous page. S3 tokens
// are opaque and far shorter; the cap only keeps an absurd query string from reaching
// the service.
const maxContinuationTokenLength = 4096

// S3ObjectListQuery selects one page of one level of a bucket.
type S3ObjectListQuery struct {
	// Prefix is the folder being browsed, such as "runs/2024/", or "" for the top. It
	// is passed to S3 as it is, so a prefix that does not end in "/" matches every key
	// and folder that starts with it.
	Prefix string

	// ContinuationToken is the nextContinuationToken of the previous page.
	ContinuationToken string

	// MaxKeys caps folders and files together. Zero means the S3 maximum, 1000.
	MaxKeys int
}

// S3ObjectListQueryFrom reads a listing query from a URL's query string.
func S3ObjectListQueryFrom(values url.Values) (S3ObjectListQuery, []httpx.FieldError) {
	var c httpx.Constraints
	q := S3ObjectListQuery{
		Prefix:            values.Get("prefix"),
		ContinuationToken: values.Get("continuationToken"),
	}

	switch {
	case len(q.Prefix) > maxS3ObjectKeyLength:
		c.Add("prefix", "Prefix must be at most 1024 bytes")
	case strings.IndexFunc(q.Prefix, unicode.IsControl) >= 0:
		c.Add("prefix", "Prefix must not contain control characters")
	}
	if len(q.ContinuationToken) > maxContinuationTokenLength {
		c.Add("continuationToken", "Continuation token is too long")
	}
	if raw := values.Get("maxKeys"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > objectstore.DefaultMaxKeys {
			c.Add("maxKeys", "maxKeys must be a whole number from 1 to 1000")
		} else {
			q.MaxKeys = n
		}
	}
	return q, c.Fields()
}

// S3ObjectListingResponse is one page of one level of a bucket.
type S3ObjectListingResponse struct {
	DataStorageID string `json:"dataStorageId"`
	Prefix        string `json:"prefix"`

	Folders []S3FolderResponse `json:"folders"`
	Files   []S3ObjectResponse `json:"files"`

	// NextContinuationToken is null on the last page.
	NextContinuationToken *string `json:"nextContinuationToken"`
}

// S3FolderResponse is a common prefix: a level below the one listed.
type S3FolderResponse struct {
	// Name is the folder's last segment, for display.
	Name string `json:"name"`

	// Prefix is what to pass as prefix to list the folder's contents.
	Prefix string `json:"prefix"`
}

// S3ObjectResponse is one object at the level listed.
type S3ObjectResponse struct {
	// Name is the key's last segment, for display.
	Name string `json:"name"`

	// Key is the full object key: the path to register a data product at.
	Key string `json:"key"`

	Size int64  `json:"size"`
	ETag string `json:"etag"`

	// LastModified is in milliseconds since the epoch, as every other timestamp here
	// is. It is 0 when the service did not report one.
	LastModified int64 `json:"lastModified"`
}

// ToS3ObjectListingResponse converts a page from the store.
//
// An object whose key ends in "/" is left out. Such an object is a zero-byte folder
// marker that S3 consoles create so an empty folder can exist. The folder it marks is
// already listed by its prefix, one level up, and the marker of the folder being listed
// has nothing in it to show. A page can therefore hold fewer entries than MaxKeys.
func ToS3ObjectListingResponse(storageID, prefix string, page *objectstore.Listing) S3ObjectListingResponse {
	out := S3ObjectListingResponse{
		DataStorageID: storageID,
		Prefix:        prefix,
		Folders:       make([]S3FolderResponse, 0, len(page.CommonPrefixes)),
		Files:         make([]S3ObjectResponse, 0, len(page.Objects)),
	}
	for _, p := range page.CommonPrefixes {
		out.Folders = append(out.Folders, S3FolderResponse{
			Name:   lastSegment(strings.TrimSuffix(p, S3FolderDelimiter)),
			Prefix: p,
		})
	}
	for _, o := range page.Objects {
		if strings.HasSuffix(o.Key, S3FolderDelimiter) {
			continue
		}
		var modified int64
		if !o.LastModified.IsZero() {
			modified = o.LastModified.UnixMilli()
		}
		out.Files = append(out.Files, S3ObjectResponse{
			Name:         lastSegment(o.Key),
			Key:          o.Key,
			Size:         o.Size,
			ETag:         o.ETag,
			LastModified: modified,
		})
	}
	if page.NextContinuationToken != "" {
		token := page.NextContinuationToken
		out.NextContinuationToken = &token
	}
	return out
}

func lastSegment(key string) string {
	return key[strings.LastIndex(key, S3FolderDelimiter)+1:]
}
