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

package controller

import (
	"net/http"

	"github.com/apache/airavata/internal/httpx"

	dto "github.com/apache/airavata/api/data/dto"
	"github.com/apache/airavata/api/data/service"
)

// S3ObjectController serves /api/v1/s3-data-storages/{dataStorageId}/objects.
type S3ObjectController struct {
	svc *service.S3ObjectService
}

// NewS3ObjectController returns a handler delegating to svc.
func NewS3ObjectController(svc *service.S3ObjectService) *S3ObjectController {
	return &S3ObjectController{svc: svc}
}

// Register mounts the listing route.
func (h *S3ObjectController) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/s3-data-storages/{dataStorageId}/objects", h.list)
}

func (h *S3ObjectController) list(w http.ResponseWriter, r *http.Request) {
	q, fields := dto.S3ObjectListQueryFrom(r.URL.Query())
	if len(fields) > 0 {
		httpx.WriteError(w, r, httpx.Invalid(fields))
		return
	}
	out, err := h.svc.List(r.Context(), r.PathValue("dataStorageId"), q)
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}
