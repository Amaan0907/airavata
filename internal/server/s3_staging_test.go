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
	"net/http"
	"testing"
)

// seedS3Product registers a dataset whose primary location is an S3 object.
func (h *harness) seedS3Product(name, token string) (storageID, productID string) {
	h.t.Helper()
	_, storageID = h.seedS3Storage(name, token)
	out := h.mustDo(http.MethodPost, "/api/v1/data-products", token, map[string]any{
		"dataName": name, "isFile": true, "path": "runs/" + name + ".dat",
		"dataStorageId": storageID, "dataStorageType": "S3",
	}, http.StatusCreated)
	return storageID, out["dataId"].(string)
}

// A product can now live on S3, under the same rules as a replica there.
func TestProductCanLiveOnS3(t *testing.T) {
	h := newHarness(t)
	storageID, productID := h.seedS3Product("on-s3", tokenAlice)

	got := h.mustDo(http.MethodGet, "/api/v1/data-products/"+productID, tokenAlice, nil, http.StatusOK)
	if got["dataStorageType"] != "S3" || got["dataStorageId"] != storageID {
		t.Errorf("product location = %v %v, want the S3 storage", got["dataStorageType"], got["dataStorageId"])
	}
	primary := got["replicas"].([]any)[0].(map[string]any)
	if primary["isPrimary"] != true || primary["dataStorageType"] != "S3" {
		t.Errorf("primary replica = %v, want it on S3", primary)
	}

	body := func(over map[string]any) map[string]any {
		out := map[string]any{"dataName": "x", "isFile": true, "path": "runs/x.dat", "dataStorageId": storageID, "dataStorageType": "S3"}
		for k, v := range over {
			out[k] = v
		}
		return out
	}
	for name, tc := range map[string]struct {
		over   map[string]any
		status int
	}{
		"S3 id named as SCP":    {map[string]any{"dataStorageType": "SCP"}, http.StatusNotFound},
		"cluster as a location": {map[string]any{"dataStorageType": "HPC"}, http.StatusBadRequest},
		"leading slash key":     {map[string]any{"path": "/runs/x.dat"}, http.StatusBadRequest},
		"dot-dot key":           {map[string]any{"path": "runs/../x.dat"}, http.StatusBadRequest},
	} {
		if rec := h.do(http.MethodPost, "/api/v1/data-products", tokenAlice, body(tc.over)); rec.Code != tc.status {
			t.Errorf("%s: status = %d, want %d", name, rec.Code, tc.status)
		}
	}

	// A bucket nobody shared with the caller cannot hold their product.
	_, bobs := h.seedS3Storage("bobs", tokenBob)
	if rec := h.do(http.MethodPost, "/api/v1/data-products", tokenAlice, body(map[string]any{"dataStorageId": bobs})); rec.Code != http.StatusForbidden {
		t.Errorf("product on an unreachable bucket: status = %d, want 403", rec.Code)
	}
}

// The content endpoint still reads SCP only. It must refuse an S3 product plainly
// rather than reach for the wrong table.
func TestContentOfAnS3ProductIsRefused(t *testing.T) {
	h := newHarness(t)
	_, productID := h.seedS3Product("content", tokenAlice)
	if rec := h.download(productID, tokenAlice); rec.Code != http.StatusBadRequest {
		t.Errorf("download of an S3 product: status = %d, want 400", rec.Code)
	}
}

func TestStagingTasksMayNameS3(t *testing.T) {
	h := newHarness(t)
	processID := h.seedProcess("s3-staging", tokenAlice)
	base := "/api/v1/processes/" + processID + "/data-staging-tasks"
	for _, storageType := range []string{"S3", "HPC", "SCP"} {
		h.mustDo(http.MethodPost, base, tokenAlice, map[string]any{
			"sourcePath": "in", "destinationPath": "/out", "sourceDataStorageType": storageType,
		}, http.StatusCreated)
	}
}

// launchWith launches a run mapping one file input and one file output, and returns its
// staging tasks keyed by direction.
func (h *harness) launchWith(name, inputProduct, outputProduct string) (in, out map[string]any) {
	h.t.Helper()
	deployment, configID, inputID, outputID := h.seedDeploymentWithIO(name)
	created := h.mustDo(http.MethodPost, "/api/v1/processes", tokenAlice, map[string]any{
		"processType": "BATCH_JOB",
		"batchProcess": map[string]any{
			"deploymentId":         deployment["deploymentId"],
			"slurmClusterConfigId": configID,
			"batchJobConfig":       map[string]any{"wallTimeMinutes": 10, "allocation": "A"},
			"baseWorkDir":          "/scratch/alice",
			"inputMappings":        []any{map[string]any{"templateInputId": inputID, "value": inputProduct}},
			"outputMappings":       []any{map[string]any{"templateOutputId": outputID, "value": outputProduct}},
		},
	}, http.StatusCreated)
	base := "/api/v1/processes/" + created["processId"].(string)
	h.mustDo(http.MethodPost, base+"/launch", tokenAlice, nil, http.StatusAccepted)

	for _, task := range h.list(base+"/data-staging-tasks", tokenAlice) {
		if task["destinationDataStorageType"] == "HPC" {
			in = task
		}
		if task["sourceDataStorageType"] == "HPC" {
			out = task
		}
	}
	if in == nil || out == nil {
		h.t.Fatalf("launch did not record an inbound and an outbound staging task")
	}
	return in, out
}

// An S3 input is staged from the bucket and an S3 output back to it. The cluster end of
// both tasks stays HPC: what reaches the cluster is an SCP copy made by this server.
func TestLaunchStagesS3InputsAndOutputs(t *testing.T) {
	h := newHarness(t)
	inStorage, inProduct := h.seedS3Product("s3-in", tokenAlice)
	outStorage, outProduct := h.seedS3Product("s3-out", tokenAlice)

	in, out := h.launchWith("s3-launch", inProduct, outProduct)
	if in["sourceDataStorageType"] != "S3" || in["sourceDataStorageId"] != inStorage || in["sourcePath"] != "runs/s3-in.dat" {
		t.Errorf("inbound task = %v, want it to read the S3 object", in)
	}
	if out["destinationDataStorageType"] != "S3" || out["destinationDataStorageId"] != outStorage || out["destinationPath"] != "runs/s3-out.dat" {
		t.Errorf("outbound task = %v, want it to write the S3 object", out)
	}
}

// dropPrimaryLocation turns a product into one registered before it had to name a path:
// no path on the product and so no primary replica. The API no longer creates these,
// but older rows can still look like this.
func (h *harness) dropPrimaryLocation(productID string) {
	h.t.Helper()
	if err := h.db.Exec(`UPDATE data_products SET path = NULL WHERE data_id = ?`, productID).Error; err != nil {
		h.t.Fatalf("clear path: %v", err)
	}
	if err := h.db.Exec(`DELETE FROM data_product_replicas WHERE data_product_id = ? AND is_primary`, productID).Error; err != nil {
		h.t.Fatalf("drop primary replica: %v", err)
	}
}

// A product with no primary location is staged from its oldest replica.
func TestLaunchStagesFromAReplicaWhenThereIsNoPrimary(t *testing.T) {
	h := newHarness(t)
	_, product := h.seedProduct("replica-only", tokenAlice)
	_, s3Storage := h.seedS3Storage("replica-only", tokenAlice)
	h.mustDo(http.MethodPost, "/api/v1/data-products/"+product+"/replicas", tokenAlice,
		s3Replica(s3Storage, "inputs/seq.fa"), http.StatusCreated)
	h.dropPrimaryLocation(product)
	_, outProduct := h.seedProduct("replica-out", tokenAlice)

	in, _ := h.launchWith("replica-launch", product, outProduct)
	if in["sourceDataStorageType"] != "S3" || in["sourcePath"] != "inputs/seq.fa" {
		t.Errorf("inbound task = %v, want it to read the replica", in)
	}
}

// An input with no location at all has nothing to stage, which is the run's problem to
// report rather than the staging task's to discover.
func TestLaunchRefusesAnInputWithNoLocation(t *testing.T) {
	h := newHarness(t)
	_, product := h.seedProduct("nowhere", tokenAlice)
	h.dropPrimaryLocation(product)
	_, outProduct := h.seedProduct("nowhere-out", tokenAlice)

	deployment, configID, inputID, outputID := h.seedDeploymentWithIO("nowhere")
	created := h.mustDo(http.MethodPost, "/api/v1/processes", tokenAlice, map[string]any{
		"processType": "BATCH_JOB",
		"batchProcess": map[string]any{
			"deploymentId":         deployment["deploymentId"],
			"slurmClusterConfigId": configID,
			"batchJobConfig":       map[string]any{"wallTimeMinutes": 10, "allocation": "A"},
			"baseWorkDir":          "/scratch/alice",
			"inputMappings":        []any{map[string]any{"templateInputId": inputID, "value": product}},
			"outputMappings":       []any{map[string]any{"templateOutputId": outputID, "value": outProduct}},
		},
	}, http.StatusCreated)
	rec := h.do(http.MethodPost, "/api/v1/processes/"+created["processId"].(string)+"/launch", tokenAlice, nil)
	if rec.Code != http.StatusConflict {
		t.Errorf("launch with an unlocated input: status = %d, want 409\nbody: %s", rec.Code, rec.Body.String())
	}
}
