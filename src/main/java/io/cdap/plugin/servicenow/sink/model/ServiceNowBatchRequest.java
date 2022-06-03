/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.servicenow.sink.model;

import com.google.gson.annotations.SerializedName;

import java.util.List;

/**
 *  ServiceNow Batch Request Model
 */
public class ServiceNowBatchRequest {

  @SerializedName("batch_request_id")
  private String batchRequestId;

  @SerializedName("rest_requests")
  private List<RestRequest> restRequests;

  public String getBatchRequestId() {
    return batchRequestId;
  }

  public void setBatchRequestId(String batchRequestId) {
    this.batchRequestId = batchRequestId;
  }

  public List<RestRequest> getRestRequests() {
    return restRequests;
  }

  public void setRestRequests(List<RestRequest> restRequests) {
    this.restRequests = restRequests;
  }

}
