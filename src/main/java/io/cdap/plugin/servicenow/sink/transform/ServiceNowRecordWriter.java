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
package io.cdap.plugin.servicenow.sink.transform;

import com.google.gson.JsonObject;
import io.cdap.plugin.servicenow.sink.ServiceNowSinkConfig;
import io.cdap.plugin.servicenow.sink.model.RestRequest;
import io.cdap.plugin.servicenow.sink.util.ServiceNowSinkAPIRequestImpl;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *  ServiceNow Record Writer class to insert/update records
 */
public class ServiceNowRecordWriter extends RecordWriter<NullWritable, JsonObject> {

  private ServiceNowSinkConfig config;
  private List<RestRequest> restRequests = new ArrayList<>();
  private ServiceNowSinkAPIRequestImpl servicenowSinkAPIImpl;

  public ServiceNowRecordWriter(ServiceNowSinkConfig config) {
    super();
    this.config = config;
    servicenowSinkAPIImpl = new ServiceNowSinkAPIRequestImpl(config);
  }

  @Override
  public void write(NullWritable key, JsonObject jsonObject) throws IOException {
    RestRequest restRequest = servicenowSinkAPIImpl.getRestRequest(jsonObject);
    restRequests.add(restRequest);
    if (restRequests.size() == config.getMaxRecordsPerBatch()) {
      Boolean isBatchCreated = servicenowSinkAPIImpl.createPostRequest(restRequests);
      if (isBatchCreated) {
        restRequests.clear();
      }
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    //call payload request
    servicenowSinkAPIImpl.createPostRequest(restRequests);
  }
  
}
