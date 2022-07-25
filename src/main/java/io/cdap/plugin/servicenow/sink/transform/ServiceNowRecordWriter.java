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

import com.github.rholder.retry.RetryException;
import com.google.gson.JsonObject;
import io.cdap.plugin.servicenow.sink.ServiceNowSinkConfig;
import io.cdap.plugin.servicenow.sink.model.RestRequest;
import io.cdap.plugin.servicenow.sink.service.ServiceNowSinkAPIRequestImpl;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 *  ServiceNow Record Writer class to insert/update records
 */
public class ServiceNowRecordWriter extends RecordWriter<NullWritable, JsonObject> {

  private ServiceNowSinkConfig config;
  private List<RestRequest> restRequests = new ArrayList<>();
  private ServiceNowSinkAPIRequestImpl servicenowSinkAPIImpl;

  private static final Logger LOG = LoggerFactory.getLogger(ServiceNowRecordWriter.class);

  public ServiceNowRecordWriter(ServiceNowSinkConfig config) {
    this.config = config;
    servicenowSinkAPIImpl = new ServiceNowSinkAPIRequestImpl(config);
  }

  @Override
  public void write(NullWritable key, JsonObject jsonObject) throws UnknownHostException {
    RestRequest restRequest = servicenowSinkAPIImpl.getRestRequest(jsonObject);
    restRequests.add(restRequest);
    if (restRequests.size() == config.getMaxRecordsPerBatch()) {
      boolean isBatchCreated;
      try {
        isBatchCreated = servicenowSinkAPIImpl.createPostRequestRetryableMode(restRequests);
      } catch (RetryException | ExecutionException exception) {
        restRequests.clear();
        LOG.info(String.format("Hostname: %s", InetAddress.getLocalHost().getHostName()));
        LOG.info(String.format("Thread ID : %s, Thread name: %s", Thread.currentThread().getId(),
                               Thread.currentThread().getName()));
        throw new RuntimeException(exception.getCause().getMessage());
      }
      if (isBatchCreated) {
        restRequests.clear();
        LOG.info(String.format("Hostname: %s", InetAddress.getLocalHost().getHostName()));
        LOG.info(String.format("Thread ID : %s, Thread name: %s", Thread.currentThread().getId(),
                               Thread.currentThread().getName()));
      } else {
        restRequests.clear();
        LOG.info(String.format("Hostname: %s", InetAddress.getLocalHost().getHostName()));
        LOG.info(String.format("Thread ID : %s, Thread name: %s", Thread.currentThread().getId(),
                               Thread.currentThread().getName()));
        throw new RuntimeException("Batch Creation Failed");
      }
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) {
    //create POST request for remaining requests
    if (!restRequests.isEmpty()) {
      try {
        servicenowSinkAPIImpl.createPostRequestRetryableMode(restRequests);
      } catch (RetryException | ExecutionException exception) {
        throw new RuntimeException(exception.getCause().getMessage());
      }
    }

  }
  
}
