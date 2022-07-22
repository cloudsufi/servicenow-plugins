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
package io.cdap.plugin.servicenow.sink.service;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIRequestBuilder;
import io.cdap.plugin.servicenow.connector.ServiceNowConnectorConfig;
import io.cdap.plugin.servicenow.restapi.RestAPIResponse;
import io.cdap.plugin.servicenow.sink.ServiceNowSinkConfig;
import io.cdap.plugin.servicenow.sink.model.RestRequest;
import io.cdap.plugin.servicenow.sink.model.ServiceNowBatchRequest;
import io.cdap.plugin.servicenow.util.ServiceNowConstants;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.core.MediaType;

/**
 * Implementation class for ServiceNow Batch Rest API.
 */
public class ServiceNowSinkAPIRequestImpl {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceNowSinkAPIRequestImpl.class);

  private static Integer counter = 1;
  private static Integer batchRequestIdCounter = 1;
  private static Integer retryCounter = 0;
  private final ServiceNowSinkConfig config;
  private final ServiceNowTableAPIClientImpl restApi;
  private final Gson gson = new Gson();


  public ServiceNowSinkAPIRequestImpl(ServiceNowSinkConfig conf) {
    this.config = conf;
    restApi = new ServiceNowTableAPIClientImpl(config.getConnection());
  }

  public RestRequest getRestRequest(JsonObject jsonObject) {
    JsonElement jsonElement = jsonObject.get(ServiceNowConstants.SYS_ID);
    String sysId = null;

    if (jsonElement == null) {
      if (config.getOperation().equals(ServiceNowConstants.UPDATE_OPERATION)) {
        throw new RuntimeException("No sys_id found in the record to be updated");
      }
    } else {
      if (config.getOperation().equals(ServiceNowConstants.INSERT_OPERATION)) {
        jsonObject.remove(ServiceNowConstants.SYS_ID);
      }
      sysId = jsonElement.getAsString();
    }

    String data = jsonObject.toString();
    String encodedData = Base64.getEncoder().encodeToString(data.getBytes(StandardCharsets.UTF_8));

    List<Header> headers = new ArrayList<>();
    Header contentTypeHeader = new BasicHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
    Header acceptHeader = new BasicHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
    headers.add(contentTypeHeader);
    headers.add(acceptHeader);

    RestRequest restRequest = new RestRequest();
    restRequest.setUrl(String.format(ServiceNowConstants.INSERT_TABLE_API_URL_TEMPLATE, config.getTableName()));
    if (config.getOperation().equals(ServiceNowConstants.UPDATE_OPERATION)) {
      restRequest.setUrl(String.format(ServiceNowConstants.UPDATE_TABLE_API_URL_TEMPLATE, config.getTableName(),
        sysId));
    }
    restRequest.setId(counter.toString());
    counter++;
    restRequest.setHeaders(headers);
    restRequest.setMethod(ServiceNowConstants.HTTP_POST);
    if (config.getOperation().equals(ServiceNowConstants.UPDATE_OPERATION)) {
      restRequest.setMethod(ServiceNowConstants.HTTP_PUT);
    }
    restRequest.setBody(encodedData);
    return restRequest;
  }

  /**
   * Inserts/Updates the list of records into ServiceNow table
   *
   * @param records The list of rest Requests
   * @return true if the apiResponse code is 200 otherwise false
   */
  public Boolean createPostRequest(List<RestRequest> records) {
    ServiceNowBatchRequest payloadRequest = getPayloadRequest(records);
    ServiceNowTableAPIRequestBuilder requestBuilder = new ServiceNowTableAPIRequestBuilder(
      config.getConnection().getRestApiEndpoint());
    RestAPIResponse apiResponse;

    try {
      String accessToken = restApi.getAccessToken();
      requestBuilder.setAuthHeader(accessToken);
      requestBuilder.setAcceptHeader(MediaType.APPLICATION_JSON);
      requestBuilder.setContentTypeHeader(MediaType.APPLICATION_JSON);
      StringEntity stringEntity = new StringEntity(gson.toJson(payloadRequest), ContentType.APPLICATION_JSON);
      requestBuilder.setEntity(stringEntity);
      apiResponse = restApi.executePost(requestBuilder.build());

      if (!apiResponse.isSuccess()) {
        LOG.error("Error - {}", getErrorMessage(apiResponse.getResponseBody()));
        throw new RuntimeException();
      } else {
        LOG.info("API Response : {} ", apiResponse.getResponseBody());
        JsonObject responseJSON = new JsonParser().parse(apiResponse.getResponseBody()).getAsJsonObject();
        JsonArray servicedRequestsArray = responseJSON.get(ServiceNowConstants.SERVICED_REQUESTS).getAsJsonArray();
        for (int i = 0; i < servicedRequestsArray.size(); i++) {
          if (servicedRequestsArray.get(i).getAsJsonObject().get("status_code").getAsInt() == HttpStatus.SC_FORBIDDEN) {
            throw new RuntimeException("Permission denied for " + config.getOperation() + " operation");
          }
        }
        JsonArray unservicedRequestsArray = responseJSON.get(ServiceNowConstants.UNSERVICED_REQUESTS).getAsJsonArray();
        if (unservicedRequestsArray.size() > 0) {
          if (retryCounter == 1) {
            throw new RuntimeException("Please decrease the Max Records per Batch while configuring ServiceNow " +
                                            "Sink Plugin or increase the REST Batch API request timeout property in " +
                                            "ServiceNow Transaction Quota Rules");
          }
          LOG.info("Optimum Max Records per Batch is {}", (servicedRequestsArray.size() - 1));
          retryUnservicedRequests(records, unservicedRequestsArray);
        }
      }
    } catch (OAuthSystemException | OAuthProblemException | UnsupportedEncodingException | InterruptedException e) {
      LOG.error("Error in creating a new record", e);
      throw new RuntimeException("Error in creating a new record");
    }
    // Reset request counter
    counter = 1;
    // Reset retry counter
    retryCounter = 0;
    return apiResponse.getHttpStatus() == HttpStatus.SC_OK;
  }

  private String getErrorMessage(String responseBody) {
    try {
      JsonObject jo = gson.fromJson(responseBody, JsonObject.class);
      return jo.getAsJsonObject("error").get("message").getAsString();
    } catch (Exception e) {
      return e.getMessage();
    }
  }

  public ServiceNowBatchRequest getPayloadRequest(List<RestRequest> restRequests) {
    ServiceNowBatchRequest payloadRequest = new ServiceNowBatchRequest();
    payloadRequest.setBatchRequestId(batchRequestIdCounter.toString());
    payloadRequest.setRestRequests(restRequests);
    batchRequestIdCounter++;

    return payloadRequest;
  }
  
  /**
   * Retry the unserviced requests     .
   *
   * @param records The list of rest Requests
   * @param unservicedRequestsArray An array of unserviced requests
   */
  private void retryUnservicedRequests(List<RestRequest> records, JsonArray unservicedRequestsArray)
    throws InterruptedException {
    List<RestRequest> unservicedRequests = new ArrayList<>();
    List<Integer> unservicedRequestsIds = new ArrayList();
    for (int i = 0; i < unservicedRequestsArray.size(); i++) {
      unservicedRequestsIds.add(unservicedRequestsArray.get(i).getAsInt());
    }
    Collections.sort(unservicedRequestsIds);
    int start = unservicedRequestsIds.get(0);
    int end = unservicedRequestsIds.get(unservicedRequestsIds.size() - 1);
    // i = start-1 because request just prior to unserviced request fail due to maximum execution time getting exceeded
    for (int i = start - 1; i <= end; i++) {
      unservicedRequests.add(records.get(i - 1));
    }
    LOG.info("Retrying unserviced requests from Request No. {} to {}", (start - 1), end);
    retryCounter++;
    createPostRequest(unservicedRequests);
  }
}
