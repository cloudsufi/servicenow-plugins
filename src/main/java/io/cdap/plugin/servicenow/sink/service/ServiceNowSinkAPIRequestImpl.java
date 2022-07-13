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

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIRequestBuilder;
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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
  private final JsonParser jsonParser = new JsonParser();
  private Boolean isCreated;


  public ServiceNowSinkAPIRequestImpl(ServiceNowSinkConfig conf) {
    this.config = conf;
    restApi = new ServiceNowTableAPIClientImpl(config);
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
      config.getRestApiEndpoint());
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
        JsonObject responseJSON = jsonParser.parse(apiResponse.getResponseBody()).getAsJsonObject();
        JsonArray servicedRequestsArray = responseJSON.get(ServiceNowConstants.SERVICED_REQUESTS).getAsJsonArray();
        for (int i = 0; i < servicedRequestsArray.size(); i++) {
          if (servicedRequestsArray.get(i).getAsJsonObject().get(ServiceNowConstants.STATUS_CODE)
            .getAsInt() == HttpStatus.SC_FORBIDDEN) {
            String encodedResponseBody = servicedRequestsArray.get(i).getAsJsonObject().get(ServiceNowConstants.BODY)
              .getAsString();
            String decodedResponseBody = new String(Base64.getDecoder().decode(encodedResponseBody));
            String errorDetail = jsonParser.parse(decodedResponseBody).getAsJsonObject().get(ServiceNowConstants.ERROR)
              .getAsJsonObject().get(ServiceNowConstants.ERROR_DETAIL).getAsString();
            if (errorDetail.equals(ServiceNowConstants.ACL_EXCEPTION)) {
              throw new RuntimeException(String.format("Permission denied for '%s' operation.", config.getOperation()));
            } else {
              LOG.warn(String.format("'%s' operation failed for '%s'", config.getOperation(), config.getTableName()));
              LOG.info("Error Response : {} ", decodedResponseBody);
            }
          }
        }

        JsonArray unservicedRequestsArray = responseJSON.get(ServiceNowConstants.UNSERVICED_REQUESTS).getAsJsonArray();
        if (unservicedRequestsArray.size() > 0) {
          LOG.info("Response status code for last serviced request is {}",
                   servicedRequestsArray.get(servicedRequestsArray.size() - 1).getAsJsonObject()
                     .get(ServiceNowConstants.STATUS_CODE).getAsInt());
          String lastServicedRequestResponseBody = servicedRequestsArray.get(servicedRequestsArray.size() - 1)
            .getAsJsonObject().get("body").getAsString();
          LOG.info("Response Body for last serviced request is {}", new String(Base64.getDecoder()
            .decode(lastServicedRequestResponseBody)));
          LOG.info("Unserviced Requests : {}", unservicedRequestsArray);

          if (retryCounter == 1) {
            throw new RuntimeException("Please decrease the Max Records per Batch while configuring ServiceNow " +
                                         "Sink Plugin or increase the REST Batch API request timeout property in " +
                                         "ServiceNow Transaction Quota Rules");
          }
          LOG.info("Optimum Max Records per Batch is {}", (servicedRequestsArray.size() - 1));
          retryUnservicedRequests(records, unservicedRequestsArray);
        }
      }
    } catch (IOException e) {
      LOG.error("Unreliable connection or an could not complete the inability the execution of HTTP POST " +
                  "within the given time constraint (socket timeout)", e.getMessage());
      throw new RetryableException();
    } catch (OAuthSystemException | OAuthProblemException | InterruptedException e) {
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
      return jo.getAsJsonObject(ServiceNowConstants.ERROR).get(ServiceNowConstants.MESSAGE).getAsString();
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
    // i = start-1 because request just prior to the unserviced request fails due to maximum execution time exceeded
    for (int i = start - 1; i <= end; i++) {
      unservicedRequests.add(records.get(i - 1));
    }
    LOG.info("Retrying last failed serviced request & unserviced requests from Request No. {} to {}", (start - 1), end);
    retryCounter++;
    createPostRequest(unservicedRequests);
  }

  /**
   * Retries to insert/update the list of records into ServiceNow table when
   * RetryableException is thrown          .
   *
   * @param records The list of rest Requests
   *
   * @return true if records are created, false otherwise
   */
  public Boolean createPostRequestRetryableMode(List<RestRequest> records) throws ExecutionException, RetryException {
    Callable<Boolean> fetchRecords = () -> {
      isCreated = createPostRequest(records);
      return true;
    };

    Retryer<Boolean> retryer = RetryerBuilder.<Boolean>newBuilder()
      .retryIfExceptionOfType(RetryableException.class)
      .withWaitStrategy(WaitStrategies.exponentialWait(ServiceNowConstants.BASE_DELAY, TimeUnit.MILLISECONDS))
      .withStopStrategy(StopStrategies.stopAfterAttempt(ServiceNowConstants.MAX_NUMBER_OF_RETRY_ATTEMPTS))
      .build();

    try {
      retryer.call(fetchRecords);
    } catch (RetryException | ExecutionException e) {
      throw e;
    }

    return isCreated;
  }
}
