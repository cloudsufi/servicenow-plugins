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
package io.cdap.plugin.servicenow.sink.util;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.plugin.servicenow.restapi.RestAPIResponse;
import io.cdap.plugin.servicenow.sink.ServiceNowSinkConfig;
import io.cdap.plugin.servicenow.sink.model.RestRequest;
import io.cdap.plugin.servicenow.sink.model.ServiceNowBatchRequest;
import io.cdap.plugin.servicenow.source.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.source.apiclient.ServiceNowTableAPIRequestBuilder;
import org.apache.http.Header;
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
import java.util.List;

/**
 * Implementation class for ServiceNow Batch Rest API.
 */
public class ServiceNowSinkAPIRequestImpl {

  private ServiceNowSinkConfig config;
  private static final Logger LOG = LoggerFactory.getLogger(ServiceNowSinkAPIRequestImpl.class);
  private ServiceNowTableAPIClientImpl restApi;
  private static final String INSERT_TABLE_API_URL_TEMPLATE = "/api/now/table/%s";
  private static final String UPDATE_TABLE_API_URL_TEMPLATE = "/api/now/table/%s/%s";
  private static final String RECORD = "record";
  private static final String HTTP_POST = "POST";
  private static final String HTTP_PUT = "PUT";
  private static final String SYS_ID = "sys_id";
  private static Integer counter = 1;
  private static Integer batchRequestIdCounter = 1;

  public ServiceNowSinkAPIRequestImpl(ServiceNowSinkConfig conf) {
    this.config = conf;
    restApi = new ServiceNowTableAPIClientImpl(config);
  }

   public RestRequest getRestRequest(JsonObject jsonObject) {
    String sysId = jsonObject.get(SYS_ID).getAsString();
    JsonObject restRequestPayload = new JsonObject();
    restRequestPayload.add(RECORD, jsonObject);

    String data = restRequestPayload.toString();
    String encodedData = Base64.getEncoder().encodeToString(data.getBytes(StandardCharsets.UTF_8));

    List<Header> headers = new ArrayList<>();
    Header contentTypeHeader = new BasicHeader("Content-Type", "application/json");
    Header acceptHeader = new BasicHeader("Accept", "application/json");
    headers.add(contentTypeHeader);
    headers.add(acceptHeader);
    
    RestRequest restRequest = new RestRequest();
    restRequest.setUrl(String.format(INSERT_TABLE_API_URL_TEMPLATE, config.getTableName()));
     if (config.getOperation().equals("update")) {
       restRequest.setUrl(String.format(UPDATE_TABLE_API_URL_TEMPLATE, config.getTableName(), sysId));
     }
    restRequest.setId(counter.toString());
    counter++;
    restRequest.setHeaders(headers);
    restRequest.setMethod(HTTP_POST);
     if (config.getOperation().equals("update")) {
       restRequest.setMethod(HTTP_PUT);
     }
    restRequest.setBody(encodedData);
    return restRequest;
  }

  public Boolean createPostRequest(List<RestRequest> records) {
    ServiceNowBatchRequest payloadRequest = getPayloadRequest(records);
    ServiceNowTableAPIRequestBuilder requestBuilder = new ServiceNowTableAPIRequestBuilder(
      config.getRestApiEndpoint());
    RestAPIResponse apiResponse;

    try {
      String accessToken = restApi.getAccessToken();
      requestBuilder.setAuthHeader(accessToken);
      requestBuilder.setAcceptHeader("application/json");
      requestBuilder.setContentTypeHeader("application/json");
      Gson gson = new Gson();
      StringEntity stringEntity = new StringEntity(gson.toJson(payloadRequest), ContentType.APPLICATION_JSON);
      requestBuilder.setEntity(stringEntity);
      apiResponse = restApi.executePost(requestBuilder.build());
      if (!apiResponse.isSuccess()) {
        LOG.error("Error - {}", getErrorMessage(apiResponse.getResponseBody()));
      }
    } catch (OAuthSystemException | OAuthProblemException | UnsupportedEncodingException e) {
      LOG.error("Error in creating a new record", e);
      throw new RuntimeException("Error in creating a new record");
    }
    return apiResponse.getHttpStatus() == HttpStatus.SC_OK;
  }

  private String getErrorMessage(String responseBody) {
    try {
      Gson gson = new Gson();
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
}
