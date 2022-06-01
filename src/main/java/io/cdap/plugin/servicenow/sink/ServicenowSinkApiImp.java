package io.cdap.plugin.servicenow.sink;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.plugin.servicenow.ServiceNowBaseConfig;
import io.cdap.plugin.servicenow.restapi.RestAPIClient;
import io.cdap.plugin.servicenow.restapi.RestAPIResponse;
import io.cdap.plugin.servicenow.source.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.source.apiclient.ServiceNowTableAPIRequestBuilder;
import io.cdap.plugin.servicenow.source.util.ServiceNowConstants;
import okhttp3.HttpUrl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.http.entity.StringEntity;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ServicenowSinkApiImp {

  private ServiceNowSinkConfig config;
  private List<Request> records = new ArrayList<>();
  private static final Logger LOG = LoggerFactory.getLogger(ServicenowSinkApiImp.class);
  private ServiceNowTableAPIClientImpl restApi;
  private String accessToken;
  //private  ServiceNowSinkConfig config;
  //Integer counter = 1;

  public ServicenowSinkApiImp(ServiceNowSinkConfig conf) {

    this.config = conf;
  }
 /* public void initialize(TaskAttemptContext taskAttemptContext) throws OAuthProblemException, OAuthSystemException {
    Configuration conf = taskAttemptContext.getConfiguration();
    restApi = new ServiceNowTableAPIClientImpl(conf);
    accessToken = restApi.getAccessToken();
  }*/
  protected Request getRecords(JsonObject jsonObject) {
    Integer counter = 1;
    String data = jsonObject.toString();
    String encodedData = Base64.getEncoder().encodeToString(data.getBytes(StandardCharsets.UTF_8));

    Map<String, String> header = new HashMap<>();
    header.put("Content-Type", "application/json");
    header.put("Accept", "application/json");
    Request request = new Request();

    request.setUrl("/api/now/table/" + config.getTableName());
    request.setId(counter.toString());
    counter++;
    request.setHeader(header);
    request.setMethod("POST");
    request.setBody(encodedData);
    return request;
    //records.add(request);
    //return records;
  }

  protected  RestAPIResponse createPostRequest(List<Request> records) {
    PayloadRequest payloadRequest = getPayloadRequest(records);
    ServiceNowTableAPIRequestBuilder requestBuilder = new ServiceNowTableAPIRequestBuilder(
      getBatchApiUrl().toString(), config.getTableName());
    RestAPIResponse apiResponse = null;

    try {
      restApi = new ServiceNowTableAPIClientImpl(config);
      String accessToken = restApi.getAccessToken();
      requestBuilder.setAuthHeader(accessToken);
      requestBuilder.setAcceptHeader("application/json");
      requestBuilder.setContentTypeHeader("application/json");
      Gson gson = new Gson();
      StringEntity stringEntity = new StringEntity(gson.toJson(payloadRequest));
      requestBuilder.setEntity(stringEntity);
      apiResponse = restApi.executePost(requestBuilder.build());
      if (!apiResponse.isSuccess()) {
        LOG.error("Error - {}", getErrorMessage(apiResponse.getResponseBody()));
      } else {
        LOG.info(apiResponse.getResponseBody().toString());
      }
    } catch (OAuthSystemException | OAuthProblemException | UnsupportedEncodingException e) {
      LOG.error("Error in creating a new record", e);
      throw new RuntimeException("Error in creating a new record");
    }
    return apiResponse;
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

  public PayloadRequest getPayloadRequest(List<Request> requests) {
    PayloadRequest payloadRequest = new PayloadRequest();
    payloadRequest.setId("1");
    payloadRequest.setRecords(requests);

    return  payloadRequest;
  }

  public URL getBatchApiUrl() {
    URL instanceURL = HttpUrl.parse(config.getRestApiEndpoint())
      .newBuilder()
      /*.addPathSegments("api")
      .addPathSegment("now")
      .addPathSegment("v1")
      .addPathSegment("batch")*/
      .build()
      .url();
    return instanceURL;
  }
  }

