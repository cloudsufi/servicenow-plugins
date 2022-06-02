package io.cdap.plugin.servicenow.sink;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.plugin.servicenow.restapi.RestAPIResponse;
import io.cdap.plugin.servicenow.source.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.source.apiclient.ServiceNowTableAPIRequestBuilder;
import okhttp3.HttpUrl;
import org.apache.http.Header;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BufferedHeader;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class ServiceNowSinkAPIImpl {

  private ServiceNowSinkConfig config;
  private static final Logger LOG = LoggerFactory.getLogger(ServiceNowSinkAPIImpl.class);
  private ServiceNowTableAPIClientImpl restApi;
  private static final String TABLE_API_URL_TEMPLATE = "/api/now/table/%s";
  private static Integer counter = 1;

  public ServiceNowSinkAPIImpl(ServiceNowSinkConfig conf) {
    this.config = conf;
  }

 /* public void initialize(TaskAttemptContext taskAttemptContext) throws OAuthProblemException, OAuthSystemException {
    Configuration conf = taskAttemptContext.getConfiguration();
    restApi = new ServiceNowTableAPIClientImpl(conf);
    accessToken = restApi.getAccessToken();
  }*/

  protected RestRequest getRestRequest(JsonObject jsonObject) {

    String data = jsonObject.toString();
    String encodedData = Base64.getEncoder().encodeToString(data.getBytes(StandardCharsets.UTF_8));

    List<Header> headers = new ArrayList<>();
    Header contentTypeHeader = new BasicHeader("Content-Type", "application/json");
    Header acceptHeader = new BasicHeader("Accept", "application/json");
    headers.add(contentTypeHeader);
    headers.add(acceptHeader);

    /*Map<String, String> header = new HashMap<>();
    header.put("Content-Type", "application/json");
    header.put("Accept", "application/json");*/
    RestRequest restRequest = new RestRequest();

    restRequest.setUrl(String.format(TABLE_API_URL_TEMPLATE, config.getTableName()));
    //restRequest.setUrl("/api/now/table/" + config.getTableName());
    restRequest.setId(counter.toString());
    counter++;
    restRequest.setHeaders(headers);
    restRequest.setMethod("POST");
    restRequest.setBody(encodedData);
    return restRequest;
  }

  protected RestAPIResponse createPostRequest(List<RestRequest> records) {
    PayloadRequest payloadRequest = getPayloadRequest(records);
    ServiceNowTableAPIRequestBuilder requestBuilder = new ServiceNowTableAPIRequestBuilder(
      config.getRestApiEndpoint());
    RestAPIResponse apiResponse;

    try {
      restApi = new ServiceNowTableAPIClientImpl(config);
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
      } else {
        LOG.info(apiResponse.getResponseBody());
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

  public PayloadRequest getPayloadRequest(List<RestRequest> restRequests) {
    PayloadRequest payloadRequest = new PayloadRequest();
    payloadRequest.setBatchRequestId("1");
    payloadRequest.setRestRequests(restRequests);

    return payloadRequest;
  }

  public URL getBatchApiUrl() {
    URL instanceURL = HttpUrl.parse(config.getRestApiEndpoint())
      .newBuilder()
      .addPathSegments("api")
      .addPathSegment("now")
      .addPathSegment("v1")
      .addPathSegment("batch")
      .build()
      .url();
    return instanceURL;
  }
}
