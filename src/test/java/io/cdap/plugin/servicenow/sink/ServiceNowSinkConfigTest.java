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
package io.cdap.plugin.servicenow.sink;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.servicenow.ServiceNowBaseConfig;
import io.cdap.plugin.servicenow.restapi.RestAPIClient;
import io.cdap.plugin.servicenow.restapi.RestAPIResponse;
import io.cdap.plugin.servicenow.source.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.source.util.ServiceNowConstants;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.oltu.oauth2.client.OAuthClient;
import org.apache.oltu.oauth2.client.URLConnectionClient;
import org.apache.oltu.oauth2.client.response.OAuthJSONAccessTokenResponse;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PrepareForTest({RestAPIClient.class, HttpClientBuilder.class, RestAPIResponse.class})
public class ServiceNowSinkConfigTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testValidateClientIdNull() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                 .setClientId(null).build(), collector);
    try {
      config.validate(collector);
      collector.getOrThrowException();
      Assert.fail("Exception is not thrown with valid client id");
    } catch (ValidationException e) {
      Assert.assertEquals(ServiceNowConstants.PROPERTY_CLIENT_ID, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidateClientSecretNull() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                 .setClientSecret(null).build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
      Assert.fail("Exception is not thrown with valid client secret");
    } catch (ValidationException e) {
      Assert.assertEquals(ServiceNowConstants.PROPERTY_CLIENT_SECRET, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidateApiEndpointNull() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                 .setRestApiEndpoint(null).build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
      Assert.fail("Exception is not thrown with valid Api Endpoint");
    } catch (ValidationException e) {
      Assert.assertEquals(ServiceNowConstants.PROPERTY_API_ENDPOINT, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidateUserNull() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                 .setUser(null).build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
      Assert.fail("Exception is not thrown with valid User");
    } catch (ValidationException e) {
      Assert.assertEquals(ServiceNowConstants.PROPERTY_USER, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidatePasswordNull() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                 .setPassword(null).build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
      Assert.fail("Exception is not thrown with valid password");
    } catch (ValidationException e) {
      Assert.assertEquals(ServiceNowConstants.PROPERTY_PASSWORD, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidTableName() {
    ServiceNowSinkConfig config = ServiceNowSinkConfigHelper.newConfigBuilder()
      .setTableName("Table").setOperation(null).build();
    Assert.assertEquals("Table", config.getTableName());
  }

  @Test
  public void testEmptyTableName() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    ServiceNowSinkConfig config = withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                 .setMaxRecordPerBatch(200L).setTableName("")
                                                                 .build(), mockFailureCollector);
    config.validate(mockFailureCollector);
    List<ValidationFailure> validationFailures = mockFailureCollector.getValidationFailures();
    ValidationFailure getResult = validationFailures.get(0);
    Assert.assertEquals("Table name must be specified.", getResult.getMessage());
  }

  @Test
  public void testBatchSizeNull() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    ServiceNowSinkConfig config = withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                 .setMaxRecordPerBatch(null).build(),
                                                                  mockFailureCollector);
    config.validate(mockFailureCollector);
    Assert.assertEquals(0L, config.getMaxRecordsPerBatch().intValue());
  }

  @Test
  public void testInvalidBatchSize() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                 .setMaxRecordPerBatch(600L).build(), collector);
    try {
      config.validateMaxRecordsPerBatch(collector);
      collector.getOrThrowException();
      Assert.fail("Exception is not thrown with valid batch Size is provided");
    } catch (ValidationException e) {
      Assert.assertEquals(ServiceNowConstants.PROPERTY_MAX_RECORDS_PER_BATCH, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testOperation() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder().
                                                                 setOperation("Insert").setMaxRecordPerBatch(200L).
                                                                 build(), collector);
    Assert.assertEquals("Insert", config.getOperation());
  }

  @Test
  public void testCheckCompatibilitySuccess() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setTableName("tableName")
                                                                             .build(), collector));
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("Id", Schema.of(Schema.Type.INT)),
                                          Schema.Field.of("StartDate", Schema.nullableOf(Schema.of(Schema.
                                                                                                     LogicalType.DATE)))
      ,
                                          Schema.Field.of("ExtraField", Schema.of(Schema.Type.STRING)),
                                          Schema.Field.of("Comment", Schema.of(Schema.Type.STRING)));

    Schema providedSchema = Schema.recordOf("providedSchema",
                                            Schema.Field.of("Id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("StartDate", Schema.nullableOf(Schema.of(Schema
                                                                                                       .LogicalType.
                                                                                                       DATE))),
                                            Schema.Field.of("Comment", Schema.nullableOf(Schema.of
                                              (Schema.Type.STRING))));
    config.checkCompatibility(actualSchema, providedSchema, true);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testCheckCompatibilityMissingField() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setTableName("tableName")
                                                                             .build(), collector));
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("Comment", Schema.of(Schema.Type.STRING)));
    Schema providedSchema = Schema.recordOf("providedSchema",
                                            Schema.Field.of("Id", Schema.of(Schema.Type.INT)));
    thrown.expect(IllegalArgumentException.class);
    config.checkCompatibility(actualSchema, providedSchema, true);
  }

  @Test
  public void testCheckCompatibilityIncorrectType() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setTableName("tableName")
                                                                             .build(), collector));
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("Id", Schema.of(Schema.Type.STRING)));

    Schema providedSchema = Schema.recordOf("providedSchema",
                                            Schema.Field.of("Id", Schema.of(Schema.Type.INT)));

    thrown.expect(IllegalArgumentException.class);
    config.getSchema(collector);
    config.checkCompatibility(actualSchema, providedSchema, true);
  }

  @Test
  public void testCheckCompatibilityIncorrectLogicalType() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setTableName("tableName")
                                                                             .build(), collector));
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("CreatedDateTime", Schema.of(Schema.LogicalType.
                                                                                         TIMESTAMP_MICROS)));

    Schema providedSchema = Schema.recordOf("providedSchema",
                                            Schema.Field.of("CreatedDateTime", Schema.of(Schema.LogicalType.
                                                                                           TIME_MICROS)));
    thrown.expect(IllegalArgumentException.class);
    config.checkCompatibility(actualSchema, providedSchema, true);
  }

  @Test
  public void testCheckCompatibilityIncorrectNullability() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setTableName("tableName")
                                                                             .build(), collector));
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("Id", Schema.nullableOf(Schema.of(Schema.Type.INT))));

    Schema providedSchema = Schema.recordOf("providedSchema",
                                            Schema.Field.of("Id", Schema.of(Schema.Type.INT)));
    thrown.expect(IllegalArgumentException.class);
    config.checkCompatibility(actualSchema, providedSchema, true);
  }

  @Test
  public void testValidateSchema() throws Exception {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setTableName("tableName")
                                                                             .setOperation("insert")
                                                                             .build(), collector));
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withParameterTypes(ServiceNowBaseConfig.class)
      .withArguments(Mockito.any(ServiceNowBaseConfig.class)).thenReturn(restApi);
    List<Map<String, Object>> result = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    map.put("key", "value");
    result.add(map);
    int httpStatus = HttpStatus.SC_OK;
    Map<String, String> headers = new HashMap<>();
    String responseBody = "{\n" +
      "    \"result\": [\n" +
      "        {\n" +
      "            \"calendar_integration\": \"1\",\n" +
      "            \"country\": \"\",\n" +
      "            \"date_format\": \"\",\n" +
      "            \"location\": \"\"\n" +
      "        }\n" +
      "    ]\n" +
      "}";
    RestAPIResponse restAPIResponse = new RestAPIResponse(httpStatus, headers, responseBody);
    Mockito.when(restApi.executeGet(Mockito.any())).thenReturn(restAPIResponse);
    Mockito.when(restApi.parseResponseToResultListOfMap(restAPIResponse.getResponseBody())).thenReturn(result);
    OAuthClient oAuthClient = Mockito.mock(OAuthClient.class);
    PowerMockito.whenNew(OAuthClient.class).
      withArguments(Mockito.any(URLConnectionClient.class)).thenReturn(oAuthClient);
    OAuthJSONAccessTokenResponse accessTokenResponse = Mockito.mock(OAuthJSONAccessTokenResponse.class);
    Mockito.when(oAuthClient.accessToken(Mockito.any(), Mockito.anyString(), Mockito.any(Class.class))).
      thenReturn(accessTokenResponse);
    Mockito.when(accessTokenResponse.getAccessToken()).thenReturn("token");
    RestAPIResponse response = Mockito.spy(restAPIResponse);
    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    HttpClientBuilder httpClientBuilder = Mockito.mock(HttpClientBuilder.class);
    PowerMockito.mockStatic(HttpClientBuilder.class);
    PowerMockito.mockStatic(RestAPIResponse.class);
    PowerMockito.when(HttpClientBuilder.create()).thenReturn(httpClientBuilder);
    Mockito.when(httpClientBuilder.build()).thenReturn(httpClient);
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);
    PowerMockito.when(RestAPIResponse.parse(ArgumentMatchers.any(), ArgumentMatchers.anyString())).
      thenReturn(response);
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("country", Schema.of(Schema.Type.STRING)));
    config.validateSchema(actualSchema, collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateSchemaWithInvalidOperation() throws Exception {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setTableName("tableName")
                                                                             .setOperation("operation")
                                                                             .build(), collector));
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withParameterTypes(ServiceNowBaseConfig.class)
      .withArguments(Mockito.any(ServiceNowBaseConfig.class)).thenReturn(restApi);
    List<Map<String, Object>> result = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    map.put("key", "value");
    result.add(map);
    int httpStatus = HttpStatus.SC_OK;
    Map<String, String> headers = new HashMap<>();
    String responseBody = "{\n" +
      "    \"result\": [\n" +
      "        {\n" +
      "            \"calendar_integration\": \"1\",\n" +
      "            \"country\": \"\",\n" +
      "            \"date_format\": \"\",\n" +
      "            \"location\": \"\"\n" +
      "        }\n" +
      "    ]\n" +
      "}";
    RestAPIResponse restAPIResponse = new RestAPIResponse(httpStatus, headers, responseBody);
    Mockito.when(restApi.executeGet(Mockito.any())).thenReturn(restAPIResponse);
    Mockito.when(restApi.parseResponseToResultListOfMap(restAPIResponse.getResponseBody())).thenReturn(result);
    OAuthClient oAuthClient = Mockito.mock(OAuthClient.class);
    PowerMockito.whenNew(OAuthClient.class).
      withArguments(Mockito.any(URLConnectionClient.class)).thenReturn(oAuthClient);
    OAuthJSONAccessTokenResponse accessTokenResponse = Mockito.mock(OAuthJSONAccessTokenResponse.class);
    Mockito.when(oAuthClient.accessToken(Mockito.any(), Mockito.anyString(), Mockito.any(Class.class))).
      thenReturn(accessTokenResponse);
    Mockito.when(accessTokenResponse.getAccessToken()).thenReturn("token");
    RestAPIResponse response = Mockito.spy(restAPIResponse);
    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    HttpClientBuilder httpClientBuilder = Mockito.mock(HttpClientBuilder.class);
    PowerMockito.mockStatic(HttpClientBuilder.class);
    PowerMockito.mockStatic(RestAPIResponse.class);
    PowerMockito.when(HttpClientBuilder.create()).thenReturn(httpClientBuilder);
    Mockito.when(httpClientBuilder.build()).thenReturn(httpClient);
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);
    PowerMockito.when(RestAPIResponse.parse(ArgumentMatchers.any(), ArgumentMatchers.anyString())).
      thenReturn(response);
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("country", Schema.of(Schema.Type.STRING)));
    try {
      config.validateSchema(actualSchema, collector);
      collector.getOrThrowException();
      Assert.fail("Exception is not thrown if operation is provided");
    } catch (ValidationException e) {
      Assert.assertEquals("Errors were encountered during validation. Unsupported value for operation: " +
                            "operation", e.getMessage());
    }
  }

  @Test
  public void testValidateSchemaWithUpdateOperation() throws Exception {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setTableName("tableName")
                                                                             .setOperation("update")
                                                                             .build(), collector));
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withParameterTypes(ServiceNowBaseConfig.class)
      .withArguments(Mockito.any(ServiceNowBaseConfig.class)).thenReturn(restApi);
    List<Map<String, Object>> result = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    map.put("key", "value");
    result.add(map);
    int httpStatus = HttpStatus.SC_OK;
    Map<String, String> headers = new HashMap<>();
    String responseBody = "{\n" +
      "    \"result\": [\n" +
      "        {\n" +
      "            \"calendar_integration\": \"1\",\n" +
      "            \"sys_id\": \"\",\n" +
      "            \"date_format\": \"\",\n" +
      "            \"location\": \"\"\n" +
      "        }\n" +
      "    ]\n" +
      "}";
    RestAPIResponse restAPIResponse = new RestAPIResponse(httpStatus, headers, responseBody);
    Mockito.when(restApi.executeGet(Mockito.any())).thenReturn(restAPIResponse);
    Mockito.when(restApi.parseResponseToResultListOfMap(restAPIResponse.getResponseBody())).thenReturn(result);
    OAuthClient oAuthClient = Mockito.mock(OAuthClient.class);
    PowerMockito.whenNew(OAuthClient.class).
      withArguments(Mockito.any(URLConnectionClient.class)).thenReturn(oAuthClient);
    OAuthJSONAccessTokenResponse accessTokenResponse = Mockito.mock(OAuthJSONAccessTokenResponse.class);
    Mockito.when(oAuthClient.accessToken(Mockito.any(), Mockito.anyString(), Mockito.any(Class.class))).
      thenReturn(accessTokenResponse);
    Mockito.when(accessTokenResponse.getAccessToken()).thenReturn("token");
    RestAPIResponse response = Mockito.spy(restAPIResponse);
    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    HttpClientBuilder httpClientBuilder = Mockito.mock(HttpClientBuilder.class);
    PowerMockito.mockStatic(HttpClientBuilder.class);
    PowerMockito.mockStatic(RestAPIResponse.class);
    PowerMockito.when(HttpClientBuilder.create()).thenReturn(httpClientBuilder);
    Mockito.when(httpClientBuilder.build()).thenReturn(httpClient);
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);
    PowerMockito.when(RestAPIResponse.parse(ArgumentMatchers.any(), ArgumentMatchers.anyString())).
      thenReturn(response);
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("sys_id", Schema.of(Schema.Type.STRING)));
    config.validateSchema(actualSchema, collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());

  }

  @Test
  public void testValidateSchemaWithUpdateOperationEmptySysId() throws Exception {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setTableName("tableName")
                                                                             .setOperation("update")
                                                                             .build(), collector));
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withParameterTypes(ServiceNowBaseConfig.class)
      .withArguments(Mockito.any(ServiceNowBaseConfig.class)).thenReturn(restApi);
    List<Map<String, Object>> result = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    map.put("key", "value");
    result.add(map);
    int httpStatus = HttpStatus.SC_OK;
    Map<String, String> headers = new HashMap<>();
    String responseBody = "{\n" +
      "    \"result\": [\n" +
      "        {\n" +
      "            \"calendar_integration\": \"1\",\n" +
      "            \"country\": \"\",\n" +
      "            \"date_format\": \"\",\n" +
      "            \"location\": \"\"\n" +
      "        }\n" +
      "    ]\n" +
      "}";
    RestAPIResponse restAPIResponse = new RestAPIResponse(httpStatus, headers, responseBody);
    Mockito.when(restApi.executeGet(Mockito.any())).thenReturn(restAPIResponse);
    Mockito.when(restApi.parseResponseToResultListOfMap(restAPIResponse.getResponseBody())).thenReturn(result);
    OAuthClient oAuthClient = Mockito.mock(OAuthClient.class);
    PowerMockito.whenNew(OAuthClient.class).
      withArguments(Mockito.any(URLConnectionClient.class)).thenReturn(oAuthClient);
    OAuthJSONAccessTokenResponse accessTokenResponse = Mockito.mock(OAuthJSONAccessTokenResponse.class);
    Mockito.when(oAuthClient.accessToken(Mockito.any(), Mockito.anyString(), Mockito.any(Class.class))).
      thenReturn(accessTokenResponse);
    Mockito.when(accessTokenResponse.getAccessToken()).thenReturn("token");
    RestAPIResponse response = Mockito.spy(restAPIResponse);
    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    HttpClientBuilder httpClientBuilder = Mockito.mock(HttpClientBuilder.class);
    PowerMockito.mockStatic(HttpClientBuilder.class);
    PowerMockito.mockStatic(RestAPIResponse.class);
    PowerMockito.when(HttpClientBuilder.create()).thenReturn(httpClientBuilder);
    Mockito.when(httpClientBuilder.build()).thenReturn(httpClient);
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);
    PowerMockito.when(RestAPIResponse.parse(ArgumentMatchers.any(), ArgumentMatchers.anyString())).
      thenReturn(response);
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("country", Schema.of(Schema.Type.STRING)));
    try {
      config.validateSchema(actualSchema, collector);
      collector.getOrThrowException();
      Assert.fail("Exception is not thrown if sys_id is present in the schema");
    } catch (ValidationException e) {
      Assert.assertEquals("Errors were encountered during validation. Schema must contain external id field " +
                            "'sys_id'", e.getMessage());
    }
  }

  @Test
  public void testValidateSchemaWithInvalidSchemaField() throws Exception {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setTableName("tableName")
                                                                             .setOperation("insert")
                                                                             .build(), collector));
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withParameterTypes(ServiceNowBaseConfig.class)
      .withArguments(Mockito.any(ServiceNowBaseConfig.class)).thenReturn(restApi);
    List<Map<String, Object>> result = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    map.put("key", "value");
    result.add(map);
    int httpStatus = HttpStatus.SC_OK;
    Map<String, String> headers = new HashMap<>();
    String responseBody = "{\n" +
      "    \"result\": [\n" +
      "        {\n" +
      "            \"location\": \"\"\n" +
      "        }\n" +
      "    ]\n" +
      "}";
    RestAPIResponse restAPIResponse = new RestAPIResponse(httpStatus, headers, responseBody);
    Mockito.when(restApi.executeGet(Mockito.any())).thenReturn(restAPIResponse);
    Mockito.when(restApi.parseResponseToResultListOfMap(restAPIResponse.getResponseBody())).thenReturn(result);
    OAuthClient oAuthClient = Mockito.mock(OAuthClient.class);
    PowerMockito.whenNew(OAuthClient.class).
      withArguments(Mockito.any(URLConnectionClient.class)).thenReturn(oAuthClient);
    OAuthJSONAccessTokenResponse accessTokenResponse = Mockito.mock(OAuthJSONAccessTokenResponse.class);
    Mockito.when(oAuthClient.accessToken(Mockito.any(), Mockito.anyString(), Mockito.any(Class.class))).
      thenReturn(accessTokenResponse);
    Mockito.when(accessTokenResponse.getAccessToken()).thenReturn("token");
    RestAPIResponse response = Mockito.spy(restAPIResponse);
    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
    HttpClientBuilder httpClientBuilder = Mockito.mock(HttpClientBuilder.class);
    PowerMockito.mockStatic(HttpClientBuilder.class);
    PowerMockito.mockStatic(RestAPIResponse.class);
    PowerMockito.when(HttpClientBuilder.create()).thenReturn(httpClientBuilder);
    Mockito.when(httpClientBuilder.build()).thenReturn(httpClient);
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);
    PowerMockito.when(RestAPIResponse.parse(ArgumentMatchers.any(), ArgumentMatchers.anyString())).
      thenReturn(response);
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("location", Schema.of(Schema.Type.STRING)),
                                          Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    try {
      config.validateSchema(actualSchema, collector);
      collector.getOrThrowException();
      Assert.fail("Exception is not thrown if name is present in Servicenow table");
    } catch (ValidationException e) {
      Assert.assertEquals("Errors were encountered during validation. Field 'name' is not present or not " +
                            "creatable in target ServiceNow table.", e.getMessage());
    }
  }

  @Test
  public void testGetSchemaNullFields() throws Exception {

    Schema schema = Schema.of(Schema.LogicalType.DATE);
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setTableName("tableName")
                                                                             .setOperation("insert")
                                                                             .build(), collector));
    try {
      config.validateSchema(schema, collector);
      collector.getOrThrowException();
      Assert.fail("Exception is not thrown if fields are provided");
    } catch (ValidationException e) {
      Assert.assertEquals("Errors were encountered during validation. Sink schema must contain at " +
                            "least one field", e.getMessage());
    }
  }

  private ServiceNowSinkConfig withServiceNowValidationMock(ServiceNowSinkConfig config,
                                                            FailureCollector collector) {
    ServiceNowSinkConfig spy = Mockito.spy(config);
    Mockito.doNothing().when(spy).validateServiceNowConnection(collector);
    return spy;
  }
}
