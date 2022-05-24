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
import io.cdap.plugin.servicenow.source.ServiceNowSourceConfigHelper;
import io.cdap.plugin.servicenow.source.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.source.util.ServiceNowConstants;
import org.apache.commons.httpclient.HttpStatus;
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
                                                                 .setClientId(null)
                                                                 .setClientSecret(ServiceNowSourceConfigHelper.TEST_CLIENT_SECRET)
                                                                 .setRestApiEndpoint(ServiceNowSourceConfigHelper.TEST_API_ENDPOINT)
                                                                 .setUser(ServiceNowSourceConfigHelper.TEST_USER)
                                                                 .setPassword(ServiceNowSourceConfigHelper.TEST_PASSWORD)
                                                                 .build(), collector);
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
                                                                 .setClientId(ServiceNowSourceConfigHelper.TEST_CLIENT_ID)
                                                                 .setClientSecret(null)
                                                                 .setRestApiEndpoint(ServiceNowSourceConfigHelper.TEST_API_ENDPOINT)
                                                                 .setUser(ServiceNowSourceConfigHelper.TEST_USER)
                                                                 .setPassword(ServiceNowSourceConfigHelper.TEST_PASSWORD)
                                                                 .build(), collector);

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
                                                                 .setClientId(ServiceNowSourceConfigHelper.TEST_CLIENT_ID)
                                                                 .setClientSecret(ServiceNowSourceConfigHelper.TEST_CLIENT_SECRET)
                                                                 .setRestApiEndpoint(null)
                                                                 .setUser(ServiceNowSourceConfigHelper.TEST_USER)
                                                                 .setPassword(ServiceNowSourceConfigHelper.TEST_PASSWORD)
                                                                 .build(), collector);

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
                                                                 .setClientId(ServiceNowSourceConfigHelper.TEST_CLIENT_ID)
                                                                 .setClientSecret(ServiceNowSourceConfigHelper.TEST_CLIENT_SECRET)
                                                                 .setRestApiEndpoint(ServiceNowSourceConfigHelper.TEST_API_ENDPOINT)
                                                                 .setUser(null)
                                                                 .setPassword(ServiceNowSourceConfigHelper.TEST_PASSWORD)
                                                                 .build(), collector);

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
                                                                 .setClientId(ServiceNowSourceConfigHelper.TEST_CLIENT_ID)
                                                                 .setClientSecret(ServiceNowSourceConfigHelper.TEST_CLIENT_SECRET)
                                                                 .setRestApiEndpoint(ServiceNowSourceConfigHelper.TEST_API_ENDPOINT)
                                                                 .setUser(ServiceNowSourceConfigHelper.TEST_USER)
                                                                 .setPassword(null)
                                                                 .build(), collector);

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
  public void testInValidCredentials() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                 .setClientId(ServiceNowSourceConfigHelper.TEST_CLIENT_ID)
                                                                 .setClientSecret(ServiceNowSourceConfigHelper.TEST_CLIENT_SECRET)
                                                                 .setRestApiEndpoint((ServiceNowSourceConfigHelper.TEST_API_ENDPOINT))
                                                                 .setUser(ServiceNowSourceConfigHelper.TEST_USER)
                                                                 .setPassword(ServiceNowSourceConfigHelper.TEST_PASSWORD)
                                                                 .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
      Assert.fail("Exception is not thrown with valid credentials");
    } catch (ValidationException e) {
      Assert.assertEquals("Errors were encountered during validation.", e.getMessage());
    }
  }

  @Test
  public void testValidTableName() {
    ServiceNowSinkConfig config = ServiceNowSinkConfigHelper.newConfigBuilder()
      .setTableName("Table").setOperation(null).build();
    Assert.assertEquals("Table", config.getTableName());
  }

  @Test
  public void testEmptyTablename() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    ServiceNowSinkConfig config = withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                 .setClientId(ServiceNowSourceConfigHelper.TEST_CLIENT_ID)
                                                                 .setClientSecret(ServiceNowSourceConfigHelper.TEST_CLIENT_SECRET)
                                                                 .setRestApiEndpoint(ServiceNowSourceConfigHelper.TEST_API_ENDPOINT)
                                                                 .setUser(ServiceNowSourceConfigHelper.TEST_USER)
                                                                 .setPassword(ServiceNowSourceConfigHelper.TEST_PASSWORD)
                                                                 .setPageSize(200L)
                                                                 .setTableName("")
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
                                                                 .setClientId(ServiceNowSourceConfigHelper.TEST_CLIENT_ID)
                                                                 .setClientSecret(ServiceNowSourceConfigHelper.TEST_CLIENT_SECRET)
                                                                 .setRestApiEndpoint(ServiceNowSourceConfigHelper.TEST_API_ENDPOINT)
                                                                 .setUser(ServiceNowSourceConfigHelper.TEST_USER)
                                                                 .setPassword(ServiceNowSourceConfigHelper.TEST_PASSWORD)
                                                                 .setPageSize(null)
                                                                 .build(), mockFailureCollector);
    config.validate(mockFailureCollector);
    List<ValidationFailure> validationFailures = mockFailureCollector.getValidationFailures();
    ValidationFailure getResult = validationFailures.get(0);
    Assert.assertEquals(0L, config.getPageSize().intValue());
  }

  @Test
  public void testInvalidBatchSize() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                 .setClientId(ServiceNowSourceConfigHelper.TEST_CLIENT_ID)
                                                                 .setClientSecret(ServiceNowSourceConfigHelper.TEST_CLIENT_SECRET)
                                                                 .setRestApiEndpoint(ServiceNowSourceConfigHelper.TEST_API_ENDPOINT)
                                                                 .setUser(ServiceNowSourceConfigHelper.TEST_USER)
                                                                 .setPassword(ServiceNowSourceConfigHelper.TEST_PASSWORD)
                                                                 .setPageSize(600L)
                                                                 .build(), collector);
    try {
      config.validatePageSize(collector);
      collector.getOrThrowException();
      Assert.fail("Exception is not thrown with valid table name is provided");
    } catch (ValidationException e) {
      Assert.assertEquals(ServiceNowConstants.PROPERTY_PAGE_SIZE, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testOperation() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder().
                                                                 setOperation("Insert").setPageSize(200L).build(),
                                                               collector);
    Assert.assertEquals("Insert", config.getOperation());
  }

  private ServiceNowSinkConfig withServiceNowValidationMock(ServiceNowSinkConfig config,
                                                            FailureCollector collector) {
    ServiceNowSinkConfig spy = Mockito.spy(config);
    Mockito.doNothing().when(spy).validateServiceNowConnection(collector);
    return spy;
  }

  @Test
  public void testCheckCompatibilitySuccess() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setClientId(ServiceNowSourceConfigHelper.TEST_CLIENT_ID)
                                                                             .setClientSecret(ServiceNowSourceConfigHelper.TEST_CLIENT_SECRET)
                                                                             .setRestApiEndpoint((ServiceNowSourceConfigHelper.TEST_API_ENDPOINT))
                                                                             .setUser(ServiceNowSourceConfigHelper.TEST_USER)
                                                                             .setPassword(ServiceNowSourceConfigHelper.TEST_PASSWORD)
                                                                             .build(), collector));
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("Id", Schema.of(Schema.Type.INT)),
                                          Schema.Field.of("StartDate", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                                          Schema.Field.of("ExtraField", Schema.of(Schema.Type.STRING)),
                                          Schema.Field.of("Comment", Schema.of(Schema.Type.STRING)));

    Schema providedSchema = Schema.recordOf("providedSchema",
                                            Schema.Field.of("Id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("StartDate", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                                            Schema.Field.of("Comment", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    config.checkCompatibility(actualSchema, providedSchema, true);
  }

  @Test
  public void testCheckCompatibilityMissingField() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setClientId(ServiceNowSourceConfigHelper.TEST_CLIENT_ID)
                                                                             .setClientSecret(ServiceNowSourceConfigHelper.TEST_CLIENT_SECRET)
                                                                             .setRestApiEndpoint((ServiceNowSourceConfigHelper.TEST_API_ENDPOINT))
                                                                             .setUser(ServiceNowSourceConfigHelper.TEST_USER)
                                                                             .setPassword(ServiceNowSourceConfigHelper.TEST_PASSWORD)
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
                                                                             .setClientId(ServiceNowSourceConfigHelper.TEST_CLIENT_ID)
                                                                             .setClientSecret(ServiceNowSourceConfigHelper.TEST_CLIENT_SECRET)
                                                                             .setRestApiEndpoint((ServiceNowSourceConfigHelper.TEST_API_ENDPOINT))
                                                                             .setUser(ServiceNowSourceConfigHelper.TEST_USER)
                                                                             .setPassword(ServiceNowSourceConfigHelper.TEST_PASSWORD)
                                                                             .build(), collector));
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("Id", Schema.of(Schema.Type.STRING)));

    Schema providedSchema = Schema.recordOf("providedSchema",
                                            Schema.Field.of("Id", Schema.of(Schema.Type.INT)));

    thrown.expect(IllegalArgumentException.class);

    config.checkCompatibility(actualSchema, providedSchema, true);
  }

  @Test
  public void testCheckCompatibilityIncorrectLogicalType() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setClientId(ServiceNowSourceConfigHelper.TEST_CLIENT_ID)
                                                                             .setClientSecret(ServiceNowSourceConfigHelper.TEST_CLIENT_SECRET)
                                                                             .setRestApiEndpoint((ServiceNowSourceConfigHelper.TEST_API_ENDPOINT))
                                                                             .setUser(ServiceNowSourceConfigHelper.TEST_USER)
                                                                             .setPassword(ServiceNowSourceConfigHelper.TEST_PASSWORD)
                                                                             .build(), collector));
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("CreatedDateTime", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)));

    Schema providedSchema = Schema.recordOf("providedSchema",
                                            Schema.Field.of("CreatedDateTime", Schema.of(Schema.LogicalType.TIME_MICROS)));

    thrown.expect(IllegalArgumentException.class);

    config.checkCompatibility(actualSchema, providedSchema, true);
  }

  @Test
  public void testCheckCompatibilityIncorrectNullability() {
    MockFailureCollector collector = new MockFailureCollector();
    ServiceNowSinkConfig config = Mockito.spy(withServiceNowValidationMock(ServiceNowSinkConfigHelper.newConfigBuilder()
                                                                             .setClientId(ServiceNowSourceConfigHelper.TEST_CLIENT_ID)
                                                                             .setClientSecret(ServiceNowSourceConfigHelper.TEST_CLIENT_SECRET)
                                                                             .setRestApiEndpoint((ServiceNowSourceConfigHelper.TEST_API_ENDPOINT))
                                                                             .setUser(ServiceNowSourceConfigHelper.TEST_USER)
                                                                             .setPassword(ServiceNowSourceConfigHelper.TEST_PASSWORD)
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
                                                                             .setClientId(ServiceNowSourceConfigHelper.TEST_CLIENT_ID)
                                                                             .setClientSecret(ServiceNowSourceConfigHelper.TEST_CLIENT_SECRET)
                                                                             .setRestApiEndpoint((ServiceNowSourceConfigHelper.TEST_API_ENDPOINT))
                                                                             .setUser(ServiceNowSourceConfigHelper.TEST_USER)
                                                                             .setPassword(ServiceNowSourceConfigHelper.TEST_PASSWORD)
                                                                             .setTableName("tablename")
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
      "            \"last_login_time\": \"2019-04-05 22:16:30\",\n" +
      "            \"source\": \"\",\n" +
      "            \"sys_updated_on\": \"2019-04-05 21:54:45\",\n" +
      "            \"building\": \"\",\n" +
      "            \"web_service_access_only\": \"false\",\n" +
      "            \"notification\": \"2\",\n" +
      "            \"enable_multifactor_authn\": \"false\",\n" +
      "            \"sys_updated_by\": \"system\",\n" +
      "            \"sys_created_on\": \"2019-04-05 21:09:12\",\n" +
      "            \"sys_domain\": {\n" +
      "                \"link\": \"https://ven05127.service-now.com/api/now/table/sys_user_group/global\",\n" +
      "                \"value\": \"global\"\n" +
      "            },\n" +
      "            \"state\": \"\",\n" +
      "            \"vip\": \"false\",\n" +
      "            \"sys_created_by\": \"admin\",\n" +
      "            \"zip\": \"\",\n" +
      "            \"home_phone\": \"\",\n" +
      "            \"time_format\": \"\",\n" +
      "            \"last_login\": \"2019-04-05\",\n" +
      "            \"active\": \"true\",\n" +
      "            \"sys_domain_path\": \"/\",\n" +
      "            \"cost_center\": \"\",\n" +
      "            \"phone\": \"\",\n" +
      "            \"name\": \"survey user\",\n" +
      "            \"employee_number\": \"\",\n" +
      "            \"gender\": \"\",\n" +
      "            \"city\": \"\",\n" +
      "            \"failed_attempts\": \"0\",\n" +
      "            \"user_name\": \"survey.user\",\n" +
      "            \"title\": \"\",\n" +
      "            \"sys_class_name\": \"sys_user\",\n" +
      "            \"sys_id\": \"005d500b536073005e0addeeff7b12f4\",\n" +
      "            \"internal_integration_user\": \"false\",\n" +
      "            \"ldap_server\": \"\",\n" +
      "            \"mobile_phone\": \"\",\n" +
      "            \"street\": \"\",\n" +
      "            \"company\": \"\",\n" +
      "            \"department\": \"\",\n" +
      "            \"first_name\": \"survey\",\n" +
      "            \"email\": \"survey.user@email.com\",\n" +
      "            \"introduction\": \"\",\n" +
      "            \"preferred_language\": \"\",\n" +
      "            \"manager\": \"\",\n" +
      "            \"sys_mod_count\": \"1\",\n" +
      "            \"last_name\": \"user\",\n" +
      "            \"photo\": \"\",\n" +
      "            \"avatar\": \"\",\n" +
      "            \"middle_name\": \"\",\n" +
      "            \"sys_tags\": \"\",\n" +
      "            \"time_zone\": \"\",\n" +
      "            \"schedule\": \"\",\n" +
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
                                          Schema.Field.of("gender", Schema.of(Schema.Type.STRING)));
    try {
      config.validateSchema(actualSchema, collector);
      collector.getOrThrowException();
      Assert.fail("Exception is not thrown with valid credentials");
    } catch (ValidationException e) {
      Assert.assertEquals("Errors were encountered during validation.", e.getMessage());
      Assert.assertEquals(1, e.getFailures().size());
    }
  }
}
