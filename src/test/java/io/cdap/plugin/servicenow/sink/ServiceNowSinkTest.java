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

import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.servicenow.ServiceNowBaseConfig;
import io.cdap.plugin.servicenow.restapi.RestAPIResponse;
import io.cdap.plugin.servicenow.source.ServiceNowBaseSourceConfig;
import io.cdap.plugin.servicenow.source.apiclient.ServiceNowTableAPIClientImpl;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceNowTableAPIClientImpl.class, ServiceNowBaseSourceConfig.class, ServiceNowSink.class,
  HttpClientBuilder.class, RestAPIResponse.class})
public class ServiceNowSinkTest {
  private static final String CLIENT_ID = "clientId";
  private static final String CLIENT_SECRET = "clientSecret";
  private static final String REST_API_ENDPOINT = "https://ven05127.service-now.com";
  private static final String USER = "user";
  private static final String PASSWORD = "password";
  private ServiceNowSink serviceNowSink;
  private ServiceNowSinkConfig serviceNowSinkConfig;

  @Before
  public void initialize() {
    serviceNowSinkConfig = ServiceNowSinkConfigHelper.newConfigBuilder()
      .setReferenceName("referenceName")
      .setRestApiEndpoint(REST_API_ENDPOINT)
      .setUser(USER)
      .setPassword(PASSWORD)
      .setClientId(CLIENT_ID)
      .setClientSecret(CLIENT_SECRET)
      .setTableName("sys_user")
      .setPageSize(200L)
      .setOperation("Insert")
      .build();
    serviceNowSink = new ServiceNowSink(serviceNowSinkConfig);
  }

  @Test
  public void testConfigurePipeline() throws Exception {
    Map<String, Object> plugins = new HashMap<>();
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(null, plugins);
    ServiceNowTableAPIClientImpl restApi = Mockito.mock(ServiceNowTableAPIClientImpl.class);
    Mockito.when(restApi.getAccessToken()).thenReturn("token");
    PowerMockito.whenNew(ServiceNowTableAPIClientImpl.class).withParameterTypes(ServiceNowBaseConfig.class)
      .withArguments(Mockito.any(ServiceNowBaseConfig.class)).thenReturn(restApi);
    List<Map<String, Object>> result = new ArrayList<>();
    int httpStatus = HttpStatus.SC_OK;
    Map<String, String> headers = new HashMap<>();
    String responseBody = "{\n" +
      "    \"result\": []\n" +
      "}";
    RestAPIResponse restAPIResponse = new RestAPIResponse(httpStatus, headers, responseBody);
    Mockito.when(restApi.executeGet(Mockito.any())).thenReturn(restAPIResponse);
    Mockito.when(restApi.parseResponseToResultListOfMap(restAPIResponse.getResponseBody())).thenReturn(result);
    serviceNowSink.configurePipeline(mockPipelineConfigurer);
    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }
}
