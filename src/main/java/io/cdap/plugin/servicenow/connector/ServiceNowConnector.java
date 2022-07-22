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
package io.cdap.plugin.servicenow.connector;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseEntityPropertyValue;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIRequestBuilder;
import io.cdap.plugin.servicenow.restapi.RestAPIResponse;
import io.cdap.plugin.servicenow.source.ServiceNowInputFormat;
import io.cdap.plugin.servicenow.util.ServiceNowConstants;
import io.cdap.plugin.servicenow.util.ServiceNowTableInfo;
import io.cdap.plugin.servicenow.util.SourceQueryMode;
import io.cdap.plugin.servicenow.util.SourceValueType;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MediaType;


/**
 * ServiceNow Connector Plugin
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(ServiceNowConstants.PLUGIN_NAME)
@Description("Connection to access data in Servicenow tables.")
public class ServiceNowConnector implements Connector {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceNowConnector.class);
  private static final String OBJECT_TABLE_LIST = "sys_db_object";
  private static final String ENTITY_TYPE_TABLE = "table";
  private static final String LABEL_NAME = "label";
  private final ServiceNowConnectorConfig config;
  private static Gson gson = new Gson();

  ServiceNowConnector(ServiceNowConnectorConfig config) {
    this.config = config;
  }

  @Override
  public void test(ConnectorContext connectorContext) throws ValidationException {
    FailureCollector collector = connectorContext.getFailureCollector();
    config.validateCredentialsFields(collector);
  }

  @Override
  public BrowseDetail browse(ConnectorContext connectorContext, BrowseRequest browseRequest) throws IOException {
    int count = 0;
    FailureCollector collector = connectorContext.getFailureCollector();
    config.validateCredentialsFields(collector);
    collector.getOrThrowException();
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    Table[] table = listTables().getResult();
    for (int i = 0; i < table.length; i++) {
      String name = table[i].getName();
      String label = table[i].getLabel();
      BrowseEntity.Builder entity = (BrowseEntity.builder(name, name, ENTITY_TYPE_TABLE).
        canBrowse(false).canSample(true));
      entity.addProperty(LABEL_NAME, BrowseEntityPropertyValue.builder(label, BrowseEntityPropertyValue.
        PropertyType.STRING).build());
      browseDetailBuilder.addEntity(entity.build());
      count++;
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }


  /**
   * @return the list of tables.
   */
  private TableList listTables() throws IOException {
    ServiceNowTableAPIRequestBuilder requestBuilder = new ServiceNowTableAPIRequestBuilder(
      config.getRestApiEndpoint(), OBJECT_TABLE_LIST);
    String accessToken = null;
    ServiceNowTableAPIClientImpl serviceNowTableAPIClient = new ServiceNowTableAPIClientImpl(config);
    try {
      accessToken = serviceNowTableAPIClient.getAccessToken();
    } catch (OAuthSystemException | OAuthProblemException e) {
      throw new IOException(e);
    }
    requestBuilder.setAuthHeader(accessToken);
    requestBuilder.setAcceptHeader(MediaType.APPLICATION_JSON);
    requestBuilder.setContentTypeHeader(MediaType.APPLICATION_JSON);
    RestAPIResponse apiResponse = null;
    apiResponse = serviceNowTableAPIClient.executeGet(requestBuilder.build());
    if (!apiResponse.isSuccess()) {
      LOG.error("Error - {}", getErrorMessage(apiResponse.getResponseBody()));
      throw new IOException();
    } else {
      String response = null;
      response = apiResponse.getResponseBody();
      return gson.fromJson(response, TableList.class);
    }
  }

  @Override
  public ConnectorSpec generateSpec(ConnectorContext connectorContext, ConnectorSpecRequest connectorSpecRequest) {
    ConnectorSpec.Builder specBuilder = ConnectorSpec.builder();
    Map<String, String> properties = new HashMap<>();
    properties.put(io.cdap.plugin.common.ConfigUtil.NAME_USE_CONNECTION, "true");
    properties.put(ConfigUtil.NAME_CONNECTION, connectorSpecRequest.getConnectionWithMacro());
    String tableName = connectorSpecRequest.getPath();
    if (tableName != null) {
      properties.put(ServiceNowConstants.PROPERTY_TABLE_NAME, tableName);
    }
    SourceQueryMode mode = SourceQueryMode.TABLE;
    SourceValueType valueType = SourceValueType.SHOW_DISPLAY_VALUE;
    List<ServiceNowTableInfo> tableInfo = ServiceNowInputFormat.fetchTableInfo(mode, config, tableName,
                                                                               null, valueType,
                                                                               null, null);
    Schema schema = tableInfo.stream().findFirst().isPresent() ? tableInfo.stream().findFirst().get().getSchema() :
                                                                                                      null;
    specBuilder.setSchema(schema);
    return specBuilder.addRelatedPlugin(new PluginSpec(ServiceNowConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE,
                                                       properties)).
      addRelatedPlugin(new PluginSpec(ServiceNowConstants.PLUGIN_NAME, BatchSink.PLUGIN_TYPE, properties)).build();
  }


  private String getErrorMessage(String responseBody) {
    try {
      JsonObject jo = gson.fromJson(responseBody, JsonObject.class);
      return jo.getAsJsonObject(ServiceNowConstants.ERROR).get(ServiceNowConstants.MESSAGE).getAsString();
    } catch (Exception e) {
      return e.getMessage();
    }
  }
}
