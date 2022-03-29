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

package io.cdap.plugin.servicenow.source;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.servicenow.source.apiclient.ServiceNowTableDataResponse;
import io.cdap.plugin.servicenow.source.util.ServiceNowColumn;
import io.cdap.plugin.servicenow.source.util.ServiceNowTableInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServiceNowMultiInputFormat.class})
public class ServiceNowMultiInputFormatTest {

  private static final String CLIENT_ID = "clientId";
  private static final String CLIENT_SECRET = "clientSecret";
  private static final String REST_API_ENDPOINT = "https://ven05127.service-now.com";
  private static final String USER = "user";
  private static final String PASSWORD = "password";
  private ServiceNowMultiSourceConfig serviceNowMultiSourceConfig;

  @Before
  public void initialize() {
      serviceNowMultiSourceConfig = Mockito.spy(ServiceNowSourceConfigHelper.newConfigBuilder()
        .setReferenceName("referenceName")
        .setRestApiEndpoint(REST_API_ENDPOINT)
        .setUser(USER)
        .setPassword(PASSWORD)
        .setClientId(CLIENT_ID)
        .setClientSecret(CLIENT_SECRET)
        .setTableNames("sys_user")
        .setValueType("Actual")
        .setStartDate("2021-01-01")
        .setEndDate("2022-02-18")
        .setTableNameField("tablename")
        .buildMultiSource());
  }

  @Test
  public void testFetchTablesInfo() {
    ServiceNowMultiSourceConfig config =
      new ServiceNowMultiSourceConfig("referenceName", "tableNameTable", "client_id",
                                      "client_secret", "http://example.com", "user",
                                      "password", "Actual", "2021-12-30",
                                      "2021-12-31", "sys_user");
    ServiceNowColumn column1 = new ServiceNowColumn("sys_created_by", "string");
    ServiceNowColumn column2 = new ServiceNowColumn("sys_updated_by", "string");
    List<ServiceNowColumn> columns = new ArrayList<>();
    columns.add(column1);
    columns.add(column2);
    ServiceNowTableDataResponse response = new ServiceNowTableDataResponse();
    response.setColumns(columns);
    int recordCount = response.getTotalRecordCount();
    String tableName = "sys_user";
    Schema schema = Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS);
    Set<ServiceNowTableInfo> serviceNowTableInfos = new HashSet<>();
    ServiceNowTableInfo serviceNowTableInfo = new ServiceNowTableInfo(tableName, schema, recordCount);
    serviceNowTableInfos.add(serviceNowTableInfo);
    PowerMockito.mockStatic(ServiceNowMultiInputFormat.class);
    PowerMockito.when(ServiceNowMultiInputFormat.fetchTablesInfo(config)).thenReturn(serviceNowTableInfos);
    Assert.assertEquals(1, ServiceNowMultiInputFormat
      .fetchTablesInfo(config)
      .size());
  }

  @Test
  public void testFetchTablesInfoWithEmptyTableNames() {
    ServiceNowMultiSourceConfig config = new ServiceNowMultiSourceConfig("Reference Name",
    "tableName", "client_id", "Client Secret", "http://example.com",
     "user", "password", "Actual", "2021-12-30", "2021-12-31", "");
    Assert.assertTrue(ServiceNowMultiInputFormat
                        .fetchTablesInfo(config)
                        .isEmpty());
  }
}
