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

package io.cdap.plugin.servicenowsink.actions;

import com.google.cloud.bigquery.TableResult;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.e2e.utils.AssertionHelper;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.servicenow.apiclient.ServiceNowAPIException;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.locators.ServiceNowPropertiesPage;
import io.cdap.plugin.servicenow.source.ServiceNowSourceConfig;
import io.cdap.plugin.servicenow.util.ServiceNowConstants;
import io.cdap.plugin.tests.hooks.TestSetupHooks;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.junit.Assert;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * ServiceNow sink - Properties page - Actions.
 */

public class ServiceNowSinkPropertiesPageActions {
  public static ServiceNowSourceConfig config;
  private static JsonObject responseFromServiceNowTable;
  private static Gson gson = new Gson();

  public static void getRecordFromServiceNowTable(String query, String tableName)
      throws ServiceNowAPIException {
    config = new ServiceNowSourceConfig(
        "", "", "", "", "",
        System.getenv("SERVICE_NOW_CLIENT_ID"),
        System.getenv("SERVICE_NOW_CLIENT_SECRET"),
        System.getenv("SERVICE_NOW_REST_API_ENDPOINT"),
        System.getenv("SERVICE_NOW_USERNAME"),
        System.getenv("SERVICE_NOW_PASSWORD"),
        "", "", "", null, false);

    ServiceNowTableAPIClientImpl tableAPIClient = new ServiceNowTableAPIClientImpl(config.getConnection(), true);
    responseFromServiceNowTable = tableAPIClient.getRecordFromServiceNowTable(tableName, query);
  }

  public static void verifyIfRecordCreatedInServiceNowIsCorrect(String query, String tableName)
      throws IOException, InterruptedException, ServiceNowAPIException {

    getRecordFromServiceNowTable(query, tableName);
    List<JsonObject> serviceNowResponse = new ArrayList<>();
    serviceNowResponse.add(responseFromServiceNowTable);

    List<JsonObject> bigQueryResponse = new ArrayList<>();
    List<Object> bigQueryRows = new ArrayList<>();
    getBigQueryTableData(bqTable, bigQueryRows);
    for (Object rows : bigQueryRows) {
      JsonObject jsonData = gson.fromJson(String.valueOf(rows), JsonObject.class);
      if (jsonData.has("sys_id") && TestSetupHooks.systemId.equals(jsonData.get("sys_id").getAsString())) {
        bigQueryResponse.add(jsonData);
        break;
      }
    }
    return compareServiceNowAndJsonData(serviceNowResponse, bigQueryResponse, bqTable);
  }

  public static void verifyIfRecordUpdatedInServiceNowIsCorrect(String query, String tableName)
      throws IOException, InterruptedException, ServiceNowAPIException {

    getRecordFromServiceNowTable(query, tableName);
    TableResult bigQueryTableData = getBigQueryTableData(TestSetupHooks.bqSourceDataset, TestSetupHooks.bqSourceTable);
    if (bigQueryTableData == null) {
      return;
    }
    String bigQueryJsonResponse = bigQueryTableData.getValues().iterator().next().get(0).getValue().toString();
    JsonObject jsonObject = gson.fromJson(bigQueryJsonResponse, JsonObject.class);
    String bigQuerySystemId = jsonObject.get(ServiceNowConstants.SYSTEM_ID).getAsString();
    JsonObject serviceNowJson = ServiceNowTableAPIClientImpl.serviceNowJsonResultArray.get(0).getAsJsonObject();
    String serviceNowSystemId = serviceNowJson.get(ServiceNowConstants.SYSTEM_ID).getAsString();

    Assert.assertTrue(bigQuerySystemId.equals(serviceNowSystemId));
  }

  public static TableResult getBigQueryTableData(String dataset, String table)
      throws IOException, InterruptedException {
    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String selectQuery = "SELECT TO_JSON(t) result FROM `" + projectId + "." + dataset + "." + table + "` AS t";
    return BigQueryClient.getQueryResult(selectQuery);
  }

  public static boolean compareServiceNowAndJsonData(List<JsonObject> serviceNowData, List<JsonObject> bigQueryData,
                                                     String tableName) throws NullPointerException {
    boolean result = false;
    if (bigQueryData == null) {
      Assert.fail("bigQueryData is null");
      return result;
    }
    if (serviceNowData.isEmpty() || bigQueryData.isEmpty()) {
      Assert.fail("One or both datasets are empty");
      return result;
    }

    BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String dataset = PluginPropertyUtils.pluginProp("dataset");
    // Build the table reference
    TableId tableRef = TableId.of(projectId, dataset, tableName);
    // Get the table schema
    Schema schema = bigQuery.getTable(tableRef).getDefinition().getSchema();

    for (int rowIndex = 0; rowIndex < serviceNowData.size(); rowIndex++) {
      JsonObject serviceNowRow = serviceNowData.get(rowIndex);
      JsonObject bigQueryRow = bigQueryData.get(rowIndex);

      for (Field field : schema.getFields()) {
        String columnName = field.getName();
        String columnType = field.getType().toString();

        switch (columnType) {
          case "BOOLEAN":
            boolean sourceAsBoolean = serviceNowRow.get(columnName).getAsBoolean();
            boolean targetAsBoolean = bigQueryRow.get(columnName).getAsBoolean();
            Assert.assertEquals("Different values found for column : %s", sourceAsBoolean, targetAsBoolean);
            break;
          case "INTEGER":
            int sourceAsInteger = serviceNowRow.get(columnName).getAsInt();
            int targetAsInteger = bigQueryRow.get(columnName).getAsInt();
            Assert.assertEquals("Different values found for column : %s", sourceAsInteger, targetAsInteger);
            break;
          case "DATETIME":
            LocalDateTime sourceDateTime = LocalDateTime.parse(serviceNowRow.get(columnName).getAsString(),
                                                               DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            LocalDateTime targetDateTime = LocalDateTime.parse(serviceNowRow.get(columnName).getAsString(),
                                                               DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            Assert.assertEquals("Different values found for column : %s", sourceDateTime, targetDateTime);
            break;
          case "FLOAT":
            double sourceVal = serviceNowRow.get(columnName).getAsDouble();
            double targetVal = bigQueryRow.get(columnName).getAsDouble();
            Assert.assertEquals(String.format("Different values found for column: %s", columnName), 0,
                                Double.compare(sourceVal, targetVal));
            break;
          default:
            JsonElement sourceElement = serviceNowRow.get(columnName);
            String sourceString = (sourceElement != null && !sourceElement.isJsonNull())
              ? sourceElement.getAsString() : null;
            JsonElement targetElement = bigQueryRow.get(columnName);
            String targetString = (targetElement != null && !targetElement.isJsonNull())
              ? targetElement.getAsString() : null;
            // Normalize values: treat empty string ("") as equivalent to null
            if ("".equals(sourceString)) {
              sourceString = null;
            }
            if ("".equals(targetString)) {
              targetString = null;
            }
            Assert.assertEquals(String.format("Different  values found for column : %s", columnName),
                                String.valueOf(sourceString), String.valueOf(targetString));
        }
      }
    }

    Assert.assertFalse("Number of rows in Source table is greater than the number of rows in Target table",
                       serviceNowData.size() > bigQueryData.size());
    return true;
  }

  /**
   * Return BigQuery supported date formats.
   */
  @Nullable
  private static Date checkBigQueryDateFormat(String value) {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");
    SimpleDateFormat timeStampFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
    Date date;
    try {
      date = dateFormat.parse(value);
      return date;
    } catch (ParseException ignored) {
    }
    try {
      date = dateTimeFormat.parse(value);
      return date;
    } catch (ParseException ignored) {
    }
    try {
      date = timeStampFormat.parse(value);
      return date;
    } catch (ParseException ignored) {
    }
    return null;
  }

  public static void verifyErrorForNonCreatableFields() {
    AssertionHelper.verifyElementDisplayed(ServiceNowPropertiesPage.fieldNotCreatableError);
  }
}
