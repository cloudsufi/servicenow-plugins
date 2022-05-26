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

package io.cdap.plugin.tests.hooks;

import com.google.api.client.util.DateTime;
import com.google.cloud.bigquery.BigQueryException;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.servicenow.restapi.RestAPIClient;
import io.cdap.plugin.servicenow.source.ServiceNowSourceConfig;
import io.cdap.plugin.servicenow.source.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.utils.enums.TablesInTableMode;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.entity.StringEntity;
import org.dmg.pmml.True;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.time.LocalDate;
import java.util.Random;

/**
 * Represents Test Setup and Clean up hooks.
 */
public class TestSetupHooks {
  public static ServiceNowSourceConfig config;
  public static String bqTargetDataset = StringUtils.EMPTY;
  public static String bqTargetTable = StringUtils.EMPTY;
  public static String bqSourceDataset = "ServiceNowTestDS";
  public static String bqSourceTable;


  @Before(order = 1, value = "@SN_SOURCE_CONFIG")
  public static void initializeServiceNowSourceConfig() {
    BeforeActions.scenario.write("Initialize ServiceNowSourceConfig");
    config = new ServiceNowSourceConfig(
      "", "", "", "", "",
      PluginPropertyUtils.pluginProp("client.id"),
      PluginPropertyUtils.pluginProp("client.secret"),
      PluginPropertyUtils.pluginProp("rest.api.endpoint"),
      PluginPropertyUtils.pluginProp("pipeline.user.username"),
      PluginPropertyUtils.pluginProp("pipeline.user.password"),
      "", "", "");
  }

  @Before(order = 2, value = "@SN_PRODUCT_CATALOG_ITEM")
  public static void createRecordInProductCatalogItemTable() throws UnsupportedEncodingException {
    BeforeActions.scenario.write("Create new record in Product Catalog Item table");
    ServiceNowTableAPIClientImpl tableAPIClient = new ServiceNowTableAPIClientImpl(config);
    String uniqueId = "TestProductCatalogItem" + RandomStringUtils.randomAlphanumeric(10);
    String recordDetails = "{'name':'" + uniqueId + "','price':'2500'}";
    StringEntity entity = new StringEntity(recordDetails);
    tableAPIClient.createRecord(TablesInTableMode.PRODUCT_CATALOG_ITEM.value, entity);
  }

  @Before(order = 3, value = "@SN_RECEIVING_SLIP_LINE")
  public static void createRecordInReceivingSlipLineTable() throws UnsupportedEncodingException {
    BeforeActions.scenario.write("Create new record in Receiving Slip Line table");
    ServiceNowTableAPIClientImpl tableAPIClient = new ServiceNowTableAPIClientImpl(config);
    String uniqueId = "TestReceivingSlipLine" + RandomStringUtils.randomAlphanumeric(10);
    String recordDetails = "{'cost':'100','quantity':'5','number':'" + uniqueId + "'}";
    StringEntity entity = new StringEntity(recordDetails);
    tableAPIClient.createRecord(TablesInTableMode.RECEIVING_SLIP_LINE.value, entity);
  }

  @Before(order = 3, value = "@SN_UPDATE_RECEIVING_SLIP_LINE")
  public static void updateRecordInReceivingSlipLineTable() throws UnsupportedEncodingException {
    BeforeActions.scenario.write("Create new record in Receiving Slip Line table");
    ServiceNowTableAPIClientImpl tableAPIClient = new ServiceNowTableAPIClientImpl(config);
    String uniqueId = "TestReceivingSlipLine" + RandomStringUtils.randomAlphanumeric(10);
    String recordDetails = "{'cost':'100','quantity':'5','number':'" + uniqueId + "'}";
    StringEntity entity = new StringEntity(recordDetails);
    tableAPIClient.createAndUpdateRecord(TablesInTableMode.RECEIVING_SLIP_LINE.value, entity);
  }

  @Before(order = 4, value = "@BQ_SINK")
  public static void setTempTargetBQDataset() {
    bqTargetDataset = "TestSN_dataset" + RandomStringUtils.randomAlphanumeric(10);
    BeforeActions.scenario.write("BigQuery Target dataset name: " + bqTargetDataset);
  }

  @Before(order = 5, value = "@BQ_SINK")
  public static void setTempTargetBQTable() {
    bqTargetTable = "TestSN_table" + RandomStringUtils.randomAlphanumeric(10);
    BeforeActions.scenario.write("BigQuery Target table name: " + bqTargetTable);
  }
  @Before(order = 1, value = "@BQ_SOURCE_TEST_RECEIVING_SLIP_LINE")
  public static void createTempSourceBQTableForReceivingSlipLineTable() throws IOException, InterruptedException {
    Random uniqueId = new Random();
    String stringUniqueId = "ServiceNow" + RandomStringUtils.randomAlphanumeric(5);
    bqSourceTable = "testTable" + stringUniqueId;

    float cost = uniqueId.nextFloat();
    int quantity = uniqueId.nextInt(1000);
    String number = "Receiving";
    LocalDate date = LocalDate.now(); //Need to pass current_date()

    BigQueryClient.getSoleQueryResult("create table `" + bqSourceDataset + "." + bqSourceTable + "` as " +
                                        "SELECT * FROM UNNEST([ STRUCT(" + cost + " AS cost, "
                                        + quantity + " AS quantity,'"  + number + "'  AS number, "
                                        + date + " AS received)])");
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " created successfully");
  }

  @Before(order = 1, value = "@BQ_SOURCE_AGENT_ASSIST_RECOMMENDATION")
  public static void createTempSourceBQTableForAgentFile() throws IOException, InterruptedException {
    String stringUniqueId = "ServiceNow" + RandomStringUtils.randomAlphanumeric(5);
    bqSourceTable = "testTable" + stringUniqueId;

    Boolean active = true;
    String name = "Agent";
    int order =12;
    LocalDate date = LocalDate.now(); //need to pass current_date()

    BigQueryClient.getSoleQueryResult("create table `" + bqSourceDataset + "." + bqSourceTable + "` as " +
                                        "SELECT * FROM UNNEST([ STRUCT(" + active + " AS active, "
                                        + order + " AS sys_mod_count,'"  + name + "'  AS name, "
                                        + date + "  AS sys_created_on)])");
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " created successfully");
  }

  @Before(order = 1, value = "@BQ_SOURCE_VENDOR_CATALOG_ITEM")
  public static void createTempSourceBQTableForAgentAssistRecomendation() throws IOException, InterruptedException {
    Random uniqueId = new Random();
    String stringUniqueId = "ServiceNow" + RandomStringUtils.randomAlphanumeric(5);
    bqSourceTable = "testTable" + stringUniqueId;

    float listPrice = uniqueId.nextFloat();
    double price = uniqueId.nextDouble();
    boolean outOfStock= true;
    String name = "check";
    int updates = uniqueId.nextInt(100);
    LocalDate date = LocalDate.now(); //need to pass CURRENT_DATETIME()

    BigQueryClient.getSoleQueryResult("create table `" + bqSourceDataset + "." + bqSourceTable + "` as " +
                                        "SELECT * FROM UNNEST([ STRUCT(" + listPrice + " AS list_price, "
                                        + outOfStock + " AS out_of_stock,"  + price + "  AS price,' "
                                        + name + " ' AS sys_update_name," + updates + " AS sys_mod_count, "
                                        + date + "  AS sys_created_on)])");

    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " created successfully");
  }

  @After(order = 1, value = "@BQ_SINK_CLEANUP")
  public static void deleteTempTargetBQTable() throws IOException, InterruptedException {
    try {
      BigQueryClient.dropBqQuery(bqTargetDataset, bqTargetTable);
      BeforeActions.scenario.write("BigQuery Target table: " + bqTargetTable + " is deleted successfully");
      bqTargetTable = StringUtils.EMPTY;
    } catch (BigQueryException e) {
      if (e.getMessage().contains("Not found: Table")) {
        BeforeActions.scenario.write("BigQuery Target Table: " + bqTargetTable + " does not exist");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }
}
