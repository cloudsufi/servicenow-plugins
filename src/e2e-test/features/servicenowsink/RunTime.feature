# Copyright © 2022 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

@ServiceNow
@SNSink
@Smoke
@Regression
Feature: ServiceNow Sink - Run time scenarios

  @TS-SN-RNTM-SINK-01 @BQ_SOURCE_TEST_RECEIVING_SLIP_LINE
  Scenario: Verify user should be able to preview the pipeline ServiceNow sink is configured for Insert operation
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Fill Reference Name
    And Configure BigQuery source plugin for Dataset and Table
    And Validate "BigQuery" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "ServiceNow" from the plugins list
    And Connect plugins: "BigQuery" and "ServiceNow" to establish connection
    And Navigate to the properties page of plugin: "ServiceNow"
    And Fill Reference Name
    And fill Credentials section for pipeline user
    And Enter input plugin property: "tableName" with value: "receiving_slip_line"
    And Select radio button plugin property: "operation" with value: "INSERT"
    And Validate "ServiceNow" plugin properties
    And Close the Plugin Properties page
    And Preview and run the pipeline
    And Wait till pipeline is in running state
    Then Verify the preview of pipeline is "success"
    And Click on the Preview Data link on the Sink plugin node: "ServiceNow"
    And Verify sink plugin's Preview Data for Input Records table and the Input Schema matches the Output Schema of Source plugin

  @TS-SN-RNTM-SINK-02 @BQ_SOURCE_TEST_RECEIVING_SLIP_LINE
  Scenario: Verify user should be able to deploy and run the pipeline ServiceNow sink is configured for Insert operation
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Fill Reference Name
    And Configure BigQuery source plugin for Dataset and Table
    And Validate "BigQuery" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "ServiceNow" from the plugins list
    And Connect plugins: "BigQuery" and "ServiceNow" to establish connection
    And Navigate to the properties page of plugin: "ServiceNow"
    And Fill Reference Name
    And fill Credentials section for pipeline user
    And Enter input plugin property: "tableName" with value: "receiving_slip_line"
    And Select radio button plugin property: "operation" with value: "INSERT"
    And Validate "ServiceNow" plugin properties
    And Close the Plugin Properties page
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count

  @TS-SN-RNTM-SINK-03 @BQ_SOURCE_TEST_RECEIVING_SLIP_LINE
  Scenario: Verify user should be able to preview the pipeline when ServiceNow sink is configured for Update operation
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Fill Reference Name
    And Configure BigQuery source plugin for Dataset and Table
    And Validate "BigQuery" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "ServiceNow" from the plugins list
    And Connect plugins: "BigQuery" and "ServiceNow" to establish connection
    And Navigate to the properties page of plugin: "ServiceNow"
    And Fill Reference Name
    And fill Credentials section for pipeline user
    And Enter input plugin property: "tableName" with value: "receiving_slip_line"
    And Select radio button plugin property: "operation" with value: "UPDATE"
    And Validate "ServiceNow" plugin properties
    And Close the Plugin Properties page
    And Preview and run the pipeline
    And Wait till pipeline is in running state
    And Verify the preview of pipeline is "success"
    And Click on the Preview Data link on the Sink plugin node: "ServiceNow"
    Then Verify sink plugin's Preview Data for Input Records table and the Input Schema matches the Output Schema of Source plugin

  @TS-SN-RNTM-SINK-04 @BQ_SOURCE_TEST_RECEIVING_SLIP_LINE
  Scenario: Verify user should be able to deploy and run the pipeline when ServiceNow sink is configured for Update operation
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery source plugin for Dataset and Table
    And Fill Reference Name
    And Validate "BigQuery" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "ServiceNow" from the plugins list
    And Connect plugins: "BigQuery" and "ServiceNow" to establish connection
    And Navigate to the properties page of plugin: "ServiceNow"
    And Fill Reference Name
    And fill Credentials section for pipeline user
    And Enter input plugin property: "tableName" with value: "receiving_slip_line"
    And Select radio button plugin property: "operation" with value: "UPDATE"
    And Validate "ServiceNow" plugin properties
    And Close the Plugin Properties page
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count

  @TS-SN-RNTM-SINK-05 @BQ_SOURCE_TEST_RECEIVING_SLIP_LINE
  Scenario: Verify user should be able to deploy and run the pipeline when plugin is configured for table Receiving Slip with Input operation
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Fill Reference Name
    And Replace input plugin property: "project" with value: "cdf-entcon"
    And Select radio button plugin property: "serviceAccountType" with value: "JSON"
    And fill json
    And Configure BigQuery source plugin for Dataset and Table
    And Validate "BigQuery" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "ServiceNow" from the plugins list
    And Connect plugins: "BigQuery" and "ServiceNow" to establish connection
    And Navigate to the properties page of plugin: "ServiceNow"
    And Fill Reference Name
    And fill Credentials section for pipeline user
    And Enter input plugin property: "tableName" with value: "receiving_slip_line"
    And Select radio button plugin property: "operation" with value: "INSERT"
    And Validate "ServiceNow" plugin properties
    And Close the Plugin Properties page
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Succeeded"
    Then Open and capture logs

  @TS-SN-RNTM-SINK-06 @BQ_SOURCE_AGENT_ASSIST_RECOMMENDATION
  Scenario: Verify user should be able to deploy and run the pipeline when plugin is configured for table Agent Assist recommendation with Input operation
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Fill Reference Name
    And Replace input plugin property: "project" with value: "cdf-entcon"
    And Select radio button plugin property: "serviceAccountType" with value: "JSON"
    And fill json
    And Configure BigQuery source plugin for Dataset and Table
    And Validate "BigQuery" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "ServiceNow" from the plugins list
    And Connect plugins: "BigQuery" and "ServiceNow" to establish connection
    And Navigate to the properties page of plugin: "ServiceNow"
    And Fill Reference Name
    And fill Credentials section for pipeline user
    And Enter input plugin property: "tableName" with value: "agent_assist_recommendation"
    And Select radio button plugin property: "operation" with value: "INSERT"
    And Validate "ServiceNow" plugin properties
    And Close the Plugin Properties page
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Succeeded"
    Then Open and capture logs

  @TS-SN-RNTM-SINK-07 @BQ_SOURCE_VENDOR_CATALOG_ITEM
  Scenario: Verify user should be able to deploy and run the pipeline when plugin is configured for table Vendor Catalog Item with Input operation
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Fill Reference Name
    And Replace input plugin property: "project" with value: "cdf-entcon"
    And Select radio button plugin property: "serviceAccountType" with value: "JSON"
    And fill json
    And Configure BigQuery source plugin for Dataset and Table
    And Validate "BigQuery" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "ServiceNow" from the plugins list
    And Connect plugins: "BigQuery" and "ServiceNow" to establish connection
    And Navigate to the properties page of plugin: "ServiceNow"
    And Fill Reference Name
    And fill Credentials section for pipeline user
    And Enter input plugin property: "tableName" with value: "vendor_catalog_item"
    And Select radio button plugin property: "operation" with value: "INSERT"
    And Validate "ServiceNow" plugin properties
    And Close the Plugin Properties page
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Succeeded"
    Then Open and capture logs


