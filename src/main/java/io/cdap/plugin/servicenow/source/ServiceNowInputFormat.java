/*
 * Copyright © 2020 Cask Data, Inc.
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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableDataResponse;
import io.cdap.plugin.servicenow.connector.ServiceNowConnectorConfig;
import io.cdap.plugin.servicenow.util.SchemaBuilder;
import io.cdap.plugin.servicenow.util.ServiceNowColumn;
import io.cdap.plugin.servicenow.util.ServiceNowConstants;
import io.cdap.plugin.servicenow.util.ServiceNowTableInfo;
import io.cdap.plugin.servicenow.util.SourceApplication;
import io.cdap.plugin.servicenow.util.SourceQueryMode;
import io.cdap.plugin.servicenow.util.SourceValueType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * ServiceNow input format.
 */
public class ServiceNowInputFormat extends InputFormat<NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceNowInputFormat.class);

  /**
   * Updates the jobConfig with the ServiceNow table information, which will then be read in getSplit() function.
   *
   * @param jobConfig the job configuration
   * @param mode      the query mode
   * @param conf      the database conf
   * @return Collection of ServiceNowTableInfo containing table and schema.
   */
  public static List<ServiceNowTableInfo> setInput(Configuration jobConfig, SourceQueryMode mode,
                                                   ServiceNowSourceConfig conf) {
    ServiceNowJobConfiguration jobConf = new ServiceNowJobConfiguration(jobConfig);
    jobConf.setPluginConfiguration(conf);

    // Depending on conf value fetch the list of fields for each table and create schema object
    // return the schema object for each table as ServiceNowTableInfo
    List<ServiceNowTableInfo> tableInfos = fetchTableInfo(mode, conf.getConnection(), conf.getTableName(),
                                                          conf.getApplicationName(), conf.getValueType(),
                                                          conf.getStartDate(), conf.getEndDate());

    jobConf.setTableInfos(tableInfos);

    return tableInfos;
  }

  public static List<ServiceNowTableInfo> fetchTableInfo(SourceQueryMode mode, ServiceNowConnectorConfig conf,
                                                         @Nullable String tableName,
                                                         @Nullable SourceApplication application,
                                                         SourceValueType valueType, @Nullable String startDate,
                                                         @Nullable String endDate) {
    // When mode = Table, fetch details from the table name provided in plugin config
    if (mode == SourceQueryMode.TABLE) {
      ServiceNowTableInfo tableInfo = getTableMetaData(tableName, conf, valueType, startDate, endDate);
      return (tableInfo == null) ? Collections.emptyList() : Collections.singletonList(tableInfo);
    }

    // When mode = Reporting, get the list of tables for application name provided in plugin config
    // and then fetch details from each of the tables.
    List<ServiceNowTableInfo> tableInfos = new ArrayList<>();

    List<String> tableNames = application.getTableNames();
    for (String table : tableNames) {
      ServiceNowTableInfo tableInfo = getTableMetaData(table, conf, valueType, startDate, endDate);
      if (tableInfo == null) {
        continue;
      }
      tableInfos.add(tableInfo);
    }

    return tableInfos;
  }

  private static ServiceNowTableInfo getTableMetaData(String tableName, ServiceNowConnectorConfig conf,
                                                      SourceValueType valueType, String startDate, String endDate) {
    // Call API to fetch first record from the table
    ServiceNowTableAPIClientImpl restApi = new ServiceNowTableAPIClientImpl(conf);

    ServiceNowTableDataResponse response = restApi.fetchTableSchema(tableName, valueType, startDate, endDate,
                                                                    true);
    if (response == null) {
      return null;
    }

    List<ServiceNowColumn> columns = response.getColumns();
    if (columns == null || columns.isEmpty()) {
      return null;
    }

    Schema schema = SchemaBuilder.constructSchema(tableName, columns);
    LOG.debug("table {}, rows = {}", tableName, response.getTotalRecordCount());
    return new ServiceNowTableInfo(tableName, schema, response.getTotalRecordCount());
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) {
    ServiceNowJobConfiguration jobConfig = new ServiceNowJobConfiguration(jobContext.getConfiguration());

    List<ServiceNowTableInfo> tableInfos = jobConfig.getTableInfos();
    List<InputSplit> resultSplits = new ArrayList<>();

    for (ServiceNowTableInfo tableInfo : tableInfos) {
      String tableName = tableInfo.getTableName();
      int totalRecords = tableInfo.getRecordCount();
      if (totalRecords <= ServiceNowConstants.PAGE_SIZE) {
        // add single split for table and continue
        resultSplits.add(new ServiceNowInputSplit(tableName, 0));
        continue;
      }

      int pages = (tableInfo.getRecordCount() / ServiceNowConstants.PAGE_SIZE);
      if (tableInfo.getRecordCount() % ServiceNowConstants.PAGE_SIZE > 0) {
        pages++;
      }
      int offset = 0;

      for (int page = 1; page <= pages; page++) {
        resultSplits.add(new ServiceNowInputSplit(tableName, offset));
        offset += ServiceNowConstants.PAGE_SIZE;
      }
    }

    return resultSplits;
  }

  @Override
  public RecordReader<NullWritable, StructuredRecord> createRecordReader(InputSplit inputSplit,
                                                                         TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {
    ServiceNowJobConfiguration jobConfig = new ServiceNowJobConfiguration(taskAttemptContext.getConfiguration());
    ServiceNowSourceConfig pluginConf = jobConfig.getPluginConf();
    return new ServiceNowRecordReader(pluginConf);
  }
}
