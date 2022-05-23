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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.servicenow.source.util.ServiceNowConstants;
import io.cdap.plugin.servicenow.source.util.ServiceNowTableInfo;
import org.apache.hadoop.io.NullWritable;

/**
 * A {@link BatchSink} that writes data into the specified table in ServiceNow.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(ServiceNowConstants.PLUGIN_NAME)
@Description("Writes to the target table in ServiceNow.")
public class ServiceNowSink extends BatchSink<StructuredRecord, NullWritable, CSVRecord> {

  private final ServiceNowSinkConfig conf;

  public ServiceNowSink(ServiceNowSinkConfig conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    conf.validate(collector);
    if (collector.getValidationFailures().isEmpty()) {
      conf.validateSchema(stageConfigurer.getInputSchema(), collector);
    }

  }

  @Override
  public void prepareRun(BatchSinkContext batchSinkContext) throws Exception {

  }


  public void transform(KeyValue<NullWritable, StructuredRecord> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(input.getValue());
  }

  private void recordLineage(BatchSourceContext context, ServiceNowTableInfo tableInfo) {

  }
}
