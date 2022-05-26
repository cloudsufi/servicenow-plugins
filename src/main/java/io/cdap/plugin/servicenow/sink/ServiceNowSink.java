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

import com.google.gson.JsonObject;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.servicenow.source.util.ServiceNowConstants;
import io.cdap.plugin.servicenow.source.util.ServiceNowTableInfo;
import org.apache.hadoop.io.NullWritable;

import java.util.stream.Collectors;

/**
 * A {@link BatchSink} that writes data into the specified table in ServiceNow.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(ServiceNowConstants.PLUGIN_NAME)
@Description("Writes to the target table in ServiceNow.")
public class ServiceNowSink extends BatchSink<StructuredRecord, NullWritable, JsonObject> {

  private final ServiceNowSinkConfig conf;
  private RecordToJsonTransformer transformer;

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
  public void prepareRun(BatchSinkContext context) throws Exception {
    Schema inputSchema = context.getInputSchema();
    FailureCollector collector = context.getFailureCollector();
    conf.validate(collector);
    collector.getOrThrowException();

    context.addOutput(Output.of(conf.referenceName, new ServicenowOutputFormatProvider(conf)));

    LineageRecorder lineageRecorder = new LineageRecorder(context, conf.referenceName);
    lineageRecorder.createExternalDataset(inputSchema);
    // Record the field level WriteOperation
    if (inputSchema.getFields() != null && !inputSchema.getFields().isEmpty()) {
      String operationDescription = String.format("Wrote to Servicenow %s", conf.getTableName());
      lineageRecorder.recordWrite("Write", operationDescription,
                                  inputSchema.getFields().stream()
                                    .map(Schema.Field::getName)
                                    .collect(Collectors.toList()));
    }

  }
  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    this.transformer = new RecordToJsonTransformer();
  }


  @Override
  public void transform(StructuredRecord record, Emitter<KeyValue<NullWritable, JsonObject>> emitter) {
    JsonObject jsonObject = transformer.transform(record);
    emitter.emit(new KeyValue<>(null, jsonObject));
  }

  private void recordLineage(BatchSourceContext context, ServiceNowTableInfo tableInfo) {

  }
}
