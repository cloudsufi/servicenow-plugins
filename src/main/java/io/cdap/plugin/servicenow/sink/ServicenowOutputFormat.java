package io.cdap.plugin.servicenow.sink;

import com.google.gson.JsonObject;
import io.cdap.plugin.servicenow.source.ServiceNowJobConfiguration;
import io.cdap.plugin.servicenow.source.ServiceNowSourceConfig;
import io.cdap.plugin.servicenow.source.util.ServiceNowTableInfo;
import io.cdap.plugin.servicenow.source.util.SourceQueryMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class ServicenowOutputFormat extends OutputFormat<NullWritable, JsonObject> {


  public static void setOutput(Configuration conf, ServiceNowSinkConfig pluginConf) {
    ServiceNowJobConfiguration jobConf = new ServiceNowJobConfiguration(conf);
    jobConf.setSinkPluginConfiguration(pluginConf);
    //ServiceNowJobConfiguration jobConfig = new ServiceNowJobConfiguration(conf);
    //ServiceNowSinkConfig pluginConf1 = jobConfig.getSinkPluginConf();
  }

  @Override
  public RecordWriter<NullWritable, JsonObject> getRecordWriter(TaskAttemptContext taskAttemptContext) throws
    IOException, InterruptedException {
    ServiceNowJobConfiguration jobConfig = new ServiceNowJobConfiguration(taskAttemptContext.getConfiguration());
    ServiceNowSinkConfig pluginConf = jobConfig.getSinkPluginConf();
    try {
      return new ServicenowRecordWriter(pluginConf);
    } catch (Exception e) {
      throw new RuntimeException("There was issue communicating with Servicenow", e);
    }
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException,
    InterruptedException {
    return new OutputCommitter() {
      @Override
      public void setupJob(JobContext jobContext) throws IOException {

      }

      @Override
      public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {

      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
        return false;
      }

      @Override
      public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {

      }

      @Override
      public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {

      }
    };
  }
}
