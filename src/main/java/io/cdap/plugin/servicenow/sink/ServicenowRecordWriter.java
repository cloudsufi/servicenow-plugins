package io.cdap.plugin.servicenow.sink;

import com.google.gson.JsonObject;
import io.cdap.plugin.servicenow.restapi.RestAPIResponse;
import io.cdap.plugin.servicenow.source.apiclient.ServiceNowTableAPIClientImpl;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ServicenowRecordWriter extends RecordWriter<NullWritable, JsonObject> {

  private ServiceNowSinkConfig config;
  private List<RestRequest> restRequests = new ArrayList<>();
  private static final Logger LOG = LoggerFactory.getLogger(ServicenowRecordWriter.class);
  private String accessToken;
  private Long maxRecordsPerBatch;
  private ServiceNowTableAPIClientImpl restApi;


  ServicenowRecordWriter(ServiceNowSinkConfig config) {
    super();
    this.config = config;
  }
  /*public ServicenowRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, OAuthProblemException,
    OAuthSystemException {
    Configuration conf = taskAttemptContext.getConfiguration();
    maxRecordsPerBatch = Long.parseLong(conf.get(ServiceNowConstants.PROPERTY_MAX_RECORDS_PER_BATCH));
    restApi = new ServiceNowTableAPIClientImpl(conf);
    accessToken = restApi.getAccessToken();
  }*/

  @Override
  public void write(NullWritable key, JsonObject jsonObject) throws IOException {
    RestAPIResponse apiResponse;
    ServiceNowSinkAPIImpl servicenowSinkAPIImpl = new ServiceNowSinkAPIImpl(config);
    RestRequest restRequest = servicenowSinkAPIImpl.getRestRequest(jsonObject);
    restRequests.add(restRequest);
    if (restRequests.size() == config.getMaxRecordsPerBatch()) {
      apiResponse = servicenowSinkAPIImpl.createPostRequest(restRequests);
      if (apiResponse.getHttpStatus() == HttpStatus.SC_CREATED) {
        restRequests.clear();
      }
    }
  }

    @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
//call payload request
  }


}
