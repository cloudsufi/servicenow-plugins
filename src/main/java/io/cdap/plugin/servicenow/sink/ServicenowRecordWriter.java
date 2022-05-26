/*package io.cdap.plugin.servicenow.sink;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.plugin.servicenow.restapi.RestAPIResponse;
import io.cdap.plugin.servicenow.source.apiclient.ServiceNowTableAPIClientImpl;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public abstract class ServicenowRecordWriter extends RecordWriter<NullWritable, JsonObject> {

  private ServiceNowSinkConfig config;


  public void createRecord(String tableName, HttpEntity entity) throws OAuthProblemException, OAuthSystemException,
  IOException {
    Integer counter = 1;
    Map<String, String> header = new HashMap<>();
    header.put("Content-Type", "application/json");
    header.put("Accept", "application/json");
    Request request = new Request();

    request.setUrl("/api/now/table/" + config.getTableName());
    request.setId(counter.toString());
    counter ++;
    request.setHeader(header);
    request.setMethod("POST");
    request.setBody("");



    HttpClient httpClient = HttpClientBuilder.create().build();
    HttpPost post = new HttpPost();
    Gson gson = new Gson();
    StringEntity postingString = new StringEntity(gson.toJson(request));
    post.setEntity(postingString);
    httpClient.execute(post);


    ServiceNowTableAPIClientImpl restApi = new ServiceNowTableAPIClientImpl(config);
    RestAPIResponse apiResponse = null;
    String accessToken = restApi.getAccessToken();

    }

  @Override
  public void write(NullWritable key, CSVRecord csvRecord) throws IOException {
    //transform method sae
    //returns the jsonobject
    //to string
    //encoded form
    //set it into body
    csvBufferSizeCheck.reset();
    csvBufferSizeCheck.write(csvRecord);

    if (csvBuffer.size() + csvBufferSizeCheck.size() > maxBytesPerBatch ||
      csvBuffer.getRecordsCount() >= maxRecordsPerBatch) {
      submitCurrentBatch();
    }

    csvBuffer.write(csvRecord);
  }


  @Override
  public void close(Reporter reporter) throws IOException {

  }
}*/
