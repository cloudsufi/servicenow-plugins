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
package io.cdap.plugin.servicenow.sink;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.plugin.servicenow.ServiceNowBaseConfig;
import io.cdap.plugin.servicenow.source.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.source.util.ServiceNowConstants;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;


/**
 *
 */
public class ServicenowOutputFormatProvider implements OutputFormatProvider {
  private static final Logger LOG = LoggerFactory.getLogger(ServicenowOutputFormatProvider.class);

  private final Map<String, String> configMap;
  private ServiceNowBaseConfig config;

  /**
   * Gets properties from config and stores them as properties in map for Mapreduce.
   *
   * @param config Servicenow batch sink configuration
   */
  public ServicenowOutputFormatProvider(ServiceNowSinkConfig config) throws OAuthProblemException,
    OAuthSystemException {
    ImmutableMap.Builder<String, String> configBuilder = new ImmutableMap.Builder<String, String>()
      .put(ServiceNowConstants.PROPERTY_TABLE_NAME, config.getTableName())
      .put(ServiceNowConstants.PROPERTY_OPERATION, config.getOperation())
      .put(ServiceNowConstants.PROPERTY_MAX_RECORDS_PER_BATCH, config.getMaxRecordsPerBatch().toString());

    ServiceNowTableAPIClientImpl restApi = new ServiceNowTableAPIClientImpl(config);
    restApi.getAccessToken();
    configBuilder.put(ServiceNowConstants.PROPERTY_USER, config.getUser())
      .put(ServiceNowConstants.PROPERTY_PASSWORD, config.getPassword())
      .put(ServiceNowConstants.PROPERTY_CLIENT_ID, config.getClientId())
      .put(ServiceNowConstants.PROPERTY_CLIENT_SECRET, config.getClientSecret())
      .put(ServiceNowConstants.PROPERTY_API_ENDPOINT, config.getRestApiEndpoint());

    this.configMap = configBuilder.build();
  }
  @Override
  public String getOutputFormatClassName() {
    return null;
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    return configMap;
  }
}
