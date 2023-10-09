/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.servicenow.sink.model;

/**
 * Model class for Schema Field from Schema API
 */
public class ServiceNowSchemaField {
  private final String label;
  private final String exampleValue;
  private final String internalType;
  private final String name;

  public ServiceNowSchemaField(String label, String exampleValue, String internalType, String name) {
    this.label = label;
    this.exampleValue = exampleValue;
    this.internalType = internalType;
    this.name = name;
  }

  public String getLabel() {
    return label;
  }

  public String getExampleValue() {
    return exampleValue;
  }

  public String getInternalType() {
    return internalType;
  }

  public String getName() {
    return name;
  }
}
