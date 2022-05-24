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

import java.util.ArrayList;
import java.util.List;

public class CSVRecordBuilder {

  List<String> columnNames = new ArrayList<>();
  List<String> values = new ArrayList<>();

    public CSVRecordBuilder setColumnNames(List<String> columnNames) {
      this.columnNames = columnNames;
      return this;
    }

    public CSVRecordBuilder setValues(List<String> values) {
      this.values = values;
      return this;
    }

    public CSVRecord build() {
      return new CSVRecord(columnNames, values);
    }
  }

