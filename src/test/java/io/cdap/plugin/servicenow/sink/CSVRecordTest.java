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

import org.junit.Assert;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;

public class CSVRecordTest {

@Test
  public void testGetCSVRecords(){
  List<String> columnNames = new ArrayList<>();
  List<String> values = new ArrayList<>();
  columnNames.add("name");
  values.add("Ram");
  Object o = new CSVRecord(columnNames, values);
  CSVRecord csvRecord = new CSVRecordBuilder().setColumnNames(columnNames).setValues(values).build();
  csvRecord.iterator();
  csvRecord.toString();
  Assert.assertTrue(csvRecord.equals(o));
  Assert.assertEquals(1, csvRecord.getColumnNames().size());
  Assert.assertEquals(1, csvRecord.getValues().size());
  Assert.assertEquals("CSVRecord{columnNames=[name], values=[Ram]}", csvRecord.toString());
}

@Test
  public void testObjectWNull(){
  List<String> columnNames = new ArrayList<>();
  List<String> values = new ArrayList<>();
  columnNames.add("name");
  values.add("Ram");
  Object o = null;
  CSVRecord csvRecord = new CSVRecordBuilder().setColumnNames(columnNames).setValues(values).build();
  Assert.assertFalse(csvRecord.equals(o));
}

@Test
  public void testHashcode(){
  List<String> columnNames = new ArrayList<>();
  List<String> values = new ArrayList<>();
  columnNames.add("name");
  values.add("Ram");
  CSVRecord csvRecord = new CSVRecordBuilder().setColumnNames(columnNames).setValues(values).build();
  Assert.assertEquals(104668788, csvRecord.hashCode());
}
}
