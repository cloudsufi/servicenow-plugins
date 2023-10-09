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
package io.cdap.plugin.servicenow.connector;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;

import java.text.NumberFormat;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Map;

/**
 * Utility class for converting the record from ServiceNow data type to CDAP schema data types
 */
public class ServiceNowRecordConverter {
  private static final String DATE_PATTERN = "yyyy-MM-dd";
  private static final String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
  private static final String TIME_PATTERN = "HH:mm:ss";

  public static void convertToValue(String fieldName, Schema fieldSchema, Map<String, String> record,
                                    StructuredRecord.Builder recordBuilder) {
    String fieldValue = record.get(fieldName);
    if (fieldValue == null || fieldValue.isEmpty()) {
      // Set 'null' value as it is
      recordBuilder.set(fieldName, null);
      return;
    }

    fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
    Schema.LogicalType fieldLogicalType = fieldSchema.getLogicalType();
    // Get values of logical types properly
    if (fieldLogicalType != null) {
      switch (fieldLogicalType) {
        case DATETIME:
          DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(DATE_TIME_PATTERN);
          try {
            recordBuilder.setDateTime(fieldName, LocalDateTime.parse(fieldValue, dateTimeFormatter));
          } catch (DateTimeParseException exception) {
            throw new UnexpectedFormatException(
              String.format("Field '%s' of type '%s' with value '%s' is not in ISO-8601 format.",
                            fieldName, fieldSchema.getDisplayName(), fieldValue), exception);
          }
          return;
        case DATE:
          DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(DATE_PATTERN);
          try {
            recordBuilder.setDate(fieldName, LocalDate.parse(fieldValue, dateFormatter));
          } catch (DateTimeParseException exception) {
            throw new UnexpectedFormatException(
              String.format("Field '%s' of type '%s' with value '%s' is not in ISO-8601 format.",
                            fieldName, fieldSchema.getDisplayName(), fieldValue), exception);
          }
          return;
        case TIME_MICROS:
          DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern(TIME_PATTERN);
          try {
            recordBuilder.setTime(fieldName, LocalTime.parse(fieldValue, timeFormatter));
          } catch (DateTimeParseException exception) {
            throw new UnexpectedFormatException(
              String.format("Field '%s' of type '%s' with value '%s' is not in ISO-8601 format.",
                            fieldName, fieldSchema.getDisplayName(), fieldValue), exception);
          }
          return;
        default:
          throw new IllegalStateException(String.format("Field '%s' is of unsupported type '%s'", fieldName,
                                                            fieldLogicalType.name().toLowerCase()));
      }
    }

    Schema.Type fieldType = fieldSchema.getType();
    switch (fieldType) {
      case STRING:
        recordBuilder.set(fieldName, fieldValue);
        return;
      case DOUBLE:
        recordBuilder.set(fieldName, convertToDoubleValue(fieldValue));
        return;
      case INT:
        recordBuilder.set(fieldName, convertToIntegerValue(fieldValue));
        return;
      case BOOLEAN:
        recordBuilder.set(fieldName, convertToBooleanValue(fieldValue));
        return;
      default:
        throw new IllegalStateException(
          String.format("Record type '%s' is not supported for field '%s'", fieldType.name(), fieldName));
    }

  }

  @VisibleForTesting
  public static Double convertToDoubleValue(String fieldValue) {
    try {
      return NumberFormat.getNumberInstance(Locale.US).parse(fieldValue).doubleValue();
    } catch (ParseException exception) {
      throw new UnexpectedFormatException(
        String.format("Field with value '%s' is not in valid format.", fieldValue), exception);
    }
  }

  @VisibleForTesting
  public static Integer convertToIntegerValue(String fieldValue) {
    try {
      return NumberFormat.getNumberInstance(java.util.Locale.US).parse(fieldValue).intValue();
    } catch (ParseException exception) {
      throw new UnexpectedFormatException(
        String.format("Field with value '%s' is not in valid format.", fieldValue), exception);
    }
  }

  @VisibleForTesting
  public static Boolean convertToBooleanValue(String fieldValue) {
    if (fieldValue.equalsIgnoreCase(Boolean.TRUE.toString()) ||
                                      fieldValue.equalsIgnoreCase(Boolean.FALSE.toString())) {
      return Boolean.parseBoolean(fieldValue);
    }
    throw new UnexpectedFormatException(
      String.format("Field with value '%s' is not in valid format.", fieldValue));
    
  }
}
