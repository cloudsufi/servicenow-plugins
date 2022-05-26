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
package io.cdap.plugin.servicenow.sink.transform;

import com.google.gson.JsonObject;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 *  Transforms structured record to JSON object
 */
public class ServiceNowTransformer {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceNowTransformer.class);
  private static final int DEFAULT_SCALE = 8;
  private static final String DATE_PATTERN = "yyyy-MM-dd";
  private static final String DATE_TIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss";

  @Nullable
  public JsonObject transform(@Nullable StructuredRecord record) {
    if (record == null) {
      // Return 'null' value as it is
      return null;
    }
    List<Schema.Field> fields = Objects.requireNonNull(record.getSchema().getFields(),
                                                       "Schema fields cannot be empty");
    JsonObject jsonObject = new JsonObject();
    for (Schema.Field field : fields) {
      String fieldName = field.getName();
      Schema nonNullableSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable()
        : field.getSchema();
      jsonObject.addProperty(field.getName(), String.valueOf(extractValue(fieldName, record.get(field.getName()),
                                                                          nonNullableSchema)));
    }
    return jsonObject;
  }

  private Object extractValue(String fieldName, Object value, Schema schema) {
    if (value == null) {
      // Return 'null' value as it is
      return null;
    }

    Schema.LogicalType fieldLogicalType = schema.getLogicalType();
    // Get values of logical types properly
    if (fieldLogicalType != null) {
      switch (fieldLogicalType) {
        case TIMESTAMP_MILLIS:
          return extractUTCDateTime(TimeUnit.MILLISECONDS.toMicros((long) value)).toString();
        case TIMESTAMP_MICROS:
          return extractUTCDateTime((Long) value).toString();
        case TIME_MILLIS:
          LocalTime time = LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(((Integer) value)));
          return time.toString();
        case TIME_MICROS:
          LocalTime localTime = LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos((Long) value));
          return localTime.toString();
        case DATE:
          SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_PATTERN);
          Date date = new Date(Integer.parseInt(value.toString()) * TimeUnit.DAYS.toMillis(1));
          return dateFormat.format(date);
        case DATETIME:
          SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_PATTERN);
          Date dateTime;
          try {
            dateTime = dateTimeFormat.parse(value.toString());
          } catch (ParseException exception) {
            LOG.error("Cannot parse the value {} to dateTime", value);
            throw new UnexpectedFormatException(
              String.format("Datetime field with value '%s' is not in ISO-8601 format.", schema.getDisplayName(),
                            fieldName), exception);
          }
          return dateTimeFormat.format(dateTime);
        case DECIMAL:
          return new BigDecimal(String.valueOf(value)).setScale(DEFAULT_SCALE,
                                                                BigDecimal.ROUND_HALF_UP);
        default:
          throw new UnexpectedFormatException(String.format("Field '%s' is of unsupported type '%s'", fieldName,
                                                            fieldLogicalType.name().toLowerCase()));
      }
    }

    Schema.Type fieldType = schema.getType();
    switch (fieldType) {
      case DOUBLE:
      case FLOAT:
        return new BigDecimal(String.valueOf(value)).setScale(DEFAULT_SCALE, BigDecimal.ROUND_HALF_UP);
      case BOOLEAN:
        return (Boolean) value;
      case INT:
        return (Integer) value;
      case BYTES:
        byte[] bytes = value instanceof ByteBuffer ? Bytes.getBytes((ByteBuffer) value) : (byte[]) value;
        byte[] encoded = Base64.getEncoder().encode(bytes);
        return new String(encoded);
      case LONG:
        return (int) (long) value;
      case STRING:
        return (String) value;
      case MAP:
        return value.toString();
      default:
        throw new UnexpectedFormatException(String.format("Field '%s' is of unsupported type '%s'", fieldName,
                                                          fieldLogicalType.name().toLowerCase()));
    }
  }

  /**
   * Get UTC zoned date and time represented by the specified timestamp in microseconds.
   *
   * @param micros timestamp in microseconds
   * @return UTC {@link ZonedDateTime} corresponding to the specified timestamp
   */
  private ZonedDateTime extractUTCDateTime(long micros) {
    ZoneId zoneId = ZoneId.ofOffset("UTC", ZoneOffset.UTC);
    long mod = TimeUnit.MICROSECONDS.convert(1, TimeUnit.SECONDS);
    int fraction = (int) (micros % mod);
    long tsInSeconds = TimeUnit.MICROSECONDS.toSeconds(micros);
    // create an Instant with time in seconds and fraction which will be stored as nano seconds.
    Instant instant = Instant.ofEpochSecond(tsInSeconds, TimeUnit.MICROSECONDS.toNanos(fraction));
    return ZonedDateTime.ofInstant(instant, zoneId);
  }
  
}
