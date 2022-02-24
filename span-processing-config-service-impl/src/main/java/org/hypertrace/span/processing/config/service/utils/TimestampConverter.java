package org.hypertrace.span.processing.config.service.utils;

import com.google.protobuf.Timestamp;
import java.time.Instant;

public class TimestampConverter {
  public static Timestamp convert(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.toEpochMilli())
        .setNanos(instant.getNano())
        .build();
  }
}
