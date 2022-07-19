package ai.traceable.config.utils;

import com.google.protobuf.Timestamp;
import java.time.Instant;

public class TimestampConverter {
  public Timestamp convert(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }
}
