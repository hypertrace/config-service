package org.hypertrace.partitioner.config.service.store;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import lombok.Value;
import org.hypertrace.core.documentstore.Key;

@Value
public class PartitionerProfileKey implements Key {

  String profile;

  @Override
  public String toString() {
    return UUID.nameUUIDFromBytes(profile.getBytes(StandardCharsets.UTF_8)).toString();
  }
}
