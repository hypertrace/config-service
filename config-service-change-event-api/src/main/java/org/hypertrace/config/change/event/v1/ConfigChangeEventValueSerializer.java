package org.hypertrace.config.change.event.v1;

import org.apache.kafka.common.serialization.Serializer;

public class ConfigChangeEventValueSerializer implements Serializer<ConfigChangeEventValue> {

  @Override
  public byte[] serialize(String topic, ConfigChangeEventValue data) {
    return data.toByteArray();
  }
}
