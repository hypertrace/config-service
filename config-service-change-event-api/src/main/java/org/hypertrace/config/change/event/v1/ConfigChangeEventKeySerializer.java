package org.hypertrace.config.change.event.v1;

import org.apache.kafka.common.serialization.Serializer;

public class ConfigChangeEventKeySerializer implements Serializer<ConfigChangeEventKey> {
  @Override
  public byte[] serialize(String topic, ConfigChangeEventKey data) {
    return data.toByteArray();
  }
}
