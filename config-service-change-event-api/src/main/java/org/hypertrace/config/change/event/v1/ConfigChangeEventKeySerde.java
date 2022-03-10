package org.hypertrace.config.change.event.v1;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class ConfigChangeEventKeySerde implements Serde<ConfigChangeEventKey> {
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public void close() {}

  @Override
  public Serializer<ConfigChangeEventKey> serializer() {
    return new ConfigChangeEventKeySerializer();
  }

  @Override
  public Deserializer<ConfigChangeEventKey> deserializer() {
    return new ConfigChangeEventKeyDeserializer();
  }
}
