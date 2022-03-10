package org.hypertrace.config.change.event.v1;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class ConfigChangeEventValueSerde implements Serde<ConfigChangeEventValue> {
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public void close() {}

  @Override
  public Serializer<ConfigChangeEventValue> serializer() {
    return new ConfigChangeEventValueSerializer();
  }

  @Override
  public Deserializer<ConfigChangeEventValue> deserializer() {
    return new ConfigChangeEventValueDeserializer();
  }
}
