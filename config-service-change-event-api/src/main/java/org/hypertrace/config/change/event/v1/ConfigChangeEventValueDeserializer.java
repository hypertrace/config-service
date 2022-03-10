package org.hypertrace.config.change.event.v1;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ConfigChangeEventValueDeserializer implements Deserializer<ConfigChangeEventValue> {

    @Override
    public ConfigChangeEventValue deserialize(String topic, byte[] data) {
      try {
        return ConfigChangeEventValue.parseFrom(data);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }

  }
}
