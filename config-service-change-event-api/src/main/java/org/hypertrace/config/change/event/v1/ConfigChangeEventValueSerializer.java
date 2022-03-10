package org.hypertrace.config.change.event.v1;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ConfigChangeEventValueSerializer implements Serializer<ConfigChangeEventValue> {

    @Override
    public byte[] serialize(String topic, ConfigChangeEventValue data) {
      return data.toByteArray();
    }
}
