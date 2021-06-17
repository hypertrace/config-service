package ai.traceable.anomaly.config.service.exclusion.converters;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;

interface ConfigConverter<T> {
  T convert(Value value) throws InvalidProtocolBufferException;

  Value convert(T config) throws InvalidProtocolBufferException;
}
