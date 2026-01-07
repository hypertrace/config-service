package ai.traceable.data.obfuscation.config.service.rules;

import ai.traceable.data.obfuscation.config.service.v1.ObfuscationStrategy;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
public class DataObfuscationConfigStore
    extends IdentifiedObjectStoreWithFilter<ObfuscationStrategy, Object> {
  public static final String DATA_OBFUSCATION_CONFIG_NAMESPACE = "dataObfuscation";
  public static final String DATA_OBFUSCATION_CONFIG_RESOURCE_NAME = "dataObfuscationConfig";

  @Inject
  public DataObfuscationConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        DATA_OBFUSCATION_CONFIG_NAMESPACE,
        DATA_OBFUSCATION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<ObfuscationStrategy> filterConfigData(
      ObfuscationStrategy data, Object filter) {
    return Optional.of(data);
  }

  @Override
  protected Optional<ObfuscationStrategy> buildDataFromValue(Value value) {
    try {
      ObfuscationStrategy.Builder builder = ObfuscationStrategy.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Error deserializing value into Data Obfuscation Config", e);
    }

    return Optional.empty();
  }

  @Override
  protected Value buildValueFromData(ObfuscationStrategy data) {
    try {
      return ConfigProtoConverter.convertToValue(data);
    } catch (InvalidProtocolBufferException e) {
      log.error("Error serializing Obfuscation Strategy into Value", e);
    }

    return Value.newBuilder().getDefaultInstanceForType();
  }

  @Override
  protected String getContextFromData(ObfuscationStrategy data) {
    return data.getId();
  }
}
