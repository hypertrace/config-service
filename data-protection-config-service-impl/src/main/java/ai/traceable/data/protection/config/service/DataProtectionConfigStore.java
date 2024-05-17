package ai.traceable.data.protection.config.service;

import ai.traceable.data.protection.config.service.v1.DataProtectionScope;
import ai.traceable.data.protection.config.service.v1.ScopedDataProtectionConfig;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
class DataProtectionConfigStore
    extends IdentifiedObjectStoreWithFilter<ScopedDataProtectionConfig, DataProtectionScope> {
  private static final String DATA_PROTECTION_CONFIG_RESOURCE_NAME = "dataProtection";
  private static final String DATA_PROTECTION_CONFIG_RESOURCE_NAMESPACE = "dataProtection";
  public static final String DEFAULT_DATA_PROTECTION_CONTEXT = "default";

  @Inject
  public DataProtectionConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        DATA_PROTECTION_CONFIG_RESOURCE_NAMESPACE,
        DATA_PROTECTION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<ScopedDataProtectionConfig> buildDataFromValue(Value value) {
    try {
      ScopedDataProtectionConfig.Builder builder = ScopedDataProtectionConfig.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception exception) {
      log.error(
          "Parsing value {} into ScopedDataProtectionConfig failed with an exception",
          value,
          exception);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(ScopedDataProtectionConfig config) {
    return ConfigProtoConverter.convertToValue(config);
  }

  @Override
  protected String getContextFromData(ScopedDataProtectionConfig data) {
    return getContextFromData(data.getScope());
  }

  protected String getContextFromData(DataProtectionScope scope) {
    if (scope.hasEnvironmentId()) {
      return scope.getEnvironmentId();
    } else {
      return DEFAULT_DATA_PROTECTION_CONTEXT;
    }
  }

  @Override
  protected Optional<ScopedDataProtectionConfig> filterConfigData(
      ScopedDataProtectionConfig data, DataProtectionScope filter) {
    if (!data.hasScope() || filter.getEnvironmentId().equals(data.getScope().getEnvironmentId())) {
      return Optional.of(data);
    }
    return Optional.empty();
  }

  @Override
  protected List<ContextualConfigObject<ScopedDataProtectionConfig>> orderFetchedObjects(
      List<ContextualConfigObject<ScopedDataProtectionConfig>> objects) {
    return objects.stream()
        .sorted(Comparator.comparing(object -> object.getData().hasScope()))
        .collect(Collectors.toUnmodifiableList());
  }
}
