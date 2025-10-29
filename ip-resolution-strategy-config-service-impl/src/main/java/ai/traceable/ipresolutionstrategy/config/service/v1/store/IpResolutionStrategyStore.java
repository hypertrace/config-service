package ai.traceable.ipresolutionstrategy.config.service.v1.store;

import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfig;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfigData;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyFilter;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

@Slf4j
public class IpResolutionStrategyStore
    extends IdentifiedObjectStoreWithFilter<
        IpResolutionStrategyConfig, IpResolutionStrategyFilter> {
  private static final String RESOURCE_NAME = "ip-resolution-strategy-config";
  private static final String NAMESPACE = "ip-resolution-strategy";

  @Inject
  public IpResolutionStrategyStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(configServiceBlockingStub, NAMESPACE, RESOURCE_NAME, configChangeEventGenerator);
  }

  @Override
  protected Optional<IpResolutionStrategyConfig> buildDataFromValue(Value value) {
    IpResolutionStrategyConfig.Builder builder = IpResolutionStrategyConfig.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Failed to convert config to IpResolutionStrategyConfig: {}", value, e);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(IpResolutionStrategyConfig data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected String getContextFromData(IpResolutionStrategyConfig data) {
    return data.getId();
  }

  @Override
  protected java.util.List<ContextualConfigObject<IpResolutionStrategyConfig>> orderFetchedObjects(
      java.util.List<ContextualConfigObject<IpResolutionStrategyConfig>> objects) {
    // No ranking semantics; return as-is
    return objects;
  }

  @Override
  protected Optional<IpResolutionStrategyConfig> filterConfigData(
      IpResolutionStrategyConfig data, IpResolutionStrategyFilter filter) {
    IpResolutionStrategyConfigData d = data.getData();

    if (filter.hasDisabled() && d.getDisabled() != filter.getDisabled()) {
      return Optional.empty();
    }
    if (!filter.getIdsList().isEmpty() && !filter.getIdsList().contains(data.getId())) {
      return Optional.empty();
    }

    // Environment scoping
    if (!filter.getEnvironmentNamesList().isEmpty()) {
      if (d.getScope().getEnvironmentScope().getEnvironmentNamesList().isEmpty()) {
        // env-wide applies to all, keep
      } else if (Collections.disjoint(
          filter.getEnvironmentNamesList(),
          d.getScope().getEnvironmentScope().getEnvironmentNamesList())) {
        return Optional.empty();
      }
    }

    // Service scoping
    if (!filter.getServiceNamesList().isEmpty()) {
      if (d.getScope().getServiceScope().getServiceNamesList().isEmpty()) {
        // service-wide applies to all, keep
      } else if (Collections.disjoint(
          filter.getServiceNamesList(), d.getScope().getServiceScope().getServiceNamesList())) {
        return Optional.empty();
      }
    }

    return Optional.of(data);
  }
}
