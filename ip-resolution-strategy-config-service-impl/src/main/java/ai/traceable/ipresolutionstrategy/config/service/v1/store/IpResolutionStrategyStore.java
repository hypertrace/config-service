package ai.traceable.ipresolutionstrategy.config.service.v1.store;

import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfig;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfigData;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfigServiceConfig;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyFilter;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class IpResolutionStrategyStore
    extends IdentifiedObjectStoreWithFilter<
        IpResolutionStrategyConfig, IpResolutionStrategyFilter> {
  private static final String RESOURCE_NAME = "ip-resolution-strategy-config";
  private static final String NAMESPACE = "ip-resolution-strategy";

  private final List<IpResolutionStrategyConfig> defaultIpResolutionStrategyConfigs;

  @Inject
  public IpResolutionStrategyStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      IpResolutionStrategyConfigServiceConfig config) {
    super(configServiceBlockingStub, NAMESPACE, RESOURCE_NAME, configChangeEventGenerator);
    this.defaultIpResolutionStrategyConfigs = config.getDefaultIpResolutionStrategyConfigs();
  }

  @Override
  public Optional<IpResolutionStrategyConfig> getData(RequestContext context, String id) {
    return super.getData(context, id)
        .or(
            () ->
                defaultIpResolutionStrategyConfigs.stream()
                    .filter(cfg -> cfg.getId().equals(id))
                    .findFirst());
  }

  @Override
  public List<IpResolutionStrategyConfig> getAllConfigData(RequestContext context) {
    List<IpResolutionStrategyConfig> configs = super.getAllConfigData(context);
    return mergeConfigs(configs, defaultIpResolutionStrategyConfigs);
  }

  @Override
  public List<IpResolutionStrategyConfig> getAllConfigData(
      RequestContext context, IpResolutionStrategyFilter filter) {
    // Fetch tenant configs without applying the disabled predicate so an explicitly disabled
    // override still suppresses a default config with the same id.
    IpResolutionStrategyFilter filterIgnoringDisabled = filter.toBuilder().clearDisabled().build();

    List<IpResolutionStrategyConfig> configs =
        super.getAllConfigData(context, filterIgnoringDisabled);

    Set<String> overriddenDefaultIds =
        configs.stream()
            .map(IpResolutionStrategyConfig::getId)
            .collect(Collectors.toUnmodifiableSet());

    List<IpResolutionStrategyConfig> filteredDefaults =
        defaultIpResolutionStrategyConfigs.stream()
            .filter(cfg -> !overriddenDefaultIds.contains(cfg.getId()))
            .filter(cfg -> filterConfigData(cfg, filterIgnoringDisabled).isPresent())
            .collect(Collectors.toUnmodifiableList());

    List<IpResolutionStrategyConfig> merged = mergeConfigs(configs, filteredDefaults);
    if (!filter.hasDisabled()) {
      return merged;
    }

    boolean disabled = filter.getDisabled();
    return merged.stream()
        .filter(cfg -> cfg.getData().getDisabled() == disabled)
        .collect(Collectors.toUnmodifiableList());
  }

  private static List<IpResolutionStrategyConfig> mergeConfigs(
      List<IpResolutionStrategyConfig> configs, List<IpResolutionStrategyConfig> defaults) {
    Map<String, IpResolutionStrategyConfig> merged = new LinkedHashMap<>();
    merged.putAll(
        defaults.stream()
            .collect(Collectors.toMap(IpResolutionStrategyConfig::getId, Function.identity())));
    merged.putAll(
        configs.stream()
            .collect(Collectors.toMap(IpResolutionStrategyConfig::getId, Function.identity())));
    return merged.values().stream().collect(Collectors.toUnmodifiableList());
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
