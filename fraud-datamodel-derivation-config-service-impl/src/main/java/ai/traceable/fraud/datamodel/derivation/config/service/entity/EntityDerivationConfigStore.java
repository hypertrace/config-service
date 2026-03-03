package ai.traceable.fraud.datamodel.derivation.config.service.entity;

import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsRequest;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class EntityDerivationConfigStore
    extends IdentifiedObjectStoreWithFilter<
        EntityDerivationConfig, GetEntityDerivationConfigsRequest> {

  private static final String ENTITY_DERIVATION_RESOURCE_NAME = "entity-derivation";
  private static final String ENTITY_DERIVATION_CONFIG_NAMESPACE = "entity-derivation-config";

  @Inject
  public EntityDerivationConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        ENTITY_DERIVATION_CONFIG_NAMESPACE,
        ENTITY_DERIVATION_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<EntityDerivationConfig> buildDataFromValue(Value value) {
    try {
      EntityDerivationConfig.Builder builder = EntityDerivationConfig.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception exception) {
      log.error(
          "Parsing the value {} into EntityDerivationConfig failed with an exception",
          value,
          exception);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(EntityDerivationConfig config) {
    return ConfigProtoConverter.convertToValue(config);
  }

  @Override
  protected String getContextFromData(EntityDerivationConfig config) {
    return config.getId();
  }

  @Override
  protected Optional<EntityDerivationConfig> filterConfigData(
      EntityDerivationConfig data, GetEntityDerivationConfigsRequest request) {
    return EntityDerivationConfigFilterUtil.applyEntityFilter(data, request)
        ? Optional.of(data)
        : Optional.empty();
  }

  @Override
  public List<EntityDerivationConfig> getAllConfigData(
      RequestContext requestContext, GetEntityDerivationConfigsRequest request) {
    List<EntityDerivationConfig> configs = super.getAllConfigData(requestContext, request);
    return configs.stream()
        .filter(config -> filterConfigData(config, request).isPresent())
        .collect(Collectors.toUnmodifiableList());
  }
}
