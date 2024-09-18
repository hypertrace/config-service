package ai.traceable.ast.config.service.store;

import static ai.traceable.ast.config.service.constants.AstConfigConstants.AST_CONFIG_NAMESPACE;

import ai.traceable.ast.config.service.v1.AstOverride;
import ai.traceable.ast.config.service.v1.AstOverrideFilter;
import ai.traceable.ast.config.service.v1.IdFilter;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
public class AstOverridesStore
    extends IdentifiedObjectStoreWithFilter<AstOverride, AstOverrideFilter> {

  private static final String AST_OVERRIDES_CONFIG_RESOURCE_NAME = "ast-overrides";

  @Inject
  public AstOverridesStore(
      final ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      final ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        AST_CONFIG_NAMESPACE,
        AST_OVERRIDES_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  @SneakyThrows
  protected Optional<AstOverride> buildDataFromValue(Value value) {
    AstOverride.Builder configBuilder = AstOverride.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, configBuilder);
    return Optional.of(configBuilder.build());
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(AstOverride astOverride) {
    return ConfigProtoConverter.convertToValue(astOverride);
  }

  @Override
  protected String getContextFromData(AstOverride astOverride) {
    return astOverride.getId();
  }

  @Override
  protected Optional<AstOverride> filterConfigData(
      final AstOverride data, final AstOverrideFilter filter) {
    if (!IdFilter.getDefaultInstance().equals(filter.getIdFilter())) {
      return filter.getIdFilter().getIdsList().contains(data.getId())
          ? Optional.of(data)
          : Optional.empty();
    }
    return Optional.of(data);
  }
}
