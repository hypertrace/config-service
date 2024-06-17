package ai.traceable.ast.config.service.store;

import static ai.traceable.ast.config.service.constants.AstConfigConstants.AST_CONFIG_NAMESPACE;

import ai.traceable.ast.config.service.v1.AstOverride;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
public class AstOverridesStore extends IdentifiedObjectStore<AstOverride> {

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
}
