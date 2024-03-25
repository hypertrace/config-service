package ai.traceable.ast.config.service.rules;

import static ai.traceable.ast.config.service.constants.AstConfigConstants.AST_CONFIG_NAMESPACE;

import ai.traceable.ast.config.service.v1.AstFeatureConfig;
import ai.traceable.ast.config.service.v1.AstFeatureConfigFilter;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

@Slf4j
public class AstFeatureConfigStore
    extends IdentifiedObjectStoreWithFilter<AstFeatureConfig, AstFeatureConfigFilter> {
  private static final String AST_FEATURE_CONFIG_RESOURCE_NAME = "ast-feature-config";

  @Inject
  public AstFeatureConfigStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        AST_CONFIG_NAMESPACE,
        AST_FEATURE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  @SneakyThrows
  protected Optional<AstFeatureConfig> buildDataFromValue(Value value) {
    AstFeatureConfig.Builder configBuilder = AstFeatureConfig.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, configBuilder);
    return Optional.of(configBuilder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(AstFeatureConfig astFeatureConfig) {
    return ConfigProtoConverter.convertToValue(astFeatureConfig);
  }

  @Override
  protected String getContextFromData(AstFeatureConfig astFeatureConfig) {
    return astFeatureConfig.getEnvironmentId();
  }

  @Override
  protected Optional<AstFeatureConfig> filterConfigData(
      AstFeatureConfig featureConfig, AstFeatureConfigFilter filter) {
    return Optional.of(featureConfig)
        .filter(
            config ->
                !filter.hasEnvironmentIdFilter()
                    || filter
                        .getEnvironmentIdFilter()
                        .getEnvironmentIdsList()
                        .contains(config.getEnvironmentId()));
  }
}
