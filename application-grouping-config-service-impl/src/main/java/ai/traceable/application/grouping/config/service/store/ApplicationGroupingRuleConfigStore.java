package ai.traceable.application.grouping.config.service.store;

import static ai.traceable.application.grouping.config.service.constants.ApplicationGroupingServiceConfigConstants.APPLICATION_GROUPING_CONFIG_NAMESPACE;
import static ai.traceable.application.grouping.config.service.constants.ApplicationGroupingServiceConfigConstants.APPLICATION_GROUPING_RULE_CONFIG_RESOURCE_NAME;

import ai.traceable.application.grouping.config.service.v1.ApplicationGroupingRuleConfig;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class ApplicationGroupingRuleConfigStore
    extends IdentifiedObjectStore<ApplicationGroupingRuleConfig> {

  @Inject
  public ApplicationGroupingRuleConfigStore(
      final ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      final ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        APPLICATION_GROUPING_CONFIG_NAMESPACE,
        APPLICATION_GROUPING_RULE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  @SneakyThrows
  protected Optional<ApplicationGroupingRuleConfig> buildDataFromValue(final Value value) {
    ApplicationGroupingRuleConfig.Builder configBuilder =
        ApplicationGroupingRuleConfig.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, configBuilder);
    return Optional.of(configBuilder.build());
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(
      final ApplicationGroupingRuleConfig applicationGroupingRuleConfig) {
    return ConfigProtoConverter.convertToValue(applicationGroupingRuleConfig);
  }

  @Override
  @SneakyThrows
  protected String getContextFromData(
      final ApplicationGroupingRuleConfig applicationGroupingRuleConfig) {
    return applicationGroupingRuleConfig.getId();
  }

  public Optional<ApplicationGroupingRuleConfig> findByRuleName(
      RequestContext requestContext, String ruleName) {
    return getAllConfigData(requestContext).stream()
        .filter(
            config -> config.getApplicationGroupingRuleConfigInfo().getRuleName().equals(ruleName))
        .findFirst();
  }
}
