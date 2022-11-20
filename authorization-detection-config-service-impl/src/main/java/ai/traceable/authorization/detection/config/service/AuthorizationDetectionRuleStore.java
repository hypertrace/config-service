package ai.traceable.authorization.detection.config.service;

import ai.traceable.authorization.detection.config.service.v1.AuthorizationDetectionRule;
import ai.traceable.authorization.detection.config.service.v1.AuthorizationDetectionRuleFilter;
import com.google.protobuf.Value;
import java.util.Collections;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

class AuthorizationDetectionRuleStore
    extends IdentifiedObjectStoreWithFilter<
        AuthorizationDetectionRule, AuthorizationDetectionRuleFilter> {
  private static final String AUTHORIZATION_DETECTION_RULE_RESOURCE_NAME =
      "authorization-detection-rule";
  private static final String AUTHORIZATION_DETECTION_NAMESPACE = "authorization-detection";

  @Inject
  AuthorizationDetectionRuleStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        AUTHORIZATION_DETECTION_NAMESPACE,
        AUTHORIZATION_DETECTION_RULE_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<AuthorizationDetectionRule> buildDataFromValue(Value ruleValue) {
    try {
      AuthorizationDetectionRule.Builder builder = AuthorizationDetectionRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(ruleValue, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(AuthorizationDetectionRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(AuthorizationDetectionRule rule) {
    return rule.getId();
  }

  @Override
  protected Optional<AuthorizationDetectionRule> filterConfigData(
      AuthorizationDetectionRule data, AuthorizationDetectionRuleFilter filter) {

    if (!filter.hasScope() || !data.hasScope()) {
      return Optional.of(data);
    }

    if (!Collections.disjoint(
        filter.getScope().getEnvironmentNamesList(), data.getScope().getEnvironmentNamesList())) {
      return Optional.of(data);
    }

    return Optional.empty();
  }
}
