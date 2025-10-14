package ai.traceable.auth.detection.config.service;

import ai.traceable.auth.detection.config.service.v1.AuthDetectionRule;
import ai.traceable.auth.detection.config.service.v1.AuthDetectionRuleFilter;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

class UserDefinedAuthDetectionRuleStore
    extends IdentifiedObjectStoreWithFilter<AuthDetectionRule, AuthDetectionRuleFilter> {
  private static final String AUTH_DETECTION_RULE_RESOURCE_NAME = "auth-detection-rule";
  private static final String AUTH_DETECTION_NAMESPACE = "auth-detection";

  @Inject
  UserDefinedAuthDetectionRuleStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        AUTH_DETECTION_NAMESPACE,
        AUTH_DETECTION_RULE_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<AuthDetectionRule> buildDataFromValue(Value ruleValue) {
    try {
      AuthDetectionRule.Builder builder = AuthDetectionRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(ruleValue, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(AuthDetectionRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(AuthDetectionRule rule) {
    return rule.getId();
  }

  @Override
  protected Optional<AuthDetectionRule> filterConfigData(
      AuthDetectionRule data, AuthDetectionRuleFilter filter) {

    if (!filter.hasScope() || !data.hasScope()) {
      return Optional.of(data);
    }

    if (!filter.getIdsList().isEmpty() && !filter.getIdsList().contains(data.getId())) {
      return Optional.empty();
    }

    if (!Collections.disjoint(
        filter.getScope().getEnvironmentNamesList(), data.getScope().getEnvironmentNamesList())) {
      return Optional.of(data);
    }

    return Optional.empty();
  }
}
