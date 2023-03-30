package ai.traceable.jwt.extraction.config.service;

import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRuleFilter;
import com.google.protobuf.Value;
import java.util.Collections;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

class UserDefinedJwtExtractionRuleStore
    extends IdentifiedObjectStoreWithFilter<JwtExtractionRule, JwtExtractionRuleFilter> {
  private static final String JWT_EXTRACTION_RULE_RESOURCE_NAME = "jwt-extraction-rule";
  private static final String JWT_EXTRACTION_NAMESPACE = "jwt-extraction";

  @Inject
  UserDefinedJwtExtractionRuleStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        JWT_EXTRACTION_NAMESPACE,
        JWT_EXTRACTION_RULE_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<JwtExtractionRule> buildDataFromValue(Value ruleValue) {
    try {
      JwtExtractionRule.Builder builder = JwtExtractionRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(ruleValue, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(JwtExtractionRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(JwtExtractionRule rule) {
    return rule.getId();
  }

  @Override
  protected Optional<JwtExtractionRule> filterConfigData(
      JwtExtractionRule data, JwtExtractionRuleFilter filter) {
    if (filter.hasDisabled() && data.getDisabled() != filter.getDisabled()) {
      return Optional.empty();
    }

    if (!filter.hasScope() || !data.hasScope()) {
      return Optional.of(data);
    }

    if (!Collections.disjoint(
        filter.getScope().getEnvironmentScope().getEnvironmentNamesList(),
        data.getScope().getEnvironmentScope().getEnvironmentNamesList())) {
      return Optional.of(data);
    }

    return Optional.empty();
  }
}
