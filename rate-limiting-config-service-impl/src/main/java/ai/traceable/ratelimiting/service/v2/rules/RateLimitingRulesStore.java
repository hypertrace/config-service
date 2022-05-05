package ai.traceable.ratelimiting.service.v2.rules;

import static ai.traceable.ratelimiting.service.v2.constants.RateLimitingConfigConstants.RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME;
import static ai.traceable.ratelimiting.service.v2.constants.RateLimitingConfigConstants.RATE_LIMITING_RULE_CONFIG_RESOURCE_NAMESPACE;

import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RateLimitingRulesStore extends IdentifiedObjectStore<RateLimitingRule> {
  Logger log = LoggerFactory.getLogger(RateLimitingRulesManager.class);

  @Inject
  public RateLimitingRulesStore(ConfigServiceBlockingStub configServiceBlockingStub) {
    super(
        configServiceBlockingStub,
        RATE_LIMITING_RULE_CONFIG_RESOURCE_NAMESPACE,
        RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME);
  }

  @Override
  protected Optional<RateLimitingRule> buildDataFromValue(Value value) {
    try {
      RateLimitingRule.Builder builder = RateLimitingRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception exception) {
      log.error(
          "Parsing the value {} into RateLimitingRule failed with an exception", value, exception);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(RateLimitingRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(RateLimitingRule rule) {
    return rule.getId();
  }
}
