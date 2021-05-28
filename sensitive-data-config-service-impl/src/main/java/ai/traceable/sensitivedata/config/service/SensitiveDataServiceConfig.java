package ai.traceable.sensitivedata.config.service;

import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import com.typesafe.config.Config;
import io.grpc.Channel;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

class SensitiveDataServiceConfig {

  private static final String SENSITIVE_DATA_CONFIG_SERVICE_CONFIG =
      "sensitive.data.config.service";

  private static final String INSIGHTS_SERVICE_CONFIG = "insights.service.config";
  private static final String DEFAULT_PII_FILTER_CONFIG = "default.pii.filter.config";
  private static final String DEFAULT_REDACTION_RULES = "default.redaction.rules";
  private static final String DEFAULT_PARAM_TYPE_REDACTION_STRATEGY =
      "default.param.type.redaction.strategy";
  private static final String DEFAULT_AUTOMATIC_SECRET_REDACTION_ENABLED =
      "default.automatic.secret.redaction.enabled";

  private final GrpcChannelRegistry channelRegistry;
  private final Config sensitiveDataConfig;
  private final Config config;

  @Inject
  SensitiveDataServiceConfig(GrpcChannelRegistry channelRegistry, Config config) {
    this.channelRegistry = channelRegistry;
    this.sensitiveDataConfig = config.getConfig(SENSITIVE_DATA_CONFIG_SERVICE_CONFIG);
    this.config = config;
  }

  PiiFilterConfig defaultPiiFilterConfig() {
    return SensitiveDataConfigUtils.toPiiFilterConfig(
        sensitiveDataConfig.getConfig(DEFAULT_PII_FILTER_CONFIG));
  }

  List<RedactionRule> defaultConditionalRedactionRules() {
    if (sensitiveDataConfig
        .getConfig(DEFAULT_REDACTION_RULES)
        .hasPath(SensitiveDataConfigUtils.REDACTION_RULES_CONFIG)) {
      return SensitiveDataConfigUtils.toRedactionRules(
          sensitiveDataConfig.getConfig(DEFAULT_REDACTION_RULES));
    }
    return Collections.emptyList();
  }

  RedactionStrategy defaultParamTypeRedactionStrategy() {
    return RedactionStrategy.valueOf(
        sensitiveDataConfig.getString(DEFAULT_PARAM_TYPE_REDACTION_STRATEGY));
  }

  boolean defaultAutomaticRedactionStrategy() {
    return sensitiveDataConfig.getBoolean(DEFAULT_AUTOMATIC_SECRET_REDACTION_ENABLED);
  }

  Channel insightsChannel() {
    return channelRegistry.forAddress(
        config.getConfig(INSIGHTS_SERVICE_CONFIG).getString("host"),
        config.getConfig(INSIGHTS_SERVICE_CONFIG).getInt("port"));
  }
}
