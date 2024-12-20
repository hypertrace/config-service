package ai.traceable.sensitivedata.config.service;

import ai.traceable.sensitivedata.config.service.v1.InvalidJsonPolicy;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import io.grpc.Channel;
import jakarta.inject.Inject;
import java.time.Duration;
import lombok.Builder;
import lombok.Value;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

class SensitiveDataServiceConfig {

  private static final String SENSITIVE_DATA_CONFIG_SERVICE_CONFIG =
      "sensitive.data.config.service";

  private static final String INSIGHTS_SERVICE_CONFIG = "insights.service.config";
  private static final String INSIGHTS_SERVICE_CONFIG_REQUEST_TIMEOUT =
      INSIGHTS_SERVICE_CONFIG + ".request.timeout";
  private static final String DEFAULT_PII_FILTER_CONFIG = "default.pii.filter.config";
  private static final String DEFAULT_REDACTION_RULES = "default.redaction.rules";
  private static final String DEFAULT_PARAM_TYPE_REDACTION_STRATEGY =
      "default.param.type.redaction.strategy";
  private static final String DEFAULT_AUTOMATIC_SECRET_REDACTION_ENABLED =
      "default.automatic.secret.redaction.enabled";
  private static final String DEFAULT_FULL_PRIVACY_MODE_ENABLED =
      "default.full.privacy.mode.enabled";

  private final GrpcChannelRegistry channelRegistry;
  private final Config sensitiveDataConfig;
  private final Config config;
  private final DefaultRedactionRules defaultRedactionRules;

  @Inject
  SensitiveDataServiceConfig(GrpcChannelRegistry channelRegistry, Config config) {
    this.channelRegistry = channelRegistry;
    this.sensitiveDataConfig = config.getConfig(SENSITIVE_DATA_CONFIG_SERVICE_CONFIG);
    this.config = config;
    this.defaultRedactionRules = this.buildDefaultRedactionRules();
  }

  PiiFilterConfig defaultPiiFilterConfig() {
    return SensitiveDataConfigUtils.toPiiFilterConfig(
        sensitiveDataConfig.getConfig(DEFAULT_PII_FILTER_CONFIG));
  }

  DefaultRedactionRules defaultRedactionRules() {
    return this.defaultRedactionRules;
  }

  RedactionStrategy defaultParamTypeRedactionStrategy() {
    return RedactionStrategy.valueOf(
        sensitiveDataConfig.getString(DEFAULT_PARAM_TYPE_REDACTION_STRATEGY));
  }

  boolean defaultAutomaticRedactionStrategy() {
    return sensitiveDataConfig.getBoolean(DEFAULT_AUTOMATIC_SECRET_REDACTION_ENABLED);
  }

  InvalidJsonPolicy defaultInvalidJsonPolicy() {
    return defaultPiiFilterConfig().getInvalidJsonPolicy();
  }

  boolean defaultFullPrivacyMode() {
    return sensitiveDataConfig.getBoolean(DEFAULT_FULL_PRIVACY_MODE_ENABLED);
  }

  GrpcClientConfig insightsClientConfig() {
    return GrpcClientConfig.builder()
        .channel(
            channelRegistry.forPlaintextAddress(
                config.getConfig(INSIGHTS_SERVICE_CONFIG).getString("host"),
                config.getConfig(INSIGHTS_SERVICE_CONFIG).getInt("port")))
        .timeout(config.getDuration(INSIGHTS_SERVICE_CONFIG_REQUEST_TIMEOUT))
        .build();
  }

  ClientConfig defaultClientConfig() {
    return ClientConfig.DEFAULT;
  }

  private DefaultRedactionRules buildDefaultRedactionRules() {
    ConfigObject defaultRules = sensitiveDataConfig.getObject(DEFAULT_REDACTION_RULES);
    return new DefaultRedactionRules(defaultRules);
  }

  @Value
  @Builder
  static class GrpcClientConfig {
    Channel channel;
    Duration timeout;
  }
}
