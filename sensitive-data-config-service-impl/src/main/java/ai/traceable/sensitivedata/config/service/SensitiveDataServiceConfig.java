package ai.traceable.sensitivedata.config.service;

import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import io.grpc.Channel;
import java.util.List;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

class SensitiveDataServiceConfig {

  private static final String SENSITIVE_DATA_CONFIG_SERVICE_CONFIG =
      "sensitive.data.config.service";

  private static final String INSIGHTS_SERVICE_CONFIG = "insights.service.config";
  private static final String DEFAULT_PII_FILTER_CONFIG = "default.pii.filter.config";
  private static final String DEFAULT_REDACTION_RULES = "default.redaction.rules";
  private static final String PREPOPULATED_REDACTION_RULES = "default.prepopulated.redaction.rules";
  private static final String DEFAULT_PARAM_TYPE_REDACTION_STRATEGY =
      "default.param.type.redaction.strategy";
  private static final String DEFAULT_AUTOMATIC_SECRET_REDACTION_ENABLED =
      "default.automatic.secret.redaction.enabled";

  private final GrpcChannelRegistry channelRegistry;
  private final Config sensitiveDataConfig;
  private final Config config;
  private final DefaultRedactionRules defaultRedactionRules;

  @Inject
  SensitiveDataServiceConfig(GrpcChannelRegistry channelRegistry, Config config) {
    this.channelRegistry = channelRegistry;
    this.sensitiveDataConfig = config.getConfig(SENSITIVE_DATA_CONFIG_SERVICE_CONFIG);
    this.config = config;
    this.defaultRedactionRules = this.buildDefaultRedactionRues();
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

  Channel insightsChannel() {
    return channelRegistry.forAddress(
        config.getConfig(INSIGHTS_SERVICE_CONFIG).getString("host"),
        config.getConfig(INSIGHTS_SERVICE_CONFIG).getInt("port"));
  }

  private DefaultRedactionRules buildDefaultRedactionRues() {
    List<? extends ConfigObject> defaultRules =
        sensitiveDataConfig.getObjectList(DEFAULT_REDACTION_RULES);
    ConfigObject prepopulatedRules = sensitiveDataConfig.getObject(PREPOPULATED_REDACTION_RULES);
    return new DefaultRedactionRules(prepopulatedRules, defaultRules);
  }
}
