package ai.traceable.sensitivedata.config.service;

import ai.traceable.sensitivedata.config.service.v1.InvalidJsonPolicy;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import io.grpc.Channel;
import java.time.Duration;
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
  private static final String DEFAULT_FULL_PRIVACY_MODE_ENABLED =
      "default.full.privacy.mode.enabled";

  private static final String FEATURE_FLAG_SERVICE_CONFIG = "feature.flag.service.config";
  private static final String FEATURE_FLAG_SERVICE_CONFIG_REQUEST_TIMEOUT =
      FEATURE_FLAG_SERVICE_CONFIG + ".request.timeout";
  private static final String FEATURE_FLAG_SERVICE_CACHE_CONFIG =
      FEATURE_FLAG_SERVICE_CONFIG + ".cache";
  private static final String FEATURE_FLAG_SERVICE_CACHE_CONFIG_REFRESH_DURATION =
      FEATURE_FLAG_SERVICE_CACHE_CONFIG + ".refresh.duration";
  private static final String FEATURE_FLAG_SERVICE_CACHE_CONFIG_EXPIRATION_DURATION =
      FEATURE_FLAG_SERVICE_CACHE_CONFIG + ".expiration.duration";
  private static final String FEATURE_FLAG_SERVICE_CACHE_CONFIG_THREAD_POOL_SIZE =
      FEATURE_FLAG_SERVICE_CACHE_CONFIG + ".thread.pool.size";
  private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration DEFAULT_REFRESH_DURATION = Duration.ofMinutes(5);
  private static final Duration DEFAULT_EXPIRATION_DURATION = Duration.ofMinutes(15);
  private static final int DEFAULT_THREAD_POOL_SIZE = 1;

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

  Channel insightsChannel() {
    return channelRegistry.forAddress(
        config.getConfig(INSIGHTS_SERVICE_CONFIG).getString("host"),
        config.getConfig(INSIGHTS_SERVICE_CONFIG).getInt("port"));
  }

  Channel featureFlagServiceChannel() {
    return channelRegistry.forPlaintextAddress(
        config.getConfig(FEATURE_FLAG_SERVICE_CONFIG).getString("host"),
        config.getConfig(FEATURE_FLAG_SERVICE_CONFIG).getInt("port"));
  }

  public Duration getRequestTimeout() {
    return config.hasPath(FEATURE_FLAG_SERVICE_CONFIG_REQUEST_TIMEOUT)
        ? config.getDuration(FEATURE_FLAG_SERVICE_CONFIG_REQUEST_TIMEOUT)
        : DEFAULT_REQUEST_TIMEOUT;
  }

  public Duration getRefreshDuration() {
    return config.hasPath(FEATURE_FLAG_SERVICE_CACHE_CONFIG_REFRESH_DURATION)
        ? config.getDuration(FEATURE_FLAG_SERVICE_CACHE_CONFIG_REFRESH_DURATION)
        : DEFAULT_REFRESH_DURATION;
  }

  public Duration getExpirationDuration() {
    return config.hasPath(FEATURE_FLAG_SERVICE_CACHE_CONFIG_EXPIRATION_DURATION)
        ? config.getDuration(FEATURE_FLAG_SERVICE_CACHE_CONFIG_EXPIRATION_DURATION)
        : DEFAULT_EXPIRATION_DURATION;
  }

  public int getThreadPoolSize() {
    return config.hasPath(FEATURE_FLAG_SERVICE_CACHE_CONFIG_THREAD_POOL_SIZE)
        ? config.getInt(FEATURE_FLAG_SERVICE_CACHE_CONFIG_THREAD_POOL_SIZE)
        : DEFAULT_THREAD_POOL_SIZE;
  }

  private DefaultRedactionRules buildDefaultRedactionRules() {
    List<? extends ConfigObject> defaultRules =
        sensitiveDataConfig.getObjectList(DEFAULT_REDACTION_RULES);
    ConfigObject prepopulatedRules = sensitiveDataConfig.getObject(PREPOPULATED_REDACTION_RULES);
    return new DefaultRedactionRules(prepopulatedRules, defaultRules);
  }
}
