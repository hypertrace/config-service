package ai.traceable.config.service.feature.caching.client;

import static java.util.Objects.requireNonNull;

import ai.traceable.featureflag.v1.FeatureFlagServiceGrpc;
import ai.traceable.featureflag.v1.FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub;
import ai.traceable.featureflag.v1.GetCurrentFlagValuesRequest;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class FeatureCachingClient {
  private static final boolean DEFAULT_DATA_CLASSIFICATION_ENHANCED_OBFUSCATION_FLAG_VALUE = false;
  private static final boolean DEFAULT_DATA_CLASSIFICATION_FILTERED_OVERRIDES_FLAG_VALUE = false;
  private static final boolean DEFAULT_IPQS_ENABLED_VALUE = false;
  private static final boolean DEFAULT_USER_ATTRIBUTION_V2_FLAG_VALUE = true;
  private static final boolean DEFAULT_USER_ATTRIBUTION_V3_FLAG_VALUE = false;
  private static final boolean DEFAULT_DETECTION_EXCLUSION_V2_FLAG_VALUE = false;
  private static final boolean DEFAULT_SESSION_IDENTIFICATION_V2_FLAG_VALUE = false;
  private static final boolean DEFAULT_TPA_MODSEC_PROCESSING_DISABLED = false;
  private static final boolean DEFAULT_TPA_CORAZA_BASED_EVALUATION = true;
  private static final boolean DEFAULT_TPA_CRS_MSG_HIDE_MATCH_VALUE = false;
  private static final boolean DEFAULT_TPA_CUSTOM_RATE_LIMIT_CONFIG_DISABLED = true;
  private static final boolean DEFAULT_RASP_INSPECTION = false;
  private static final boolean DEFAULT_THREAT_SCORING_NOTIFICATION_RULE_MIGRATION_FLAG_VALUE =
      false;
  private static final boolean DEFAULT_TRACEABLE_EDGE_DECISION_FLAG_VALUE = false;
  private static final String DATA_CLASSIFICATION_ENHANCED_OBFUSCATION_FLAG =
      "data-classification.enhanced-obfuscation";
  private static final String DATA_CLASSIFICATION_FILTERED_OVERRIDES_FLAG =
      "data-classification.filtered-overrides";
  private static final String IPQS_ENABLED_FLAG = "enricher.ipqs-ip-intelligence";
  // Flag is no longer UI-only and has been renamed, but key can't be changed without creating anew
  private static final String USER_ATTRIBUTION_V2_FLAG = "ui.user-attribution-v2";
  private static final String USER_ATTRIBUTION_V3_FLAG = "ui.user-attribution-v3";
  private static final String DETECTION_EXCLUSION_V2_FLAG = "ui.detection-exclusions-v2";
  private static final String SESSION_IDENTIFICATION_V2_FLAG = "session-identification.v2";
  private static final String TPA_MODSEC_PROCESSING_DISABLED = "tpa.modsec-processing-disabled";
  private static final String TPA_CORAZA_BASED_EVALUATION = "tpa.coraza-based-evaluation";
  private static final String TPA_CRS_MSG_HIDE_MATCH_VALUE = "tpa.crs-msg-hide-match-value";
  private static final String TPA_CUSTOM_RATE_LIMIT_CONFIG_DISABLED =
      "tpa.custom-rate-limit-config-disabled";
  private static final String RASP_INSPECTION = "enricher.rasp-inspection";
  private static final String THREAT_SCORING_NOTIFICATION_RULE_MIGRATION_FLAG =
      "notifications.threat-scoring-configuration";
  private static final String TRACEABLE_EDGE_DECISION_FLAG = "traceable-edge.edge-decision";

  private static final List<String> ALL_FLAGS_TO_FETCH =
      List.of(
          DATA_CLASSIFICATION_ENHANCED_OBFUSCATION_FLAG,
          DATA_CLASSIFICATION_FILTERED_OVERRIDES_FLAG,
          IPQS_ENABLED_FLAG,
          USER_ATTRIBUTION_V2_FLAG,
          USER_ATTRIBUTION_V3_FLAG,
          DETECTION_EXCLUSION_V2_FLAG,
          SESSION_IDENTIFICATION_V2_FLAG,
          TPA_MODSEC_PROCESSING_DISABLED,
          TPA_CORAZA_BASED_EVALUATION,
          TPA_CRS_MSG_HIDE_MATCH_VALUE,
          TPA_CUSTOM_RATE_LIMIT_CONFIG_DISABLED,
          RASP_INSPECTION,
          THREAT_SCORING_NOTIFICATION_RULE_MIGRATION_FLAG,
          TRACEABLE_EDGE_DECISION_FLAG);
  private final FeatureFlagServiceBlockingStub featureFlagStub;
  private final LoadingCache<ContextualKey<Void>, Map<String, Boolean>> featureFlagCache;
  private final Duration featureFlagRequestTimeout;

  @Inject
  public FeatureCachingClient(
      FeatureCachingClientConfig config, GrpcChannelRegistry channelRegistry) {
    this.featureFlagStub =
        FeatureFlagServiceGrpc.newBlockingStub(
                channelRegistry.forPlaintextAddress(config.getHost(), config.getPort()))
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    this.featureFlagRequestTimeout = config.getRequestTimeout();
    this.featureFlagCache =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(config.getRefreshDuration())
            .expireAfterAccess(config.getExpirationDuration())
            .build(
                CacheLoader.asyncReloading(
                    CacheLoader.from(this::getFeatureFlagMap),
                    Executors.newFixedThreadPool(
                        config.getThreadPoolSize(), this.buildThreadFactory())));
  }

  public boolean isThreatScoringNotificationRuleMigrationEnabled(RequestContext requestContext) {
    try {
      return requireNonNull(
          this.featureFlagCache
              .get(requestContext.buildInternalContextualKey())
              .get(THREAT_SCORING_NOTIFICATION_RULE_MIGRATION_FLAG));
    } catch (Exception exception) {
      log.error(
          "Failed to retrieve current feature flag value for Threat Scoring Notification Rule Migration",
          exception);
      return DEFAULT_THREAT_SCORING_NOTIFICATION_RULE_MIGRATION_FLAG_VALUE;
    }
  }

  public boolean isDataClassificationEnhancedObfuscationEnabled(RequestContext requestContext) {
    try {
      return requireNonNull(
          this.featureFlagCache
              .get(requestContext.buildInternalContextualKey())
              .get(DATA_CLASSIFICATION_ENHANCED_OBFUSCATION_FLAG));
    } catch (Exception exception) {
      log.error(
          "Failed to retrieve current feature flag value for Data Classification Enhanced Obfuscation",
          exception);
      return DEFAULT_DATA_CLASSIFICATION_ENHANCED_OBFUSCATION_FLAG_VALUE;
    }
  }

  public boolean areDataClassificationFilteredOverridesEnabled(RequestContext requestContext) {
    try {
      return requireNonNull(
          this.featureFlagCache
              .get(requestContext.buildInternalContextualKey())
              .get(DATA_CLASSIFICATION_FILTERED_OVERRIDES_FLAG));
    } catch (Exception exception) {
      log.error(
          "Failed to retrieve current feature flag value for Data Classification Filtered Overrides",
          exception);
      return DEFAULT_DATA_CLASSIFICATION_FILTERED_OVERRIDES_FLAG_VALUE;
    }
  }

  public boolean isIpqsEnabledForRegionToIpMapping(RequestContext requestContext) {
    try {
      return requireNonNull(
          this.featureFlagCache
              .get(requestContext.buildInternalContextualKey())
              .get(IPQS_ENABLED_FLAG));
    } catch (Exception exception) {
      log.error("Failed to retrieve current feature flag value for IPQS", exception);
      return DEFAULT_IPQS_ENABLED_VALUE;
    }
  }

  public boolean isUserAttributionV2Enabled(RequestContext requestContext) {
    try {
      return requireNonNull(
          this.featureFlagCache
              .get(requestContext.buildInternalContextualKey())
              .get(USER_ATTRIBUTION_V2_FLAG));
    } catch (Exception exception) {
      log.warn("Failed to retrieve current feature flag value for User Attribution V2", exception);
      return DEFAULT_USER_ATTRIBUTION_V2_FLAG_VALUE;
    }
  }

  public boolean isUserAttributionV3Enabled(RequestContext requestContext) {
    try {
      return requireNonNull(
          this.featureFlagCache
              .get(requestContext.buildInternalContextualKey())
              .get(USER_ATTRIBUTION_V3_FLAG));
    } catch (Exception exception) {
      log.warn("Failed to retrieve current feature flag value for User Attribution V3", exception);
      return DEFAULT_USER_ATTRIBUTION_V3_FLAG_VALUE;
    }
  }

  public boolean isDetectionExclusionV2EnabledForTenant(RequestContext requestContext) {
    try {
      return requireNonNull(
          this.featureFlagCache
              .get(
                  RequestContext.forTenantId(requestContext.getTenantId().get())
                      .buildInternalContextualKey())
              .get(DETECTION_EXCLUSION_V2_FLAG));
    } catch (Exception exception) {
      log.warn(
          "Failed to retrieve current feature flag value for Detection Exclusion V2", exception);
      return DEFAULT_DETECTION_EXCLUSION_V2_FLAG_VALUE;
    }
  }

  public boolean isTpaModSecProcessingDisabled(RequestContext requestContext) {
    try {
      return requireNonNull(
          this.featureFlagCache
              .get(requestContext.buildInternalContextualKey())
              .get(TPA_MODSEC_PROCESSING_DISABLED));
    } catch (Exception exception) {
      log.warn(
          "Failed to retrieve current feature flag value for TPA ModSec Processing", exception);
      return DEFAULT_TPA_MODSEC_PROCESSING_DISABLED;
    }
  }

  public boolean isTpaCorazaBasedEvaluationEnabled(RequestContext requestContext) {
    try {
      return requireNonNull(
          this.featureFlagCache
              .get(requestContext.buildInternalContextualKey())
              .get(TPA_CORAZA_BASED_EVALUATION));
    } catch (Exception exception) {
      log.warn(
          "Failed to retrieve current feature flag value for TPA ModSec Coraza Processing",
          exception);
      return DEFAULT_TPA_CORAZA_BASED_EVALUATION;
    }
  }

  public boolean isTpaCrsMsgHideMatchValueEnabled(RequestContext requestContext) {
    try {
      return requireNonNull(
          this.featureFlagCache
              .get(requestContext.buildInternalContextualKey())
              .get(TPA_CRS_MSG_HIDE_MATCH_VALUE));
    } catch (Exception exception) {
      log.warn(
          "Failed to retrieve current feature flag value for TPA CRS Message Hide Match Value in modsec rules",
          exception);
      return DEFAULT_TPA_CRS_MSG_HIDE_MATCH_VALUE;
    }
  }

  public boolean isTpaCustomRateLimitConfigDisabled(RequestContext requestContext) {
    try {
      return requireNonNull(
          this.featureFlagCache
              .get(requestContext.buildInternalContextualKey())
              .get(TPA_CUSTOM_RATE_LIMIT_CONFIG_DISABLED));
    } catch (Exception exception) {
      log.warn(
          "Failed to retrieve current feature flag value for TPA custom rate limit config disabled",
          exception);
      return DEFAULT_TPA_CUSTOM_RATE_LIMIT_CONFIG_DISABLED;
    }
  }

  public boolean isRaspInspectionEnabled(RequestContext requestContext) {
    try {
      return requireNonNull(
          this.featureFlagCache
              .get(requestContext.buildInternalContextualKey())
              .get(RASP_INSPECTION));
    } catch (Exception exception) {
      log.warn("Failed to retrieve current feature flag value for RASP Inspection", exception);
      return DEFAULT_RASP_INSPECTION;
    }
  }

  public boolean isSessionIdentificationV2EnabledForTenant(RequestContext requestContext) {
    try {
      return requireNonNull(
          this.featureFlagCache
              .get(requestContext.buildInternalContextualKey())
              .get(SESSION_IDENTIFICATION_V2_FLAG));
    } catch (Exception exception) {
      log.warn(
          "Failed to retrieve current feature flag value for Session Identification V2", exception);
      return DEFAULT_SESSION_IDENTIFICATION_V2_FLAG_VALUE;
    }
  }

  public boolean isEdgeDecisionEnabledForTenant(RequestContext requestContext) {
    try {
      return requireNonNull(
          this.featureFlagCache
              .get(requestContext.buildInternalContextualKey())
              .get(TRACEABLE_EDGE_DECISION_FLAG));
    } catch (Exception exception) {
      log.warn("Failed to retrieve current feature flag value for edge decision flag", exception);
      return DEFAULT_TRACEABLE_EDGE_DECISION_FLAG_VALUE;
    }
  }

  private ThreadFactory buildThreadFactory() {
    return new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("feature-flag-cache-%d")
        .build();
  }

  private Map<String, Boolean> getFeatureFlagMap(ContextualKey<?> key) {
    return key
        .callInContext(
            () ->
                this.featureFlagStub
                    .withDeadlineAfter(
                        this.featureFlagRequestTimeout.toMillis(), TimeUnit.MILLISECONDS)
                    .getCurrentFlagValues(
                        GetCurrentFlagValuesRequest.newBuilder()
                            .addAllFlagKeys(ALL_FLAGS_TO_FETCH)
                            .build()))
        .getValuesMap()
        .entrySet()
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(Entry::getKey, entry -> entry.getValue().getBoolean()));
  }
}
