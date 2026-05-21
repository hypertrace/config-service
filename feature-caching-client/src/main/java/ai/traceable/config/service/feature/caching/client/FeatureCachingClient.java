package ai.traceable.config.service.feature.caching.client;

import static java.util.Objects.requireNonNull;

import ai.traceable.featureflag.v1.FeatureFlagServiceGrpc;
import ai.traceable.featureflag.v1.FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub;
import ai.traceable.featureflag.v1.FeatureFlagValue;
import ai.traceable.featureflag.v1.GetCurrentFlagValuesRequest;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private static final boolean DEFAULT_TPA_CUSTOM_RATE_LIMIT_CONFIG = false;
  private static final boolean DEFAULT_RASP_INSPECTION = false;
  private static final boolean DEFAULT_THREAT_SCORING_NOTIFICATION_RULE_MIGRATION_FLAG_VALUE =
      false;
  private static final boolean DEFAULT_TRACEABLE_EDGE_DECISION_FLAG_VALUE = false;
  private static final boolean DEFAULT_WAAP_VERSIONING_FLAG_VALUE = false;
  private static final boolean DEFAULT_GENAI_DETECTION_V2_VALUE = false;
  private static final boolean DEFAULT_PROTECTION_ENGINE_WEBAPP_PROTECTION_FLAG_VALUE = false;
  private static final boolean DEFAULT_PROTECTION_ENGINE_API_PROTECTION_FLAG_VALUE = false;
  private static final boolean DEFAULT_PROTECTION_ENGINE_POST_DETECTION_FILTERING_FLAG_VALUE =
      false;
  private static final boolean DEFAULT_PROTECTION_ENGINE_CUSTOM_SIGNATURE_FLAG_VALUE = false;
  private static final Set<String> DEFAULT_HIDDEN_DEFENSE_AI_FEATURES_VALUE =
      Collections.emptySet();
  private static final boolean DEFAULT_PROTECTION_BLOCKING_DUAL_EVALUATION_FLAG_VALUE = false;
  private static final boolean DEFAULT_PROTECTION_ENGINE_AI_APP_PROTECTION_FLAG_VALUE = false;
  private static final boolean DEFAULT_GENAI_ML_BASED_AI_CLASSIFICATION_ENABLED_VALUE = false;

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
  private static final String TPA_CUSTOM_RATE_LIMIT_CONFIG = "tpa.custom-rate-limit-config";
  private static final String RASP_INSPECTION = "enricher.rasp-inspection";
  private static final String THREAT_SCORING_NOTIFICATION_RULE_MIGRATION_FLAG =
      "notifications.threat-scoring-configuration";
  private static final String TRACEABLE_EDGE_DECISION_FLAG = "traceable-edge.edge-decision";
  private static final String CONFIG_SERVICE_WAAP_RULES_VERSIONING =
      "config-service.waap-rules-versioning";
  private static final String GENAI_DETECTION_V2_FLAG = "enricher.genai-detection-v2";
  private static final String PROTECTION_ENGINE_WEBAPP_PROTECTION_FLAG =
      "protection-engine.webapp-protection";
  private static final String PROTECTION_ENGINE_API_PROTECTION_FLAG =
      "protection-engine.api-protection";
  private static final String PROTECTION_ENGINE_POST_DETECTION_FILTERING_FLAG =
      "protection-engine.post-detection-filtering";
  private static final String PROTECTION_ENGINE_CUSTOM_SIGNATURE_FLAG =
      "protection-engine.custom-signature-rules";
  private static final String HIDDEN_DEFENSE_AI_FEATURES =
      "graphql.security-settings.defense-ai.hidden";
  private static final String API_PROTECT_CONFIG_POLICIES_REVAMP_FLAG =
      "api-protect.policies.revamp";
  private static final String API_PROTECT_CONFIG_POLICIES_MIGRATION_FLAG =
      "api-protect.policies.migration";
  private static final String PROTECTION_BLOCKING_DUAL_EVALUATION_FLAG =
      "protection.blocking.dual-evaluation";
  private static final String PROTECTION_ENGINE_AI_APP_PROTECTION_FLAG =
      "protection-engine.ai-app-protection";
  private static final String BLOCKING_AVAILABLE_DEFENSE_AI_FEATURES_FLAG =
      "graphql.security-settings.defense-ai.blocking-available";
  private static final String GENAI_ML_BASED_AI_CLASSIFICATION_ENABLED =
      "genai.ml-based-ai-classification";

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
          TPA_CUSTOM_RATE_LIMIT_CONFIG,
          RASP_INSPECTION,
          THREAT_SCORING_NOTIFICATION_RULE_MIGRATION_FLAG,
          TRACEABLE_EDGE_DECISION_FLAG,
          CONFIG_SERVICE_WAAP_RULES_VERSIONING,
          GENAI_DETECTION_V2_FLAG,
          API_PROTECT_CONFIG_POLICIES_REVAMP_FLAG,
          API_PROTECT_CONFIG_POLICIES_MIGRATION_FLAG,
          PROTECTION_ENGINE_WEBAPP_PROTECTION_FLAG,
          PROTECTION_ENGINE_API_PROTECTION_FLAG,
          PROTECTION_ENGINE_POST_DETECTION_FILTERING_FLAG,
          PROTECTION_ENGINE_CUSTOM_SIGNATURE_FLAG,
          HIDDEN_DEFENSE_AI_FEATURES,
          PROTECTION_BLOCKING_DUAL_EVALUATION_FLAG,
          PROTECTION_ENGINE_AI_APP_PROTECTION_FLAG,
          BLOCKING_AVAILABLE_DEFENSE_AI_FEATURES_FLAG,
          GENAI_ML_BASED_AI_CLASSIFICATION_ENABLED);
  private final FeatureFlagServiceBlockingStub featureFlagStub;
  private final LoadingCache<ContextualKey<Void>, Map<String, FeatureFlagValue>> featureFlagCache;
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

  public Set<String> getHiddenDefenseAiFeatures(RequestContext requestContext) {
    try {
      return requireNonNull(
              this.featureFlagCache
                  .get(requestContext.buildInternalContextualKey())
                  .get(HIDDEN_DEFENSE_AI_FEATURES))
          .getList()
          .getValuesList()
          .stream()
          .map(FeatureFlagValue::getString)
          .collect(Collectors.toSet());
    } catch (Exception exception) {
      log.error(
          "Failed to retrieve current feature flag value for Hidden defense AI features",
          exception);
      return DEFAULT_HIDDEN_DEFENSE_AI_FEATURES_VALUE;
    }
  }

  public boolean isGenAiDetectionV2Enabled(RequestContext requestContext) {
    try {
      return requireNonNull(
              this.featureFlagCache
                  .get(requestContext.buildInternalContextualKey())
                  .get(GENAI_DETECTION_V2_FLAG))
          .getBoolean();
    } catch (Exception exception) {
      log.error("Failed to retrieve current feature flag value for Genai Detection V2", exception);
      return DEFAULT_GENAI_DETECTION_V2_VALUE;
    }
  }

  public boolean isApiProtectConfigPoliciesRevampEnabled(RequestContext requestContext) {
    try {
      return requireNonNull(
              this.featureFlagCache
                  .get(requestContext.buildInternalContextualKey())
                  .get(API_PROTECT_CONFIG_POLICIES_REVAMP_FLAG))
          .getBoolean();
    } catch (Exception exception) {
      log.error(
          "Failed to retrieve current feature flag value for API Protect Config Policies Revamp",
          exception);
      return false;
    }
  }

  public boolean isApiProtectConfigPoliciesMigrationEnabled(RequestContext requestContext) {
    try {
      return requireNonNull(
              this.featureFlagCache
                  .get(requestContext.buildInternalContextualKey())
                  .get(API_PROTECT_CONFIG_POLICIES_MIGRATION_FLAG))
          .getBoolean();
    } catch (Exception exception) {
      log.error(
          "Failed to retrieve current feature flag value for API Protect Config Policies Migration",
          exception);
      return false;
    }
  }

  public Set<String> getBlockingAvailableDefenseAiFeatures(RequestContext requestContext) {
    try {
      return requireNonNull(
              this.featureFlagCache
                  .get(requestContext.buildInternalContextualKey())
                  .get(BLOCKING_AVAILABLE_DEFENSE_AI_FEATURES_FLAG))
          .getList()
          .getValuesList()
          .stream()
          .map(FeatureFlagValue::getString)
          .collect(Collectors.toUnmodifiableSet());
    } catch (Exception exception) {
      log.error(
          "Failed to retrieve current feature flag value for Blocking Available Defense AI Features",
          exception);
      return Collections.emptySet();
    }
  }

  public boolean isThreatScoringNotificationRuleMigrationEnabled(RequestContext requestContext) {
    try {
      return requireNonNull(
              this.featureFlagCache
                  .get(requestContext.buildInternalContextualKey())
                  .get(THREAT_SCORING_NOTIFICATION_RULE_MIGRATION_FLAG))
          .getBoolean();
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
                  .get(DATA_CLASSIFICATION_ENHANCED_OBFUSCATION_FLAG))
          .getBoolean();
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
                  .get(DATA_CLASSIFICATION_FILTERED_OVERRIDES_FLAG))
          .getBoolean();
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
                  .get(IPQS_ENABLED_FLAG))
          .getBoolean();
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
                  .get(USER_ATTRIBUTION_V2_FLAG))
          .getBoolean();
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
                  .get(USER_ATTRIBUTION_V3_FLAG))
          .getBoolean();
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
                  .get(DETECTION_EXCLUSION_V2_FLAG))
          .getBoolean();
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
                  .get(TPA_MODSEC_PROCESSING_DISABLED))
          .getBoolean();
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
                  .get(TPA_CORAZA_BASED_EVALUATION))
          .getBoolean();
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
                  .get(TPA_CRS_MSG_HIDE_MATCH_VALUE))
          .getBoolean();
    } catch (Exception exception) {
      log.warn(
          "Failed to retrieve current feature flag value for TPA CRS Message Hide Match Value in modsec rules",
          exception);
      return DEFAULT_TPA_CRS_MSG_HIDE_MATCH_VALUE;
    }
  }

  public boolean isTpaCustomRateLimitConfigEnabled(RequestContext requestContext) {
    try {
      return requireNonNull(
              this.featureFlagCache
                  .get(requestContext.buildInternalContextualKey())
                  .get(TPA_CUSTOM_RATE_LIMIT_CONFIG))
          .getBoolean();
    } catch (Exception exception) {
      log.warn(
          "Failed to retrieve current feature flag value for TPA custom rate limit config",
          exception);
      return DEFAULT_TPA_CUSTOM_RATE_LIMIT_CONFIG;
    }
  }

  public boolean isRaspInspectionEnabled(RequestContext requestContext) {
    try {
      return requireNonNull(
              this.featureFlagCache
                  .get(requestContext.buildInternalContextualKey())
                  .get(RASP_INSPECTION))
          .getBoolean();
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
                  .get(SESSION_IDENTIFICATION_V2_FLAG))
          .getBoolean();
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
                  .get(TRACEABLE_EDGE_DECISION_FLAG))
          .getBoolean();
    } catch (Exception exception) {
      log.warn("Failed to retrieve current feature flag value for edge decision flag", exception);
      return DEFAULT_TRACEABLE_EDGE_DECISION_FLAG_VALUE;
    }
  }

  public boolean isWAAPVersioningEnabledForTenant(RequestContext requestContext) {
    try {
      return requireNonNull(
              this.featureFlagCache
                  .get(requestContext.buildInternalContextualKey())
                  .get(CONFIG_SERVICE_WAAP_RULES_VERSIONING))
          .getBoolean();
    } catch (Exception exception) {
      log.warn("Failed to retrieve current feature flag value for WAAP versioning", exception);
      return DEFAULT_WAAP_VERSIONING_FLAG_VALUE;
    }
  }

  public boolean isProtectionEngineWebAppProtectionEnabledForTenant(RequestContext requestContext) {
    try {
      return requireNonNull(
              this.featureFlagCache
                  .get(requestContext.buildInternalContextualKey())
                  .get(PROTECTION_ENGINE_WEBAPP_PROTECTION_FLAG))
          .getBoolean();
    } catch (Exception exception) {
      log.warn(
          "Failed to retrieve current feature flag value for Protection Engine Web App Protection",
          exception);
      return DEFAULT_PROTECTION_ENGINE_WEBAPP_PROTECTION_FLAG_VALUE;
    }
  }

  public boolean isProtectionBlockingDualEvaluationEnabledForTenant(RequestContext requestContext) {
    try {
      return requireNonNull(
              this.featureFlagCache
                  .get(requestContext.buildInternalContextualKey())
                  .get(PROTECTION_BLOCKING_DUAL_EVALUATION_FLAG))
          .getBoolean();
    } catch (Exception exception) {
      log.warn(
          "Failed to retrieve current feature flag value for Protection Blocking Dual Evaluation",
          exception);
      return DEFAULT_PROTECTION_BLOCKING_DUAL_EVALUATION_FLAG_VALUE;
    }
  }

  public boolean isGenAiMlBasedAiClassificationEnabled(RequestContext requestContext) {
    try {
      return requireNonNull(
              this.featureFlagCache
                  .get(requestContext.buildInternalContextualKey())
                  .get(GENAI_ML_BASED_AI_CLASSIFICATION_ENABLED))
          .getBoolean();
    } catch (Exception exception) {
      log.warn(
          "Failed to retrieve current feature flag value for GenAI ML based AI classification",
          exception);
      return DEFAULT_GENAI_ML_BASED_AI_CLASSIFICATION_ENABLED_VALUE;
    }
  }

  public boolean isProtectionEngineApiProtectEnabledForTenant(RequestContext requestContext) {
    try {
      return requireNonNull(
              this.featureFlagCache
                  .get(requestContext.buildInternalContextualKey())
                  .get(PROTECTION_ENGINE_API_PROTECTION_FLAG))
          .getBoolean();
    } catch (Exception exception) {
      log.warn(
          "Failed to retrieve current feature flag value for Protection Engine API Protection",
          exception);
      return DEFAULT_PROTECTION_ENGINE_API_PROTECTION_FLAG_VALUE;
    }
  }

  public boolean isProtectionEnginePostDetectionFilteringEnabledForTenant(
      RequestContext requestContext) {
    try {
      return requireNonNull(
              this.featureFlagCache
                  .get(requestContext.buildInternalContextualKey())
                  .get(PROTECTION_ENGINE_POST_DETECTION_FILTERING_FLAG))
          .getBoolean();
    } catch (Exception exception) {
      log.warn(
          "Failed to retrieve current feature flag value for Protection Engine Post Detection Filtering",
          exception);
      return DEFAULT_PROTECTION_ENGINE_POST_DETECTION_FILTERING_FLAG_VALUE;
    }
  }

  public boolean isProtectionEngineCustomSignatureEnabledForTenant(RequestContext requestContext) {
    try {
      return requireNonNull(
              this.featureFlagCache
                  .get(requestContext.buildInternalContextualKey())
                  .get(PROTECTION_ENGINE_CUSTOM_SIGNATURE_FLAG))
          .getBoolean();
    } catch (Exception exception) {
      log.warn(
          "Failed to retrieve current feature flag value for Protection Engine Custom Signature",
          exception);
      return DEFAULT_PROTECTION_ENGINE_CUSTOM_SIGNATURE_FLAG_VALUE;
    }
  }

  public boolean isProtectionEngineAiAppProtectionEnabledForTenant(RequestContext requestContext) {
    try {
      return requireNonNull(
              this.featureFlagCache
                  .get(requestContext.buildInternalContextualKey())
                  .get(PROTECTION_ENGINE_AI_APP_PROTECTION_FLAG))
          .getBoolean();
    } catch (Exception exception) {
      log.warn(
          "Failed to retrieve current feature flag value for Protection Engine AI App Protection",
          exception);
      return DEFAULT_PROTECTION_ENGINE_AI_APP_PROTECTION_FLAG_VALUE;
    }
  }

  private ThreadFactory buildThreadFactory() {
    return new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("feature-flag-cache-%d")
        .build();
  }

  private Map<String, FeatureFlagValue> getFeatureFlagMap(ContextualKey<?> key) {
    return key.callInContext(
            () ->
                this.featureFlagStub
                    .withDeadlineAfter(
                        this.featureFlagRequestTimeout.toMillis(), TimeUnit.MILLISECONDS)
                    .getCurrentFlagValues(
                        GetCurrentFlagValuesRequest.newBuilder()
                            .addAllFlagKeys(ALL_FLAGS_TO_FETCH)
                            .build()))
        .getValuesMap();
  }
}
