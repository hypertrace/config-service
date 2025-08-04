package ai.traceable.anomaly.config.service.global.status;

import static ai.traceable.anomaly.config.service.global.AnomalyGlobalConfigServiceConstants.GLOBAL_ANOMALY_CONFIG_NAMESPACE;
import static ai.traceable.anomaly.config.service.global.AnomalyGlobalConfigServiceConstants.GLOBAL_ANOMALY_CONFIG_STATUS_RESOURCE_NAME;

import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils;
import ai.traceable.anomaly.config.service.common.license.LicenseInfoLoader;
import ai.traceable.anomaly.config.service.global.AnomalyGlobalConfigServiceConfig;
import ai.traceable.anomaly.config.service.v1.AnomalyConfidenceLevel;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.RuleType;
import ai.traceable.anomaly.config.service.v1.RuleVersionConfigType;
import ai.traceable.anomaly.config.service.v1.global.ApiGlobalConfig;
import ai.traceable.anomaly.config.service.v1.global.GlobalGenAiConfig;
import ai.traceable.anomaly.config.service.v1.global.GlobalModsecConfig;
import ai.traceable.anomaly.config.service.v1.global.ModsecDefaultConfigsType;
import ai.traceable.anomaly.config.service.v1.global.ModsecGlobalConfig;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatusChange;
import ai.traceable.license.metering.service.api.v1.LicenseInfo;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Slf4j
public class GlobalAnomalyConfigStatusManagerImpl
    extends IdentifiedObjectStore<ScopedAnomalyConfigStatusChange>
    implements GlobalAnomalyConfigStatusManager {
  private static final String SCOPED_ANOMALY_GLOBAL_CONFIG_ACTION_TIMER =
      "scoped.anomaly.global.config.action.timer";
  private static final String TENANT_ID_TAG = "tenantId";
  private static final String ACTION_TAG = "action";
  private static final String SCOPE_TAG = "scope";
  private static final String ANOMALY_DETECTION_TYPE_TAG = "anomalyDetectionType";
  private static final Map<Tags, Timer> TIMER_MAP = new ConcurrentHashMap<>();
  private static final String DISABLED = "Disabled";
  private static final String ENABLED = "Enabled";
  private static final String ANOMALY_DETECTION_TYPE_WAF = "Waf";
  private static final String ANOMALY_DETECTION_TYPE_API_PROTECTION = "Api Protection";

  private final AnomalyGlobalConfigServiceConfig config;
  private final ScopedGlobalConfigStatusChangeConverter configConverter;
  private final AnomalyConfigScopeUtils anomalyConfigScopeUtils;
  private final LicenseInfoLoader licenseInfoLoader;

  @Inject
  public GlobalAnomalyConfigStatusManagerImpl(
      AnomalyGlobalConfigServiceConfig config,
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ScopedGlobalConfigStatusChangeConverter configConverter,
      AnomalyConfigScopeUtils anomalyConfigScopeUtils,
      LicenseInfoLoader licenseInfoLoader,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        GLOBAL_ANOMALY_CONFIG_NAMESPACE,
        GLOBAL_ANOMALY_CONFIG_STATUS_RESOURCE_NAME,
        configChangeEventGenerator);
    this.config = config;
    this.configConverter = configConverter;
    this.anomalyConfigScopeUtils = anomalyConfigScopeUtils;
    this.licenseInfoLoader = licenseInfoLoader;
  }

  @Override
  protected Optional<ScopedAnomalyConfigStatusChange> buildDataFromValue(Value value) {
    try {
      return Optional.of(configConverter.convert(value));
    } catch (InvalidProtocolBufferException exception) {
      log.error("Unable to convert config to ScopedAnomalyConfigStatusChange for value: {}", value);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(ScopedAnomalyConfigStatusChange data) {
    return configConverter.convert(data);
  }

  @Override
  protected String getContextFromData(ScopedAnomalyConfigStatusChange data) {
    return anomalyConfigScopeUtils.getContextFromAnomalyConfigScope(
        getTenantId(RequestContext.CURRENT.get()), data.getConfigScope());
  }

  @Override
  public List<ScopedAnomalyConfigStatus> getAllScopedAnomalyConfigStatusConfigs(
      RequestContext requestContext, List<AnomalyConfigScope> applicableScopesList) {
    Map<String, ScopedAnomalyConfigStatusChange> configMap =
        getFilteredConfigMap(requestContext, applicableScopesList);
    List<ScopedAnomalyConfigStatus> resolvedConfigs =
        configMap.values().stream()
            .map(
                scopedAnomalyConfigStatusChange ->
                    getResolvedConfig(
                        requestContext,
                        configMap,
                        scopedAnomalyConfigStatusChange.getConfigScope(),
                        anomalyConfigScopeUtils.getContextsWithIncreasingPriority(
                            getTenantId(requestContext),
                            scopedAnomalyConfigStatusChange.getConfigScope())))
            .collect(Collectors.toList());
    if (!configMap.containsKey(getTenantId(requestContext))) {
      AnomalyConfigStatus configStatus = getDefaultTierConfig(requestContext);
      resolvedConfigs.add(
          ScopedAnomalyConfigStatus.newBuilder()
              .setConfigScope(anomalyConfigScopeUtils.getDefaultCustomerConfigScope())
              .setConfigStatus(configStatus)
              .setMinConfidenceLevel(config.getMinConfidenceLevel())
              .setApiGlobalConfig(
                  ApiGlobalConfig.newBuilder()
                      .setDisabled(configStatus.getDisabled())
                      .setDefaultConfigsType(config.getApiDefaultConfigsType())
                      .build())
              .setModsecGlobalConfig(
                  ModsecGlobalConfig.newBuilder()
                      .setDisabled(configStatus.getDisabled())
                      .setMinConfidenceLevel(config.getMinConfidenceLevel())
                      .setDefaultConfigsType(config.getModsecDefaultConfigsType())
                      .build())
              .setGlobalModsecConfig(
                  GlobalModsecConfig.newBuilder()
                      .setDisabled(configStatus.getDisabled())
                      .setMinConfidenceLevel(config.getMinConfidenceLevel())
                      .setDefaultConfigsType(config.getModsecDefaultConfigsType())
                      .build())
              .setGlobalGenAiConfig(
                  GlobalGenAiConfig.newBuilder().setDisabled(config.isGenAiDisabled()))
              .build());
    }
    return Collections.unmodifiableList(resolvedConfigs);
  }

  @Override
  public List<ScopedAnomalyConfigStatusChange> getAllUnresolvedScopedAnomalyConfigStatusConfigs(
      RequestContext requestContext, List<AnomalyConfigScope> applicableScopesList) {
    Map<String, ScopedAnomalyConfigStatusChange> configMap =
        getFilteredConfigMap(requestContext, applicableScopesList);
    return configMap.values().stream()
        .map(this::migrateScopedAnomalyConfigStatusChange)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public ScopedAnomalyConfigStatus getScopedAnomalyConfigStatus(
      RequestContext requestContext, AnomalyConfigScope configScope) {
    return getResolvedConfig(
        requestContext,
        getFilteredConfigMap(requestContext, Collections.emptyList()),
        configScope,
        anomalyConfigScopeUtils.getContextsWithIncreasingPriority(
            getTenantId(requestContext), configScope));
  }

  @Override
  public ScopedAnomalyConfigStatusChange getUnresolvedScopedAnomalyConfigStatus(
      RequestContext requestContext, AnomalyConfigScope configScope) {
    Optional<ScopedAnomalyConfigStatusChange> scopedAnomalyConfigStatusChangeOptional =
        getData(
                requestContext,
                anomalyConfigScopeUtils.getContextFromAnomalyConfigScope(
                    getTenantId(requestContext), configScope))
            .map(this::migrateScopedAnomalyConfigStatusChange);
    return scopedAnomalyConfigStatusChangeOptional.orElse(
        ScopedAnomalyConfigStatusChange.newBuilder().setConfigScope(configScope).build());
  }

  @Override
  public ScopedAnomalyConfigStatusChange updateScopedAnomalyConfigStatus(
      RequestContext requestContext, ScopedAnomalyConfigStatusChange scopedConfigStatusChange) {
    String anomalyDetectionType = null;
    String action = null;
    if (scopedConfigStatusChange.getGlobalModsecConfigChange().hasDisabled()) {
      action =
          scopedConfigStatusChange.getGlobalModsecConfigChange().getDisabled() ? DISABLED : ENABLED;
      anomalyDetectionType = ANOMALY_DETECTION_TYPE_WAF;
    } else if (scopedConfigStatusChange.getGlobalApiConfigChange().hasDisabled()) {
      action =
          scopedConfigStatusChange.getGlobalApiConfigChange().getDisabled() ? DISABLED : ENABLED;
      anomalyDetectionType = ANOMALY_DETECTION_TYPE_API_PROTECTION;
    }

    ScopedAnomalyConfigStatusChange scopedAnomalyConfigStatusChange =
        getData(requestContext, getContextFromData(scopedConfigStatusChange))
            .map(
                existing -> {
                  ScopedAnomalyConfigStatusChange merged =
                      configConverter.merge(scopedConfigStatusChange, existing);
                  return GlobalAnomalyConfigStatusUtils.handleMergedConfigChange(
                      merged, scopedConfigStatusChange);
                })
            .orElseGet(
                () ->
                    GlobalAnomalyConfigStatusUtils.handleMergedConfigChange(
                        scopedConfigStatusChange, scopedConfigStatusChange));
    if (action != null) {
      String tenantId = requestContext.getTenantId().orElseThrow();
      return getTimer(
              tenantId,
              action,
              anomalyDetectionType,
              getScopeString(scopedConfigStatusChange.getConfigScope(), tenantId))
          .record(() -> upsertObject(requestContext, scopedAnomalyConfigStatusChange).getData());
    }
    return upsertObject(requestContext, scopedAnomalyConfigStatusChange).getData();
  }

  @Override
  public void deleteScopedAnomalyGlobalConfigStatus(
      RequestContext requestContext, AnomalyConfigScope scope) {
    deleteObject(
        requestContext,
        anomalyConfigScopeUtils.getContextFromAnomalyConfigScope(
            getTenantId(RequestContext.CURRENT.get()), scope));
  }

  @Override
  public ScopedAnomalyConfigStatusChange deleteRuleVersionConfigType(
      RequestContext requestContext,
      AnomalyConfigScope scope,
      List<RuleVersionConfigType> ruleVersionConfigTypes,
      RuleType ruleType) {
    String context =
        anomalyConfigScopeUtils.getContextFromAnomalyConfigScope(
            getTenantId(RequestContext.CURRENT.get()), scope);
    Optional<ScopedAnomalyConfigStatusChange> existingConfig = getData(requestContext, context);

    if (existingConfig.isEmpty()) {
      return ScopedAnomalyConfigStatusChange.getDefaultInstance();
    }

    ScopedAnomalyConfigStatusChange.Builder updatedConfig = existingConfig.get().toBuilder();
    boolean needsUpdate = false;

    for (RuleVersionConfigType configType : ruleVersionConfigTypes) {
      switch (configType) {
        case RULE_VERSION_CONFIG_TYPE_STABLE:
          needsUpdate |= clearStableVersion(updatedConfig, ruleType);
          break;
        case RULE_VERSION_CONFIG_TYPE_OVERRIDE:
          needsUpdate |= clearOverrideVersion(updatedConfig, ruleType);
          break;
        case RULE_VERSION_CONFIG_TYPE_EXPERIMENTAL:
          needsUpdate |= clearExperimentalVersion(updatedConfig, ruleType);
          break;
        default:
          throw new IllegalArgumentException("Unsupported config type: " + configType.name());
      }
    }

    return needsUpdate
        ? upsertObject(requestContext, updatedConfig.build()).getData()
        : existingConfig.get();
  }

  private ScopedAnomalyConfigStatus getResolvedConfig(
      RequestContext requestContext,
      Map<String, ScopedAnomalyConfigStatusChange> configMap,
      AnomalyConfigScope configScope,
      List<String> contextsWithIncreasingPriority) {
    AnomalyConfigStatusChange configStatusChange = AnomalyConfigStatusChange.getDefaultInstance();
    ScopedAnomalyConfigStatusChange.Builder scopedAnomalyConfigBuilder =
        ScopedAnomalyConfigStatusChange.newBuilder();
    for (String context : contextsWithIncreasingPriority) {
      if (configMap.containsKey(context)) {
        configStatusChange =
            configConverter.merge(configMap.get(context).getConfigStatus(), configStatusChange);
        scopedAnomalyConfigBuilder = scopedAnomalyConfigBuilder.mergeFrom(configMap.get(context));
      }
    }
    scopedAnomalyConfigBuilder.setConfigScope(configScope);
    ScopedAnomalyConfigStatus.Builder builder =
        configConverter
            .convertScopedConfig(
                migrateScopedAnomalyConfigStatusChange(scopedAnomalyConfigBuilder.build()),
                config,
                configConverter.merge(configStatusChange, getDefaultTierConfig(requestContext)))
            .toBuilder();
    // default profile should not fallback to tenant level for an env, it should always use env
    // level value if present or the default value
    if (configScope.hasEnvironmentScope()
        && Optional.ofNullable(configMap.get(configScope.getEnvironmentScope().getEnvironmentId()))
            .map(
                configStatus ->
                    configStatus
                        .getGlobalModsecConfigChange()
                        .getDefaultConfigsType()
                        .equals(ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_UNSPECIFIED))
            .orElse(true)) {
      builder
          .getModsecGlobalConfigBuilder()
          .setDefaultConfigsType(config.getEnvScopeModsecDefaultConfigsType());
      builder
          .getGlobalModsecConfigBuilder()
          .setDefaultConfigsType(config.getEnvScopeModsecDefaultConfigsType());
    }
    return builder.build();
  }

  private ScopedAnomalyConfigStatusChange migrateScopedAnomalyConfigStatusChange(
      ScopedAnomalyConfigStatusChange scopedAnomalyConfigStatusChange) {
    ScopedAnomalyConfigStatusChange.Builder builder =
        ScopedAnomalyConfigStatusChange.newBuilder(scopedAnomalyConfigStatusChange);
    if (scopedAnomalyConfigStatusChange.getConfigStatus().hasDisabled()) {
      if (!scopedAnomalyConfigStatusChange.getModsecGlobalConfig().hasDisabled()) {
        builder
            .getModsecGlobalConfigBuilder()
            .setDisabled(scopedAnomalyConfigStatusChange.getConfigStatus().getDisabled());
      }
      if (!scopedAnomalyConfigStatusChange.getApiGlobalConfig().hasDisabled()) {
        builder
            .getApiGlobalConfigBuilder()
            .setDisabled(scopedAnomalyConfigStatusChange.getConfigStatus().getDisabled());
      }
    }

    if (scopedAnomalyConfigStatusChange.hasEnabledForExitSpans()) {
      if (!scopedAnomalyConfigStatusChange.getModsecGlobalConfig().hasEnabledForExitSpans()) {
        builder
            .getModsecGlobalConfigBuilder()
            .setEnabledForExitSpans(scopedAnomalyConfigStatusChange.getEnabledForExitSpans());
      }
      if (!scopedAnomalyConfigStatusChange.getApiGlobalConfig().hasEnabledForExitSpans()) {
        builder
            .getApiGlobalConfigBuilder()
            .setEnabledForExitSpans(scopedAnomalyConfigStatusChange.getEnabledForExitSpans());
      }
    }

    if (scopedAnomalyConfigStatusChange.hasMinConfidenceLevel()) {
      if (!scopedAnomalyConfigStatusChange
          .getModsecGlobalConfig()
          .getMinConfidenceLevel()
          .equals(AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_UNSPECIFIED)) {
        builder
            .getModsecGlobalConfigBuilder()
            .setMinConfidenceLevel(scopedAnomalyConfigStatusChange.getMinConfidenceLevel());
      }
    }
    return builder.build();
  }

  private AnomalyConfigStatus getDefaultTierConfig(RequestContext requestContext) {
    try {
      return config.getConfigStatus(licenseInfoLoader.getLicenseTier(requestContext));
    } catch (ExecutionException e) {
      log.warn("Unable to retrieve license tier for tenant:{}", getTenantId(requestContext), e);
      return config.getConfigStatus(LicenseInfo.Tier.TIER_UNSPECIFIED);
    }
  }

  private final String getTenantId(RequestContext requestContext) {
    return requestContext
        .getTenantId()
        .orElseThrow(
            () -> new IllegalArgumentException("Unable to get tenant id from request context"));
  }

  private Map<String, ScopedAnomalyConfigStatusChange> getFilteredConfigMap(
      RequestContext requestContext, List<AnomalyConfigScope> applicableScopesList) {
    Map<String, ScopedAnomalyConfigStatusChange> scopedAnomalyConfigStatusChangeMap =
        getAllObjects(requestContext).stream()
            .collect(
                Collectors.toMap(
                    ContextualConfigObject::getContext,
                    ConfigObject::getData,
                    (previous, current) ->
                        previous // sorted by latest in getAllObjects so keep the previous entry
                    ));
    return anomalyConfigScopeUtils.filterConfigMap(
        scopedAnomalyConfigStatusChangeMap,
        applicableScopesList,
        ScopedAnomalyConfigStatusChange::getConfigScope);
  }

  private Timer getTimer(
      String tenantId, String action, String anomalyDetectionType, String scope) {
    Tags metricTags =
        Tags.of(
            TENANT_ID_TAG,
            tenantId,
            ACTION_TAG,
            action,
            ANOMALY_DETECTION_TYPE_TAG,
            anomalyDetectionType,
            SCOPE_TAG,
            scope);

    return TIMER_MAP.computeIfAbsent(
        metricTags,
        id ->
            PlatformMetricsRegistry.registerTimer(
                SCOPED_ANOMALY_GLOBAL_CONFIG_ACTION_TIMER,
                metricTags.stream().collect(Collectors.toMap(Tag::getKey, Tag::getValue))));
  }

  private String getScopeString(AnomalyConfigScope configScope, String tenantId) {
    switch (configScope.getScopeCase()) {
      case CUSTOMER_SCOPE:
        return "Customer ID: " + tenantId;
      case ENVIRONMENT_SCOPE:
        return "Environment ID: " + configScope.getEnvironmentScope().getEnvironmentId();
      case SERVICE_SCOPE:
        return "Service ID: " + configScope.getServiceScope().getId();
      case API_SCOPE:
        return "Api ID: " + configScope.getApiScope().getId();
      case BACKEND_SCOPE:
        return "Backend Scope ID: " + configScope.getBackendScope().getId();
      case BACKEND_API_SCOPE:
        return "Backend Api Scope ID:" + configScope.getBackendApiScope().getId();
      default:
        throw new RuntimeException(
            String.format("Invalid scope found: {%s}", configScope.getScopeCase()));
    }
  }

  private boolean clearStableVersion(
      ScopedAnomalyConfigStatusChange.Builder config, RuleType ruleType) {
    if (ruleType == RuleType.RULE_TYPE_WEB_APPLICATION
        && config.getGlobalModsecConfigChange().getRuleVersionDataChange().hasStableVersion()) {
      config
          .getGlobalModsecConfigChangeBuilder()
          .getRuleVersionDataChangeBuilder()
          .clearStableVersion();
      return true;
    } else if (ruleType == RuleType.RULE_TYPE_API_PROTECTION
        && config.getGlobalApiConfigChange().getRuleVersionDataChange().hasStableVersion()) {
      config
          .getGlobalApiConfigChangeBuilder()
          .getRuleVersionDataChangeBuilder()
          .clearStableVersion();
      return true;
    }
    return false;
  }

  private boolean clearOverrideVersion(
      ScopedAnomalyConfigStatusChange.Builder config, RuleType ruleType) {
    if (ruleType == RuleType.RULE_TYPE_WEB_APPLICATION
        && config.getGlobalModsecConfigChange().getRuleVersionDataChange().hasOverrideVersion()) {
      config
          .getGlobalModsecConfigChangeBuilder()
          .getRuleVersionDataChangeBuilder()
          .clearOverrideVersion();
      return true;
    } else if (ruleType == RuleType.RULE_TYPE_API_PROTECTION
        && config.getGlobalApiConfigChange().getRuleVersionDataChange().hasOverrideVersion()) {
      config
          .getGlobalApiConfigChangeBuilder()
          .getRuleVersionDataChangeBuilder()
          .clearOverrideVersion();
      return true;
    }
    return false;
  }

  private boolean clearExperimentalVersion(
      ScopedAnomalyConfigStatusChange.Builder config, RuleType ruleType) {
    if (ruleType == RuleType.RULE_TYPE_WEB_APPLICATION
        && config
            .getGlobalModsecConfigChange()
            .getRuleVersionDataChange()
            .hasExperimentalVersion()) {
      config
          .getGlobalModsecConfigChangeBuilder()
          .getRuleVersionDataChangeBuilder()
          .clearExperimentalVersion();
      return true;
    }
    return false;
  }
}
