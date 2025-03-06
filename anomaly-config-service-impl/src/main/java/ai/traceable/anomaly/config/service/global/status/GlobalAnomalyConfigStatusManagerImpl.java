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
import ai.traceable.anomaly.config.service.v1.global.ApiGlobalConfig;
import ai.traceable.anomaly.config.service.v1.global.ModsecGlobalConfig;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatusChange;
import ai.traceable.license.metering.service.api.v1.LicenseInfo;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

@Slf4j
public class GlobalAnomalyConfigStatusManagerImpl
    extends IdentifiedObjectStore<ScopedAnomalyConfigStatusChange>
    implements GlobalAnomalyConfigStatusManager {

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
      RequestContext requestContext) {
    Map<String, ScopedAnomalyConfigStatusChange> configMap = fetchConfigMap(requestContext);
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
              .build());
    }
    return Collections.unmodifiableList(resolvedConfigs);
  }

  @Override
  public List<ScopedAnomalyConfigStatusChange> getAllUnresolvedScopedAnomalyConfigStatusConfigs(
      RequestContext requestContext) {
    Map<String, ScopedAnomalyConfigStatusChange> configMap = fetchConfigMap(requestContext);
    return configMap.values().stream()
        .map(this::migrateScopedAnomalyConfigStatusChange)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public ScopedAnomalyConfigStatus getScopedAnomalyConfigStatus(
      RequestContext requestContext, AnomalyConfigScope configScope) {
    return getResolvedConfig(
        requestContext,
        fetchConfigMap(requestContext),
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
    return upsertObject(
            requestContext,
            getData(requestContext, getContextFromData(scopedConfigStatusChange))
                .map(
                    existingScopedConfigStatusChange ->
                        configConverter.merge(
                            scopedConfigStatusChange, existingScopedConfigStatusChange))
                .orElse(scopedConfigStatusChange))
        .getData();
  }

  @Override
  public void deleteScopedAnomalyGlobalConfigStatus(
      RequestContext requestContext, AnomalyConfigScope scope) {
    deleteObject(
        requestContext,
        anomalyConfigScopeUtils.getContextFromAnomalyConfigScope(
            getTenantId(RequestContext.CURRENT.get()), scope));
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
    return configConverter.convertScopedConfig(
        migrateScopedAnomalyConfigStatusChange(scopedAnomalyConfigBuilder.build()),
        config,
        configConverter.merge(configStatusChange, getDefaultTierConfig(requestContext)));
  }

  private Map<String, ScopedAnomalyConfigStatusChange> fetchConfigMap(
      RequestContext requestContext) {
    return getAllObjects(requestContext).stream()
        .collect(Collectors.toMap(ContextualConfigObject::getContext, ConfigObject::getData));
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
}
