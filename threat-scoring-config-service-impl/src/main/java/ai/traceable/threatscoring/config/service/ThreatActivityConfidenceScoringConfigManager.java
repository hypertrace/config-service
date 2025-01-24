package ai.traceable.threatscoring.config.service;

import ai.traceable.threatscoring.config.service.store.ScopedThreatScoringConfigsStore;
import ai.traceable.threatscoring.config.service.v1.DeleteThreatActivityConfidenceScoringConfigOverridesRequest;
import ai.traceable.threatscoring.config.service.v1.GetScopedThreatScoringConfigsRequest;
import ai.traceable.threatscoring.config.service.v1.OverrideThreatActivityConfidenceScoringConfigRequest;
import ai.traceable.threatscoring.config.service.v1.ScopedThreatScoringConfigs;
import ai.traceable.threatscoring.config.service.v1.ThreatActivityConfidenceScoringConfig;
import ai.traceable.threatscoring.config.service.v1.ThreatScoringConfigs;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ThreatActivityConfidenceScoringConfigManager {
  private static final String THREAT_SCORING_RESOURCE_NAMESPACE =
      "threat-scoring-confidence-scoring-config";
  private final ScopedThreatScoringConfigsStore scopedThreatScoringConfigsStore;
  private final ScopedThreatScoringConfigs defaultScopedThreatScoringConfigs;
  private final ThreatScoringConfigScopeUtils threatScoringConfigScopeUtils;

  @Inject
  public ThreatActivityConfidenceScoringConfigManager(
      ThreatScoringConfigScopeUtils threatScoringConfigScopeUtils,
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      DefaultThreatScoringConfig defaultThreatScoringConfig) {
    this.threatScoringConfigScopeUtils = threatScoringConfigScopeUtils;
    this.defaultScopedThreatScoringConfigs = defaultThreatScoringConfig.getDefaultConfig();
    this.scopedThreatScoringConfigsStore =
        new ScopedThreatScoringConfigsStore(
            configServiceBlockingStub,
            configChangeEventGenerator,
            THREAT_SCORING_RESOURCE_NAMESPACE);
  }

  @VisibleForTesting
  ThreatActivityConfidenceScoringConfigManager(
      ThreatScoringConfigScopeUtils threatScoringConfigScopeUtils,
      ScopedThreatScoringConfigsStore scopedThreatScoringConfigsStore,
      ScopedThreatScoringConfigs defaultScopedThreatScoringConfigs) {
    this.threatScoringConfigScopeUtils = threatScoringConfigScopeUtils;
    this.defaultScopedThreatScoringConfigs = defaultScopedThreatScoringConfigs;
    this.scopedThreatScoringConfigsStore = scopedThreatScoringConfigsStore;
  }

  ThreatActivityConfidenceScoringConfig getResolvedConfig(
      GetScopedThreatScoringConfigsRequest request, RequestContext requestContext) {
    List<ScopedThreatScoringConfigs> configsList =
        new ArrayList<>(Collections.singleton(defaultScopedThreatScoringConfigs));
    configsList.addAll(
        scopedThreatScoringConfigsStore.fetchConfigsInContextOrder(
            requestContext,
            threatScoringConfigScopeUtils.getContextsWithIncreasingPriority(
                request.getConfigScope())));
    return mergeConfigs(configsList)
        .orElse(ThreatActivityConfidenceScoringConfig.getDefaultInstance());
  }

  ThreatActivityConfidenceScoringConfig overrideConfig(
      OverrideThreatActivityConfidenceScoringConfigRequest request, RequestContext requestContext) {
    ScopedThreatScoringConfigs.Builder builder =
        ScopedThreatScoringConfigs.newBuilder()
            .setConfigs(
                ThreatScoringConfigs.newBuilder()
                    .setThreatActivityConfidenceScoringConfig(
                        request.getThreatActivityConfidenceScoringConfig()));
    if (request.hasScope()) {
      builder.setConfigScope(request.getScope());
    }
    return scopedThreatScoringConfigsStore
        .upsertObject(requestContext, builder.build())
        .getData()
        .getConfigs()
        .getThreatActivityConfidenceScoringConfig();
  }

  void deleteOverrides(
      DeleteThreatActivityConfidenceScoringConfigOverridesRequest request,
      RequestContext requestContext) {
    scopedThreatScoringConfigsStore.deleteObject(
        requestContext,
        scopedThreatScoringConfigsStore.getContextFromData(request.getConfigScope()));
  }

  ThreatActivityConfidenceScoringConfig getDefaultThreatActivityConfidenceScoringConfig() {
    return defaultScopedThreatScoringConfigs
        .getConfigs()
        .getThreatActivityConfidenceScoringConfig();
  }

  private Optional<ThreatActivityConfidenceScoringConfig> mergeConfigs(
      List<ScopedThreatScoringConfigs> scopedDataProtectionConfigList) {
    return scopedDataProtectionConfigList.stream()
        .map(config -> config.getConfigs().getThreatActivityConfidenceScoringConfig())
        .reduce((first, second) -> first.toBuilder().mergeFrom(second).build());
  }
}
