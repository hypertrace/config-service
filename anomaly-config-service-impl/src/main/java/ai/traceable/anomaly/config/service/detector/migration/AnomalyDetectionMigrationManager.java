package ai.traceable.anomaly.config.service.detector.migration;

import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigManagerImpl;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.migration.AnomalyDetectionMigrationConfig;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@Singleton
public class AnomalyDetectionMigrationManager {

  private final FeatureCachingClient featureCachingClient;
  private final AnomalyDetectionMigrationStore anomalyDetectionMigrationStore;
  private final AnomalyDetectionConfigManagerImpl detectionConfigManager;
  private final ApiProtectMigrationProcessor apiProtectMigrationProcessor;
  private final Set<ContextualKey<Void>> apiProtectMigrationCompletedTenantsSet = new HashSet<>();

  @Inject
  public AnomalyDetectionMigrationManager(
      FeatureCachingClient featureCachingClient,
      AnomalyDetectionMigrationStore anomalyDetectionMigrationStore,
      AnomalyDetectionConfigManagerImpl detectionConfigManager,
      ApiProtectMigrationProcessor apiProtectMigrationProcessor) {
    this.featureCachingClient = featureCachingClient;
    this.anomalyDetectionMigrationStore = anomalyDetectionMigrationStore;
    this.detectionConfigManager = detectionConfigManager;
    this.apiProtectMigrationProcessor = apiProtectMigrationProcessor;
  }

  public void migrateToApiProtectIfApplicable(RequestContext requestContext) {
    if (!featureCachingClient.isApiProtectConfigPoliciesRevampEnabled(requestContext)) {
      return;
    }

    ContextualKey<Void> contextualKey = requestContext.buildInternalContextualKey();
    if (apiProtectMigrationCompletedTenantsSet.contains(contextualKey)) {
      return;
    }

    AnomalyDetectionMigrationConfig migrationConfig =
        anomalyDetectionMigrationStore
            .getData(requestContext)
            .orElse(AnomalyDetectionMigrationConfig.newBuilder().build());

    if (migrationConfig.getApiProtectMigrationCompleted()) {
      apiProtectMigrationCompletedTenantsSet.add(contextualKey);
    } else {
      updateApiProtectAnomalyConfigs(requestContext, migrationConfig);
    }
  }

  private void updateApiProtectAnomalyConfigs(
      RequestContext requestContext, AnomalyDetectionMigrationConfig migrationConfig) {
    try {
      List<ScopedAnomalyDetectionConfig> detectionConfigs =
          detectionConfigManager.getAllObjects(requestContext).stream()
              .map(ConfigObject::getData)
              .collect(Collectors.toList());

      if (detectionConfigs.isEmpty()) {
        markMigrationCompleted(requestContext, migrationConfig);
        return;
      }
      processMigration(requestContext, migrationConfig, detectionConfigs);
    } catch (Exception e) {
      log.error("Error updating API Protect anomaly configs", e);
    }
  }

  private void processMigration(
      RequestContext requestContext,
      AnomalyDetectionMigrationConfig migrationConfig,
      List<ScopedAnomalyDetectionConfig> configs) {
    int updated = 0;
    boolean allConfigsMigrated = true;

    for (ScopedAnomalyDetectionConfig config : configs) {
      try {
        ScopedAnomalyDetectionConfig updatedConfig =
            apiProtectMigrationProcessor.migrateConfig(config);
        if (!updatedConfig.equals(config)) {
          detectionConfigManager.upsertObject(requestContext, updatedConfig);
          updated++;
        }
      } catch (Exception e) {
        log.error("Failed to migrate config: {}", config, e);
        allConfigsMigrated = false;
      }
    }

    log.debug(
        "API Protect migration - updated {} configs, all migrated: {}",
        updated,
        allConfigsMigrated);
    if (allConfigsMigrated) {
      markMigrationCompleted(requestContext, migrationConfig);
    }
  }

  private void markMigrationCompleted(
      RequestContext requestContext, AnomalyDetectionMigrationConfig migrationConfig) {
    anomalyDetectionMigrationStore.upsertObject(
        requestContext, migrationConfig.toBuilder().setApiProtectMigrationCompleted(true).build());
    apiProtectMigrationCompletedTenantsSet.add(requestContext.buildInternalContextualKey());
  }
}
