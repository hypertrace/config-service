package ai.traceable.risk.config.service.v2.grid.manager;

import ai.traceable.risk.config.service.v2.RiskConfigBuilder;
import ai.traceable.risk.config.service.v2.RiskConfigIdGenerator;
import ai.traceable.risk.config.service.v2.RiskConfigScope;
import ai.traceable.risk.config.service.v2.RiskScoringGridCell;
import ai.traceable.risk.config.service.v2.RiskScoringGridConfig;
import ai.traceable.risk.config.service.v2.RiskScoringGridConfigValues;
import ai.traceable.risk.config.service.v2.grid.comparator.RiskScoringGridConfigComparator;
import java.util.Collection;
import java.util.Optional;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RiskScoringGridConfigManagerImpl implements RiskScoringGridConfigManager {

  private final IdentifiedObjectStore<RiskScoringGridConfigValues> configStore;
  private final RiskConfigBuilder<RiskScoringGridConfigValues> riskConfigBuilder;
  private final RiskScoringGridConfigComparator gridConfigComparator;
  private final RiskScoringGridConfigValues defaultRiskScoringGridConfigValues;
  private final RiskConfigIdGenerator configIdGenerator;
  private final String ID_NAME = "RiskScoringGrid";

  @Override
  public RiskScoringGridConfig getRiskScoringGridConfig(
      RequestContext requestContext, RiskConfigScope riskConfigScope) {
    RiskScoringGridConfigValues scopedDefaultRiskScoringGridConfigValues =
        buildMergedGlobalAndDefaultConfigValues(requestContext, riskConfigScope);
    Optional<RiskScoringGridConfigValues> fetchedConfig =
        configStore.getData(requestContext, configIdGenerator.generateId(ID_NAME, riskConfigScope));
    if (fetchedConfig.isEmpty()) {
      return buildRiskScoringGridConfig(scopedDefaultRiskScoringGridConfigValues, true);
    }
    return buildRiskScoringGridConfig(
        riskConfigBuilder.mergeConfigs(
            fetchedConfig.get(), scopedDefaultRiskScoringGridConfigValues),
        false);
  }

  @Override
  public RiskScoringGridConfig updateRiskScoringGridConfig(
      RequestContext requestContext,
      RiskConfigScope riskConfigScope,
      Collection<RiskScoringGridCell> riskScoringGridCells) {

    RiskScoringGridConfigValues scopedDefaultRiskScoringGridConfigValues =
        buildMergedGlobalAndDefaultConfigValues(requestContext, riskConfigScope);
    RiskScoringGridConfigValues configValues =
        RiskScoringGridConfigValues.newBuilder()
            .setRiskConfigScope(riskConfigScope)
            .addAllRiskScoringGridCells(riskScoringGridCells)
            .build();

    Optional<RiskScoringGridConfigValues> fetchedConfig =
        configStore.getData(requestContext, configIdGenerator.generateId(ID_NAME, riskConfigScope));

    if (fetchedConfig.isPresent()) {
      configValues = riskConfigBuilder.mergeConfigs(configValues, fetchedConfig.get());
      if (gridConfigComparator.isGridValuesEqual(configValues, fetchedConfig.get())) {
        return buildRiskScoringGridConfig(
            riskConfigBuilder.mergeConfigs(configValues, scopedDefaultRiskScoringGridConfigValues),
            false);
      }
    } else {
      configValues =
          riskConfigBuilder.mergeConfigs(configValues, scopedDefaultRiskScoringGridConfigValues);
    }

    if (gridConfigComparator.isGridValuesEqual(configValues, defaultRiskScoringGridConfigValues)) {
      return resetRiskScoringGridConfig(requestContext, riskConfigScope);
    }

    configValues = configStore.upsertObject(requestContext, configValues).getData();
    return buildRiskScoringGridConfig(
        riskConfigBuilder.mergeConfigs(configValues, scopedDefaultRiskScoringGridConfigValues),
        false);
  }

  @Override
  public RiskScoringGridConfig resetRiskScoringGridConfig(
      RequestContext requestContext, RiskConfigScope riskConfigScope) {
    configStore.deleteObject(
        requestContext, configIdGenerator.generateId(ID_NAME, riskConfigScope));
    RiskScoringGridConfigValues scopedDefaultRiskScoringGridConfigValues =
        buildMergedGlobalAndDefaultConfigValues(requestContext, riskConfigScope);
    return buildRiskScoringGridConfig(scopedDefaultRiskScoringGridConfigValues, true);
  }

  private RiskScoringGridConfig buildRiskScoringGridConfig(
      RiskScoringGridConfigValues riskScoringGridConfigValues, boolean isDefault) {
    return RiskScoringGridConfig.newBuilder()
        .setRiskScoringGridConfigValues(riskScoringGridConfigValues)
        .setIsDefault(isDefault)
        .build();
  }

  private RiskScoringGridConfigValues buildMergedGlobalAndDefaultConfigValues(
      RequestContext requestContext, RiskConfigScope riskConfigScope) {
    RiskScoringGridConfigValues fetchedGlobalConfig =
        configStore
            .getData(
                requestContext,
                configIdGenerator.generateId(ID_NAME, RiskConfigScope.getDefaultInstance()))
            .orElse(RiskScoringGridConfigValues.getDefaultInstance());
    RiskScoringGridConfigValues mergedGlobalAndDefaultConfig =
        riskConfigBuilder.mergeConfigs(fetchedGlobalConfig, defaultRiskScoringGridConfigValues);
    return RiskScoringGridConfigValues.newBuilder()
        .setRiskConfigScope(riskConfigScope)
        .addAllRiskScoringGridCells(mergedGlobalAndDefaultConfig.getRiskScoringGridCellsList())
        .build();
  }
}
