package ai.traceable.risk.config.service.level.processor;

import ai.traceable.risk.config.service.level.RiskLevelConfigManager;
import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskLevelConfig;
import ai.traceable.risk.config.service.v1.RiskLevelConfigValues;
import io.grpc.Status;
import java.util.Optional;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.DefaultObjectStore;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RiskLevelConfigManagerImpl implements RiskLevelConfigManager {

  private final DefaultObjectStore<RiskLevelConfigValues> configStore;
  private final RiskConfigUtils<RiskLevelConfigValues> configUtils;
  private final RiskLevelConfigValues defaultRiskLevelConfigValues;

  @Inject
  public RiskLevelConfigManagerImpl(
      DefaultObjectStore<RiskLevelConfigValues> configStore,
      RiskConfigUtils<RiskLevelConfigValues> configUtils,
      RiskLevelConfigValues defaultRiskLevelConfigValues) {
    this.configStore = configStore;
    this.configUtils = configUtils;
    this.defaultRiskLevelConfigValues = defaultRiskLevelConfigValues;
  }

  @Override
  public RiskLevelConfig getRiskLevelConfig(RequestContext requestContext) {
    Optional<RiskLevelConfigValues> fetchedConfig = configStore.getData(requestContext);
    if (fetchedConfig.isEmpty()) {
      return buildRiskLevelConfig(defaultRiskLevelConfigValues, true);
    }
    return buildRiskLevelConfig(
        configUtils.mergeConfigs(fetchedConfig.get(), defaultRiskLevelConfigValues), false);
  }

  @Override
  public RiskLevelConfig updateRiskLevelConfig(
      RequestContext requestContext, RiskLevelConfigValues riskLevelConfigValues) {

    RiskLevelConfigValues configValues = riskLevelConfigValues;
    Status validationStatus = configUtils.validateConfig(configValues);
    if (!validationStatus.isOk()) {
      throw validationStatus.asRuntimeException();
    }

    if (configUtils.isConfigDefault(configValues, defaultRiskLevelConfigValues)) {
      return resetRiskLevelConfig(requestContext);
    }
    configValues = configStore.upsertObject(requestContext, configValues).getData();
    return buildRiskLevelConfig(configValues, false);
  }

  @Override
  public RiskLevelConfig resetRiskLevelConfig(RequestContext requestContext) {
    // remove specific config if persisted..
    configStore.deleteObject(requestContext);

    return buildRiskLevelConfig(defaultRiskLevelConfigValues, true);
  }

  private RiskLevelConfig buildRiskLevelConfig(RiskLevelConfigValues values, boolean isDefault) {
    return RiskLevelConfig.newBuilder()
        .setRiskLevelConfigValues(values)
        .setIsDefault(isDefault)
        .build();
  }
}
