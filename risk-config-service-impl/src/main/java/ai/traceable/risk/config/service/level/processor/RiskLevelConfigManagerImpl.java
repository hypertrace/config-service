package ai.traceable.risk.config.service.level.processor;

import ai.traceable.risk.config.service.level.RiskLevelConfigManager;
import ai.traceable.risk.config.service.processor.RiskConfigServiceDao;
import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskLevelConfig;
import ai.traceable.risk.config.service.v1.RiskLevelConfigValues;
import io.grpc.Status;
import java.util.Optional;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RiskLevelConfigManagerImpl implements RiskLevelConfigManager {

  private final RiskConfigServiceDao<RiskLevelConfigValues> configServiceDao;
  private final RiskConfigUtils<RiskLevelConfigValues> configUtils;
  private final RiskLevelConfigValues defaultRiskLevelConfigValues;

  @Inject
  public RiskLevelConfigManagerImpl(
      RiskConfigServiceDao<RiskLevelConfigValues> configServiceDao,
      RiskConfigUtils<RiskLevelConfigValues> configUtils,
      RiskLevelConfigValues defaultRiskLevelConfigValues) {
    this.configServiceDao = configServiceDao;
    this.configUtils = configUtils;
    this.defaultRiskLevelConfigValues = defaultRiskLevelConfigValues;
  }

  @Override
  public RiskLevelConfig getRiskLevelConfig(RequestContext requestContext) {
    Optional<RiskLevelConfigValues> fetchedConfig = configServiceDao.fetchConfig(requestContext);
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
    configValues = configServiceDao.upsertConfig(requestContext, configValues);
    return buildRiskLevelConfig(configValues, false);
  }

  @Override
  public RiskLevelConfig resetRiskLevelConfig(RequestContext requestContext) {
    // remove specific config if persisted..
    configServiceDao.deleteConfig(requestContext);
    return buildRiskLevelConfig(defaultRiskLevelConfigValues, true);
  }

  private RiskLevelConfig buildRiskLevelConfig(RiskLevelConfigValues values, boolean isDefault) {
    return RiskLevelConfig.newBuilder()
        .setRiskLevelConfigValues(values)
        .setIsDefault(isDefault)
        .build();
  }
}
