package ai.traceable.risk.config.service.factorgrid.processor;

import ai.traceable.risk.config.service.factorgrid.RiskFactorGridConfigManager;
import ai.traceable.risk.config.service.processor.RiskConfigServiceDao;
import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskFactorGridConfig;
import ai.traceable.risk.config.service.v1.RiskFactorGridConfigValues;
import io.grpc.Status;
import java.util.Optional;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RiskFactorGridConfigManagerImpl implements RiskFactorGridConfigManager {

  private final RiskConfigServiceDao<RiskFactorGridConfigValues> configServiceDao;
  private final RiskConfigUtils<RiskFactorGridConfigValues> configUtils;
  private final RiskFactorGridConfigValues defaultRiskFactorGridConfigValues;

  @Inject
  public RiskFactorGridConfigManagerImpl(
      RiskConfigServiceDao<RiskFactorGridConfigValues> configServiceDao,
      RiskConfigUtils<RiskFactorGridConfigValues> configUtils,
      RiskFactorGridConfigValues defaultRiskFactorGridConfigValues) {
    this.configServiceDao = configServiceDao;
    this.configUtils = configUtils;
    this.defaultRiskFactorGridConfigValues = defaultRiskFactorGridConfigValues;
  }

  @Override
  public RiskFactorGridConfig getRiskFactorGridConfig(RequestContext requestContext) {
    Optional<RiskFactorGridConfigValues> fetchedConfig =
        configServiceDao.fetchConfig(requestContext);
    if (fetchedConfig.isEmpty()) {
      return buildRiskFactorGridConfig(defaultRiskFactorGridConfigValues, true);
    }
    return buildRiskFactorGridConfig(
        configUtils.mergeConfigs(fetchedConfig.get(), defaultRiskFactorGridConfigValues), false);
  }

  @Override
  public RiskFactorGridConfig updateRiskFactorGridConfig(
      RequestContext requestContext, RiskFactorGridConfigValues riskFactorGridConfigValues) {

    RiskFactorGridConfigValues configValues = riskFactorGridConfigValues;
    Status validationStatus = configUtils.validateConfig(configValues);
    if (!validationStatus.isOk()) {
      throw validationStatus.asRuntimeException();
    }

    if (configUtils.isConfigDefault(configValues, defaultRiskFactorGridConfigValues)) {
      return resetRiskFactorGridConfig(requestContext);
    }
    configValues = configServiceDao.upsertConfig(requestContext, configValues);
    return buildRiskFactorGridConfig(
        configUtils.mergeConfigs(configValues, defaultRiskFactorGridConfigValues), false);
  }

  @Override
  public RiskFactorGridConfig resetRiskFactorGridConfig(RequestContext requestContext) {
    // remove specific config if persisted..
    configServiceDao.deleteConfig(requestContext);
    return buildRiskFactorGridConfig(defaultRiskFactorGridConfigValues, true);
  }

  private RiskFactorGridConfig buildRiskFactorGridConfig(
      RiskFactorGridConfigValues values, boolean isDefault) {
    return RiskFactorGridConfig.newBuilder()
        .setRiskFactorGridConfigValues(values)
        .setIsDefault(isDefault)
        .build();
  }
}
