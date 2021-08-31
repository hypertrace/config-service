package ai.traceable.risk.config.service.factorgrid.processor;

import ai.traceable.risk.config.service.RiskConfigConstants;
import ai.traceable.risk.config.service.processor.RiskConfigConverter;
import ai.traceable.risk.config.service.processor.RiskConfigServiceDao;
import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskFactorGridConfigValues;
import javax.inject.Inject;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class RiskFactorGridConfigServiceDao
    extends RiskConfigServiceDao<RiskFactorGridConfigValues> {

  @Inject
  protected RiskFactorGridConfigServiceDao(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      RiskConfigConverter<RiskFactorGridConfigValues> configConverter,
      RiskConfigUtils<RiskFactorGridConfigValues> configUtils) {
    super(configServiceBlockingStub, configConverter, configUtils);
  }

  @Override
  protected String getConfigResourceName() {
    return RiskConfigConstants.RISK_FACTOR_GRID_CONFIG_RESOURCE_NAME;
  }
}
