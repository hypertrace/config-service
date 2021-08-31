package ai.traceable.risk.config.service.level.processor;

import ai.traceable.risk.config.service.RiskConfigConstants;
import ai.traceable.risk.config.service.processor.RiskConfigConverter;
import ai.traceable.risk.config.service.processor.RiskConfigServiceDao;
import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskLevelConfigValues;
import javax.inject.Inject;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class RiskLevelConfigServiceDao extends RiskConfigServiceDao<RiskLevelConfigValues> {

  @Inject
  protected RiskLevelConfigServiceDao(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      RiskConfigConverter<RiskLevelConfigValues> configConverter,
      RiskConfigUtils<RiskLevelConfigValues> configUtils) {
    super(configServiceBlockingStub, configConverter, configUtils);
  }

  @Override
  protected String getConfigResourceName() {
    return RiskConfigConstants.RISK_LEVEL_CONFIG_RESOURCE_NAME;
  }
}
