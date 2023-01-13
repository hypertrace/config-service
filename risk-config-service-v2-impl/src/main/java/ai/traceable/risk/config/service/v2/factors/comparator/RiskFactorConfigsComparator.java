package ai.traceable.risk.config.service.v2.factors.comparator;

import ai.traceable.risk.config.service.v2.RiskFactorConfig;

public interface RiskFactorConfigsComparator {

  boolean isFactorConfigEqual(RiskFactorConfig specificConfig, RiskFactorConfig defaultConfig);
}
