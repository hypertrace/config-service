package ai.traceable.anomaly.config.service.registry.credentialstuffing;

import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.detector.CredentialStuffingAnomalyDetectionConfig;
import java.util.Map;

public interface CredentialStuffingRulesRegistry {

  Map<String, AnomalyRuleInfo> getCredentialStuffingRuleInfos();

  Map<String, CredentialStuffingAnomalyDetectionConfig> getCredentialStuffingRuleIdToConfigMap();
}
