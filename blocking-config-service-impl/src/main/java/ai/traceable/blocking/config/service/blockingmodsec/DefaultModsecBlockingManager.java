package ai.traceable.blocking.config.service.blockingmodsec;

import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesResponse;
import ai.traceable.blocking.config.service.v1.SafeCrsBlockingRules;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import java.util.List;

class DefaultModsecBlockingManager implements ModsecBlockingManager {
  private static final GetModsecCrsRulesRequest getModsecCrsRulesRequest =
      GetModsecCrsRulesRequest.newBuilder()
          .addAllSubRuleTypes(List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK))
          .setRemoveDisabledRules(true)
          .build();

  private final AnomalyModsecConfigServiceBlockingStub configServiceBlockingStub;
  private final UuidGenerator uuidGenerator;

  @Inject
  public DefaultModsecBlockingManager(
      AnomalyModsecConfigServiceBlockingStub anomalyModsecConfigServiceBlockingStub,
      UuidGenerator uuidGenerator) {
    this.configServiceBlockingStub = anomalyModsecConfigServiceBlockingStub;
    this.uuidGenerator = uuidGenerator;
  }

  public SafeCrsBlockingRules getBlockingRules(String requestHash) {
    GetModsecCrsRulesResponse response =
        configServiceBlockingStub.getModsecCrsRules(getModsecCrsRulesRequest);

    String blockingCrsRulesBlob;
    if (response.getModsecCrsRulesList().size() == 1
        && response.getModsecCrsRulesList().get(0).getSubRuleType()
            == AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK) {
      // The request was done for only one modsec type so we should be getting only 1 element
      blockingCrsRulesBlob = response.getModsecCrsRulesList().get(0).getModsecCrsRulesBlob();
    } else {
      throw (new RuntimeException("Unable to get modsec crs blocking rules"));
    }
    String responseHash = uuidGenerator.generateId(blockingCrsRulesBlob);

    SafeCrsBlockingRules.Builder builder = SafeCrsBlockingRules.newBuilder().setHash(responseHash);
    if (!responseHash.equals(requestHash)) {
      builder.setSafeCrsRulesBlob(blockingCrsRulesBlob);
    }
    return builder.build();
  }
}
