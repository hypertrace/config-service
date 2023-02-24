package ai.traceable.blocking.config.service.common.modsec;

import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesResponse;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import com.google.inject.Inject;
import java.util.List;

public class BlockingModsecBlobFetcher {

  private final AnomalyModsecConfigServiceBlockingStub configServiceBlockingStub;

  @Inject
  public BlockingModsecBlobFetcher(
      AnomalyModsecConfigServiceBlockingStub anomalyModsecConfigServiceBlockingStub) {
    this.configServiceBlockingStub = anomalyModsecConfigServiceBlockingStub;
  }

  public String getBlockingModsecBlob(ModsecRuleVersion modsecRuleVersion) {
    GetModsecCrsRulesResponse response =
        configServiceBlockingStub.getModsecCrsRules(
            GetModsecCrsRulesRequest.newBuilder()
                .addAllSubRuleTypes(List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK))
                .setRuleVersion(modsecRuleVersion)
                .setRemoveDisabledRules(true)
                .build());

    String blockingCrsRulesBlob;
    if (response.getModsecCrsRulesList().size() == 1
        && response.getModsecCrsRulesList().get(0).getSubRuleType()
            == AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK) {
      // The request was done for only one modsec type so we should be getting only 1 element
      blockingCrsRulesBlob = response.getModsecCrsRulesList().get(0).getModsecCrsRulesBlob();
    } else {
      throw (new RuntimeException("Unable to get modsec crs blocking rules"));
    }

    return blockingCrsRulesBlob;
  }
}
