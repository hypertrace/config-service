package ai.traceable.localprocessing.config.service.regularmodsec;

import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesResponse;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesType;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.RegularModsecDetectionRules;
import com.google.inject.Inject;
import java.util.List;

class DefaultRegularModsecDetectionManager implements RegularModsecDetectionManager {
  private final AnomalyModsecConfigServiceBlockingStub configServiceBlockingStub;
  private final UuidGenerator uuidGenerator;

  @Inject
  public DefaultRegularModsecDetectionManager(
      AnomalyModsecConfigServiceBlockingStub anomalyModsecConfigServiceBlockingStub,
      UuidGenerator uuidGenerator) {
    this.configServiceBlockingStub = anomalyModsecConfigServiceBlockingStub;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public RegularModsecDetectionRules getDetectionRules(String requestHash) {
    GetModsecCrsRulesResponse response =
        configServiceBlockingStub.getModsecCrsRules(
            GetModsecCrsRulesRequest.newBuilder()
                .addAllModsecCrsRulesTypes(
                    List.of(ModsecCrsRulesType.MODSEC_CRS_RULES_TYPE_REGULAR))
                .build());

    String regularCrsRulesBlob;
    if (response.getModsecCrsRulesList().size() == 1
        && response.getModsecCrsRulesList().get(0).getModsecCrsRulesType()
            == ModsecCrsRulesType.MODSEC_CRS_RULES_TYPE_REGULAR) {
      // The request was done for only one modsec type so we should be getting only 1 element
      regularCrsRulesBlob = response.getModsecCrsRulesList().get(0).getModsecCrsRulesBlob();
    } else {
      throw (new RuntimeException("Unable to get modsec crs detection rules"));
    }
    String responseHash = uuidGenerator.generateId(regularCrsRulesBlob);

    RegularModsecDetectionRules.Builder builder =
        RegularModsecDetectionRules.newBuilder().setHash(responseHash);
    if (!responseHash.equals(requestHash)) {
      builder.setRegularModsecDetectionRulesBlob(regularCrsRulesBlob);
    }
    return builder.build();
  }
}
