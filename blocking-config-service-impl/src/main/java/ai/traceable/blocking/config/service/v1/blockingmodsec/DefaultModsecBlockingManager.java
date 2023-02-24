package ai.traceable.blocking.config.service.v1.blockingmodsec;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.blocking.config.service.common.modsec.BlockingModsecBlobFetcher;
import ai.traceable.blocking.config.service.v1.SafeCrsBlockingRules;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;

class DefaultModsecBlockingManager implements ModsecBlockingManager {

  private final BlockingModsecBlobFetcher blockingModsecBlobFetcher;
  private final UuidGenerator uuidGenerator;

  @Inject
  public DefaultModsecBlockingManager(
      BlockingModsecBlobFetcher blockingModsecBlobFetcher, UuidGenerator uuidGenerator) {
    this.blockingModsecBlobFetcher = blockingModsecBlobFetcher;
    this.uuidGenerator = uuidGenerator;
  }

  public SafeCrsBlockingRules getBlockingRules(String requestHash) {
    String blockingCrsRulesBlob =
        blockingModsecBlobFetcher.getBlockingModsecBlob(ModsecRuleVersion.MODSEC_RULE_VERSION_V3);
    String responseHash = uuidGenerator.generateId(blockingCrsRulesBlob);

    SafeCrsBlockingRules.Builder builder = SafeCrsBlockingRules.newBuilder().setHash(responseHash);
    if (!responseHash.equals(requestHash)) {
      builder.setSafeCrsRulesBlob(blockingCrsRulesBlob);
    }
    return builder.build();
  }
}
