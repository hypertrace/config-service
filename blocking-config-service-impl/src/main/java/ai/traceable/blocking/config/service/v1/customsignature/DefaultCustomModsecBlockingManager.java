package ai.traceable.blocking.config.service.v1.customsignature;

import ai.traceable.blocking.config.service.common.customsignature.CustomSignatureBlobFetcher;
import ai.traceable.blocking.config.service.v1.CustomModsecBlockingRules;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import com.google.inject.Inject;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DefaultCustomModsecBlockingManager implements CustomModsecBlockingManager {
  private final CustomSignatureBlobFetcher customSignatureBlobFetcher;
  private final UuidGenerator uuidGenerator;

  @Inject
  public DefaultCustomModsecBlockingManager(
      CustomSignatureBlobFetcher customSignatureBlobFetcher, UuidGenerator uuidGenerator) {
    this.customSignatureBlobFetcher = customSignatureBlobFetcher;
    this.uuidGenerator = uuidGenerator;
  }

  public CustomModsecBlockingRules getEnabledBlockingRules(
      RequestContext requestContext, String requestHash, Optional<String> environmentId) {
    String enabledCustomSignatureRulesBlob =
        customSignatureBlobFetcher.getEnabledCustomSignatureRulesBlob(
            requestContext, CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3, environmentId);

    String responseHash = uuidGenerator.generateId(enabledCustomSignatureRulesBlob);

    CustomModsecBlockingRules.Builder customModsecBlockingRulesBuilder =
        CustomModsecBlockingRules.newBuilder().setHash(responseHash);
    if (!responseHash.equals(requestHash)) {
      customModsecBlockingRulesBuilder.setCustomModsecRulesBlob(enabledCustomSignatureRulesBlob);
    }
    return customModsecBlockingRulesBuilder.build();
  }
}
