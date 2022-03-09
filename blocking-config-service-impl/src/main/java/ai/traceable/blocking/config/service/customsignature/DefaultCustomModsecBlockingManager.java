package ai.traceable.blocking.config.service.customsignature;

import ai.traceable.blocking.config.service.UuidGenerator;
import ai.traceable.blocking.config.service.v1.CustomModsecBlockingRules;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import com.google.inject.Inject;

class DefaultCustomModsecBlockingManager implements CustomModsecBlockingManager {

  private final CustomSignatureConfigServiceBlockingStub configServiceBlockingStub;
  private final UuidGenerator uuidGenerator;

  @Inject
  public DefaultCustomModsecBlockingManager(
      CustomSignatureConfigServiceBlockingStub configServiceBlockingStub,
      UuidGenerator uuidGenerator) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.uuidGenerator = uuidGenerator;
  }

  public CustomModsecBlockingRules getEnabledBlockingRules(String requestHash) {
    GetCustomSignatureModsecRulesResponse response =
        configServiceBlockingStub.getCustomSignatureModsecRules(
            GetCustomSignatureModsecRulesRequest.newBuilder()
                .setFilter(
                    GetRulesFilter.newBuilder()
                        .addEventTypes(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                        .addEventTypes(EventType.EVENT_TYPE_ALLOW)
                        .setDisabled(false)
                        .build())
                .build());

    String responseHash = uuidGenerator.generateId(response.getModsecRulesBlob());

    CustomModsecBlockingRules.Builder customModsecBlockingRulesBuilder =
        CustomModsecBlockingRules.newBuilder().setHash(responseHash);
    if (!responseHash.equals(requestHash)) {
      customModsecBlockingRulesBuilder.setCustomModsecRulesBlob(response.getModsecRulesBlob());
    }
    return customModsecBlockingRulesBuilder.build();
  }
}
