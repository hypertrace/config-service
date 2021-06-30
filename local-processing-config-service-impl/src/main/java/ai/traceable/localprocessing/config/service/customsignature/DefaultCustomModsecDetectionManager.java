package ai.traceable.localprocessing.config.service.customsignature;

import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.CustomModsecDetectionRules;
import com.google.inject.Inject;

public class DefaultCustomModsecDetectionManager implements CustomModsecDetectionManager {
  private final CustomSignatureConfigServiceBlockingStub configServiceBlockingStub;
  private final UuidGenerator uuidGenerator;

  @Inject
  public DefaultCustomModsecDetectionManager(
      CustomSignatureConfigServiceBlockingStub configServiceBlockingStub,
      UuidGenerator uuidGenerator) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public CustomModsecDetectionRules getEnabledRules(String requestHash) {
    GetCustomSignatureModsecRulesResponse response =
        configServiceBlockingStub.getCustomSignatureModsecRules(
            GetCustomSignatureModsecRulesRequest.newBuilder()
                .setFilter(
                    GetRulesFilter.newBuilder()
                        .addEventTypes(EventType.EVENT_TYPE_NORMAL_DETECTION)
                        .setDisabled(false)
                        .build())
                .build());

    String responseHash = uuidGenerator.generateId(response.getModsecRulesBlob());

    CustomModsecDetectionRules.Builder customModsecBlockingRulesBuilder =
        CustomModsecDetectionRules.newBuilder().setHash(responseHash);
    if (!responseHash.equals(requestHash)) {
      customModsecBlockingRulesBuilder.setCustomModsecDetectionRulesBlob(
          response.getModsecRulesBlob());
    }
    return customModsecBlockingRulesBuilder.build();
  }
}
