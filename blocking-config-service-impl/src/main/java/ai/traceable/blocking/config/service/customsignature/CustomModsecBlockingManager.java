package ai.traceable.blocking.config.service.customsignature;

import ai.traceable.blocking.config.service.v1.CustomModsecBlockingRules;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import com.google.inject.Inject;

public class CustomModsecBlockingManager {

  private final CustomSignatureConfigServiceBlockingStub configServiceBlockingStub;

  @Inject
  CustomModsecBlockingManager(CustomSignatureConfigServiceBlockingStub configServiceBlockingStub) {
    this.configServiceBlockingStub = configServiceBlockingStub;
  }

  public CustomModsecBlockingRules getEnabledBlockingRules() {
    GetCustomSignatureModsecRulesResponse response =
        configServiceBlockingStub.getCustomSignatureModsecRules(
            GetCustomSignatureModsecRulesRequest.newBuilder()
                .setFilter(
                    GetRulesFilter.newBuilder()
                        .addEventTypes(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                        .setDisabled(false)
                        .build())
                .build());
    return CustomModsecBlockingRules.newBuilder()
        .setCustomModsecRulesBlob(response.getModsecRulesBlob())
        .build();
  }
}
