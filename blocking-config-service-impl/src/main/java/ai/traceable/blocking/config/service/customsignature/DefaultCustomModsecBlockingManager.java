package ai.traceable.blocking.config.service.customsignature;

import ai.traceable.blocking.config.service.v1.CustomModsecBlockingRules;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import com.google.inject.Inject;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DefaultCustomModsecBlockingManager implements CustomModsecBlockingManager {
  private static final GetCustomSignatureModsecRulesRequest getCustomSignatureModsecRulesRequest =
      GetCustomSignatureModsecRulesRequest.newBuilder()
          .setFilter(
              GetRulesFilter.newBuilder()
                  .addEventTypes(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                  .addEventTypes(EventType.EVENT_TYPE_ALLOW)
                  .setDisabled(false)
                  .build())
          .build();

  private final CustomSignatureConfigServiceBlockingStub configServiceBlockingStub;
  private final UuidGenerator uuidGenerator;

  @Inject
  public DefaultCustomModsecBlockingManager(
      CustomSignatureConfigServiceBlockingStub configServiceBlockingStub,
      UuidGenerator uuidGenerator) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.uuidGenerator = uuidGenerator;
  }

  public CustomModsecBlockingRules getEnabledBlockingRules(
      RequestContext requestContext, String requestHash, Optional<String> environmentId) {
    GetCustomSignatureModsecRulesResponse response =
        requestContext.call(
            () ->
                configServiceBlockingStub.getCustomSignatureModsecRules(
                    getCustomSignatureModsecRulesRequest));

    String responseHash = uuidGenerator.generateId(response.getModsecRulesBlob());

    CustomModsecBlockingRules.Builder customModsecBlockingRulesBuilder =
        CustomModsecBlockingRules.newBuilder().setHash(responseHash);
    if (!responseHash.equals(requestHash)) {
      customModsecBlockingRulesBuilder.setCustomModsecRulesBlob(response.getModsecRulesBlob());
    }
    return customModsecBlockingRulesBuilder.build();
  }
}
