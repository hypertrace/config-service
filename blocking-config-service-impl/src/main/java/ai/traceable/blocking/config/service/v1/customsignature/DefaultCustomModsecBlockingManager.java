package ai.traceable.blocking.config.service.v1.customsignature;

import ai.traceable.blocking.config.service.v1.CustomModsecBlockingRules;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DefaultCustomModsecBlockingManager implements CustomModsecBlockingManager {
  private static final GetCustomSignatureModsecRulesRequest
      DEFAULT_GET_CUSTOM_SIGNATURE_MODSEC_RULES_REQUEST =
          GetCustomSignatureModsecRulesRequest.newBuilder()
              .setFilter(
                  GetRulesFilter.newBuilder()
                      .addEventTypes(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                      .addEventTypes(EventType.EVENT_TYPE_ALLOW)
                      .setDisabled(false))
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
    GetCustomSignatureModsecRulesRequest request =
        environmentId
            .map(
                id ->
                    GetCustomSignatureModsecRulesRequest.newBuilder()
                        .setFilter(
                            GetRulesFilter.newBuilder()
                                .addAllEventTypes(
                                    ImmutableList.of(
                                        EventType.EVENT_TYPE_DETECTION_AND_BLOCKING,
                                        EventType.EVENT_TYPE_ALLOW))
                                .setDisabled(false)
                                .setRuleScope(
                                    RuleScope.newBuilder()
                                        .setEnvironmentScope(
                                            EnvironmentScope.newBuilder().addEnvironmentIds(id))))
                        .build())
            .orElse(DEFAULT_GET_CUSTOM_SIGNATURE_MODSEC_RULES_REQUEST);

    GetCustomSignatureModsecRulesResponse response =
        requestContext.call(() -> configServiceBlockingStub.getCustomSignatureModsecRules(request));

    String responseHash = uuidGenerator.generateId(response.getModsecRulesBlob());

    CustomModsecBlockingRules.Builder customModsecBlockingRulesBuilder =
        CustomModsecBlockingRules.newBuilder().setHash(responseHash);
    if (!responseHash.equals(requestHash)) {
      customModsecBlockingRulesBuilder.setCustomModsecRulesBlob(response.getModsecRulesBlob());
    }
    return customModsecBlockingRulesBuilder.build();
  }
}
