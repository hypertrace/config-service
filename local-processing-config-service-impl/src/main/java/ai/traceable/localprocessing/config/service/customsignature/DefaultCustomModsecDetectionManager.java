package ai.traceable.localprocessing.config.service.customsignature;

import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.CustomModsecDetectionRules;
import com.google.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DefaultCustomModsecDetectionManager implements CustomModsecDetectionManager {
  private static final CustomModsecDetectionRules EMPTY_MODSEC_RULES =
      CustomModsecDetectionRules.newBuilder()
          .setCustomModsecDetectionRulesBlob("")
          .setHash(UuidGenerator.EMPTY_STRING_UUID)
          .build();

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
  public CustomModsecDetectionRules getEnabledRules(
      RequestContext requestContext, String requestHash, boolean shouldUseCoraza) {
    GetCustomSignatureModsecRulesResponse response =
        requestContext.call(
            () ->
                configServiceBlockingStub.getCustomSignatureModsecRules(
                    GetCustomSignatureModsecRulesRequest.newBuilder()
                        .setFilter(
                            GetRulesFilter.newBuilder()
                                .addEventTypes(EventType.EVENT_TYPE_NORMAL_DETECTION)
                                .setDisabled(false)
                                .build())
                        .setRuleVersion(
                            shouldUseCoraza
                                ? CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_CORAZA_V3
                                : CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_UNSPECIFIED)
                        .build()));

    String customModSecRulesBlob = response.getModsecRulesBlob();

    String responseHash = uuidGenerator.generateId(customModSecRulesBlob);

    CustomModsecDetectionRules.Builder customModsecBlockingRulesBuilder =
        CustomModsecDetectionRules.newBuilder().setHash(responseHash);
    if (!responseHash.equals(requestHash)) {
      customModsecBlockingRulesBuilder.setCustomModsecDetectionRulesBlob(customModSecRulesBlob);
    }
    return customModsecBlockingRulesBuilder.build();
  }

  @Override
  public CustomModsecDetectionRules getEmptyRules() {
    return EMPTY_MODSEC_RULES;
  }
}
