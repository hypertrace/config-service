package ai.traceable.blocking.config.service.common.rules.fetchers;

import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import java.util.Optional;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class CustomSignatureRulesFetcher implements RulesFetcher {
  private static final GetRulesFilter DEFAULT_GET_CUSTOM_SIGNATURE_MODSEC_RULES_FILTER =
      GetRulesFilter.newBuilder()
          .addEventTypes(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
          .addEventTypes(EventType.EVENT_TYPE_ALLOW)
          .setDisabled(false)
          .build();

  private final CustomSignatureConfigServiceBlockingStub configServiceBlockingStub;

  @Inject
  CustomSignatureRulesFetcher(CustomSignatureConfigServiceBlockingStub configServiceBlockingStub) {
    this.configServiceBlockingStub = configServiceBlockingStub;
  }

  public GetCustomSignatureModsecRulesResponse fetchModsecRules(
      RequestContext requestContext,
      Optional<String> environmentId,
      CustomModsecRuleVersion customModsecRuleVersion) {
    GetCustomSignatureModsecRulesRequest request =
        GetCustomSignatureModsecRulesRequest.newBuilder()
            .setRuleVersion(customModsecRuleVersion)
            .setFilter(
                environmentId
                    .map(
                        id ->
                            (DEFAULT_GET_CUSTOM_SIGNATURE_MODSEC_RULES_FILTER.toBuilder()
                                .setRuleScope(
                                    RuleScope.newBuilder()
                                        .setEnvironmentScope(
                                            EnvironmentScope.newBuilder().addEnvironmentIds(id)))
                                .build()))
                    .orElse(DEFAULT_GET_CUSTOM_SIGNATURE_MODSEC_RULES_FILTER))
            .build();

    return requestContext.call(
        () -> configServiceBlockingStub.getCustomSignatureModsecRules(request));
  }
}
