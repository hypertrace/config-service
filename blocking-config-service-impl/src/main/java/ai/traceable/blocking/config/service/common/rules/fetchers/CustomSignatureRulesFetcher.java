package ai.traceable.blocking.config.service.common.rules.fetchers;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesTarget;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class CustomSignatureRulesFetcher implements RulesFetcher {
  private static final GetRulesFilter DEFAULT_GET_CUSTOM_SIGNATURE_MODSEC_RULES_FILTER =
      GetRulesFilter.newBuilder().setDisabled(false).build();

  private final CustomSignatureConfigServiceBlockingStub configServiceBlockingStub;
  private final ClientConfig clientConfig;

  @Inject
  CustomSignatureRulesFetcher(
      CustomSignatureConfigServiceBlockingStub configServiceBlockingStub,
      ClientConfig clientConfig) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.clientConfig = clientConfig;
  }

  public GetCustomSignatureModsecRulesResponse fetchModsecRules(
      RequestContext requestContext,
      Optional<String> environmentId,
      CustomModsecRuleVersion customModsecRuleVersion) {
    GetCustomSignatureModsecRulesRequest request =
        GetCustomSignatureModsecRulesRequest.newBuilder()
            .setRuleVersion(customModsecRuleVersion)
            .setFilter(
                DEFAULT_GET_CUSTOM_SIGNATURE_MODSEC_RULES_FILTER.toBuilder()
                    .setRuleScope(
                        RuleScope.newBuilder()
                            .setEnvironmentScope(
                                environmentId
                                    .map(id -> EnvironmentScope.newBuilder().addEnvironmentIds(id))
                                    .orElse(EnvironmentScope.newBuilder())))
                    .build())
            .setModsecCrsRulesTarget(ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TA_BLOCKING)
            .build();

    return requestContext.call(
        () ->
            configServiceBlockingStub
                .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .getCustomSignatureModsecRules(request));
  }
}
