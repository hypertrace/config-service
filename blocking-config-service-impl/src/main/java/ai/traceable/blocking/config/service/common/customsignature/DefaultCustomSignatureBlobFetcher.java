package ai.traceable.blocking.config.service.common.customsignature;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesTarget;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DefaultCustomSignatureBlobFetcher implements CustomSignatureBlobFetcher {
  private static final GetRulesFilter DEFAULT_GET_CUSTOM_SIGNATURE_MODSEC_RULES_FILTER =
      GetRulesFilter.newBuilder()
          .addEventTypes(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
          .addEventTypes(EventType.EVENT_TYPE_ALLOW)
          .setDisabled(false)
          .build();

  private final CustomSignatureConfigServiceBlockingStub configServiceBlockingStub;
  private final ClientConfig clientConfig;

  @Inject
  DefaultCustomSignatureBlobFetcher(
      CustomSignatureConfigServiceBlockingStub configServiceBlockingStub,
      ClientConfig clientConfig) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.clientConfig = clientConfig;
  }

  @Override
  public String getEnabledCustomSignatureRulesBlob(
      RequestContext requestContext,
      CustomModsecRuleVersion customModsecRuleVersion,
      Optional<String> environmentId) {
    GetCustomSignatureModsecRulesRequest request =
        GetCustomSignatureModsecRulesRequest.newBuilder()
            .setRuleVersion(customModsecRuleVersion)
            .setFilter(
                environmentId
                    .map(
                        id ->
                            (GetRulesFilter.newBuilder()
                                .addAllEventTypes(
                                    ImmutableList.of(
                                        EventType.EVENT_TYPE_DETECTION_AND_BLOCKING,
                                        EventType.EVENT_TYPE_ALLOW))
                                .setDisabled(false)
                                .setRuleScope(
                                    RuleScope.newBuilder()
                                        .setEnvironmentScope(
                                            EnvironmentScope.newBuilder().addEnvironmentIds(id)))
                                .build()))
                    .orElse(DEFAULT_GET_CUSTOM_SIGNATURE_MODSEC_RULES_FILTER))
            .setModsecCrsRulesTarget(ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TA_BLOCKING)
            .build();

    GetCustomSignatureModsecRulesResponse response =
        requestContext.call(
            () ->
                configServiceBlockingStub
                    .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getCustomSignatureModsecRules(request));

    return response.getModsecRulesBlob();
  }
}
