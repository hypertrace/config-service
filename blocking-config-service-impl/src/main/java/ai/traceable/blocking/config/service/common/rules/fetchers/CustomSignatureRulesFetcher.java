package ai.traceable.blocking.config.service.common.rules.fetchers;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesTarget;
import ai.traceable.blocking.config.service.common.rules.ModsecRulesData;
import ai.traceable.customsignature.config.service.v1.Category;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.CustomSignatureInlineRule;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
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
            .setModsecCrsRulesTarget(ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TA_BLOCKING)
            .setFilter(
                DEFAULT_GET_CUSTOM_SIGNATURE_MODSEC_RULES_FILTER.toBuilder()
                    .addCategories(Category.CATEGORY_CUSTOM_SIGNATURE)
                    .setRuleScope(
                        RuleScope.newBuilder()
                            .setEnvironmentScope(
                                environmentId
                                    .map(id -> EnvironmentScope.newBuilder().addEnvironmentIds(id))
                                    .orElse(EnvironmentScope.newBuilder())))
                    .build())
            .build();

    return requestContext.call(
        () ->
            configServiceBlockingStub
                .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .getCustomSignatureModsecRules(request));
  }

  @Nullable
  public Map<String, ModsecRulesData<CustomSignatureInlineRule>> fetchCustomSignatureInlineRules(
      RequestContext requestContext, Optional<String> environmentId, Set<String> serviceNames) {
    if (serviceNames.isEmpty()) {
      return Collections.emptyMap();
    }

    GetCustomSignatureModsecRulesRequest request =
        GetCustomSignatureModsecRulesRequest.newBuilder()
            .setRuleVersion(CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS)
            .addAllServiceNames(serviceNames)
            .setModsecCrsRulesTarget(ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TA_BLOCKING)
            .setFilter(
                GetRulesFilter.newBuilder()
                    .addCategories(Category.CATEGORY_CUSTOM_SIGNATURE)
                    .setRuleScope(
                        environmentId
                            .map(
                                id ->
                                    RuleScope.newBuilder()
                                        .setEnvironmentScope(
                                            EnvironmentScope.newBuilder().addEnvironmentIds(id))
                                        .build())
                            .orElse(RuleScope.getDefaultInstance()))
                    .build())
            .build();

    GetCustomSignatureModsecRulesResponse response =
        requestContext.call(
            () ->
                configServiceBlockingStub
                    .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getCustomSignatureModsecRules(request));

    Map<String, ModsecRulesData<CustomSignatureInlineRule>> modsecRulesDataMap =
        response.getModsecBlobsDataList().stream()
            .flatMap(
                modsecBlobData -> {
                  ModsecRulesData<CustomSignatureInlineRule> modsecRulesData =
                      new ModsecRulesData<>(
                          response.getModsecDirectivesBlob(),
                          modsecBlobData.getModsecBlob(),
                          modsecBlobData.getCustomSignatureRuleIdsList(),
                          response.getInlineRulesList(),
                          customSignatureInlineRule -> customSignatureInlineRule.getRule().getId());
                  return modsecBlobData.getServiceNamesList().stream()
                      .map(serviceName -> Map.entry(serviceName, modsecRulesData));
                })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    // add empty entry for service names that do not have any custom signature inline rules
    serviceNames.forEach(
        serviceName ->
            modsecRulesDataMap.computeIfAbsent(serviceName, sName -> new ModsecRulesData<>()));
    return modsecRulesDataMap;
  }
}
