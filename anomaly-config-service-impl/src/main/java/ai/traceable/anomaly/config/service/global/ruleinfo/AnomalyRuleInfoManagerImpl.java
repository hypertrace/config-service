package ai.traceable.anomaly.config.service.global.ruleinfo;

import ai.traceable.anomaly.config.service.registry.accounttakeover.AccountTakeoverRulesRegistry;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.registry.credentialstuffing.CredentialStuffingRulesRegistry;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.registry.volumetric.VolumetricRulesRegistry;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleTypeVersion;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class AnomalyRuleInfoManagerImpl implements RuleInfoManager {
  private final ApiDefinitionRegistry apiDefinitionRegistry;
  private final ModsecRulesRegistry modsecRulesRegistry;
  private final SessionRulesRegistry sessionRulesRegistry;
  private final VolumetricRulesRegistry volumetricRulesRegistry;
  private final CredentialStuffingRulesRegistry credentialStuffingRulesRegistry;
  private final AccountTakeoverRulesRegistry accountTakeoverRulesRegistry;
  private final WebAppRuleInfoProvider webAppRuleInfoProvider;
  private final ApiProtectionRuleInfoProvider apiProtectionRuleInfoProvider;
  private final AiAppRuleInfoProvider aiAppRuleInfoProvider;
  private final FeatureCachingClient featureCachingClient;

  @Inject
  AnomalyRuleInfoManagerImpl(
      ApiDefinitionRegistry apiDefinitionRegistry,
      ModsecRulesRegistry modsecRulesRegistry,
      SessionRulesRegistry sessionRulesRegistry,
      VolumetricRulesRegistry volumetricRulesRegistry,
      CredentialStuffingRulesRegistry credentialStuffingRulesRegistry,
      AccountTakeoverRulesRegistry accountTakeoverRulesRegistry,
      WebAppRuleInfoProvider webAppRuleInfoProvider,
      ApiProtectionRuleInfoProvider apiProtectionRuleInfoProvider,
      AiAppRuleInfoProvider aiAppRuleInfoProvider,
      FeatureCachingClient featureCachingClient) {
    this.apiDefinitionRegistry = apiDefinitionRegistry;
    this.modsecRulesRegistry = modsecRulesRegistry;
    this.sessionRulesRegistry = sessionRulesRegistry;
    this.volumetricRulesRegistry = volumetricRulesRegistry;
    this.credentialStuffingRulesRegistry = credentialStuffingRulesRegistry;
    this.accountTakeoverRulesRegistry = accountTakeoverRulesRegistry;
    this.webAppRuleInfoProvider = webAppRuleInfoProvider;
    this.apiProtectionRuleInfoProvider = apiProtectionRuleInfoProvider;
    this.aiAppRuleInfoProvider = aiAppRuleInfoProvider;
    this.featureCachingClient = featureCachingClient;
  }

  @Override
  public List<AnomalyRuleInfo> getModsecAnomalyRuleInfo(
      RequestContext requestContext,
      ModsecRuleVersion ruleVersion,
      RuleVersion version,
      boolean useTestModsecRules) {
    // Duplicate entries in both request and response are removed
    HashSet<AnomalyRuleInfo> ruleInfos = new HashSet<>();
    if (featureCachingClient.isWAAPVersioningEnabledForTenant(requestContext)
        && version != null
        && !version.equals(RuleVersion.getDefaultInstance())) {
      ruleInfos.addAll(webAppRuleInfoProvider.getWebAppRuleInfo(version));
    } else {
      ruleInfos.addAll(
          modsecRulesRegistry.getModsecRuleInfos(ruleVersion, useTestModsecRules).values());
    }
    return ImmutableList.copyOf(ruleInfos);
  }

  @Override
  public List<AnomalyRuleInfo> getAllApiProtectionAnomalyRuleInfo(
      RequestContext requestContext, RuleVersion version) {
    if (featureCachingClient.isApiProtectConfigPoliciesRevampEnabled(requestContext)
        && version != null
        && !version.equals(RuleVersion.getDefaultInstance())) {
      return apiProtectionRuleInfoProvider.getAllApiProtectRuleInfo(version);
    }
    return List.of();
  }

  @Override
  public List<AnomalyRuleInfo> getAnomalyRuleInfos(
      List<AnomalyEventFamily> eventFamilies,
      ModsecRuleVersion ruleVersion,
      List<AnomalyRuleTypeVersion> anomalyRuleTypeVersions,
      boolean useTestModsecRules) {
    // Duplicate entries in both request and response are removed
    HashSet<AnomalyRuleInfo> ruleInfos = new HashSet<>();
    if (Objects.isNull(anomalyRuleTypeVersions) || anomalyRuleTypeVersions.isEmpty()) {
      anomalyRuleTypeVersions =
          eventFamilies.stream()
              .map(
                  eventFamily ->
                      AnomalyRuleTypeVersion.newBuilder()
                          .setAnomalyEventFamily(eventFamily)
                          .build())
              .collect(Collectors.toList());
    }
    new HashSet<>(anomalyRuleTypeVersions)
        .forEach(
            anomalyRuleTypeVersion -> {
              switch (anomalyRuleTypeVersion.getAnomalyEventFamily()) {
                case ANOMALY_EVENT_FAMILY_API_DEF:
                  if (!anomalyRuleTypeVersion
                      .getRuleVersion()
                      .equals(RuleVersion.getDefaultInstance())) {
                    ruleInfos.addAll(
                        apiProtectionRuleInfoProvider.getApiProtectRuleInfo(
                            anomalyRuleTypeVersion.getRuleVersion(),
                            AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF));
                  } else {
                    ruleInfos.addAll(apiDefinitionRegistry.getApiDefRuleInfos().values());
                  }
                  break;
                case ANOMALY_EVENT_FAMILY_MODSEC:
                  if (!anomalyRuleTypeVersion
                      .getRuleVersion()
                      .equals(RuleVersion.getDefaultInstance())) {
                    ruleInfos.addAll(
                        webAppRuleInfoProvider.getWebAppRuleInfo(
                            anomalyRuleTypeVersion.getRuleVersion()));
                  } else {
                    ruleInfos.addAll(
                        modsecRulesRegistry
                            .getModsecRuleInfos(ruleVersion, useTestModsecRules)
                            .values());
                  }
                  break;
                case ANOMALY_EVENT_FAMILY_SESSION:
                  if (!anomalyRuleTypeVersion
                      .getRuleVersion()
                      .equals(RuleVersion.getDefaultInstance())) {
                    ruleInfos.addAll(
                        apiProtectionRuleInfoProvider.getApiProtectRuleInfo(
                            anomalyRuleTypeVersion.getRuleVersion(),
                            AnomalyEventFamily.ANOMALY_EVENT_FAMILY_SESSION));
                  } else {
                    ruleInfos.addAll(sessionRulesRegistry.getSessionRuleInfos().values());
                  }
                  break;
                case ANOMALY_EVENT_FAMILY_VOLUMETRIC:
                  if (!anomalyRuleTypeVersion
                      .getRuleVersion()
                      .equals(RuleVersion.getDefaultInstance())) {
                    ruleInfos.addAll(
                        apiProtectionRuleInfoProvider.getApiProtectRuleInfo(
                            anomalyRuleTypeVersion.getRuleVersion(),
                            AnomalyEventFamily.ANOMALY_EVENT_FAMILY_VOLUMETRIC));
                  } else {
                    ruleInfos.addAll(volumetricRulesRegistry.getVolumetricRuleInfos().values());
                  }
                  break;
                case ANOMALY_EVENT_FAMILY_CREDENTIAL_STUFFING:
                  if (!anomalyRuleTypeVersion
                      .getRuleVersion()
                      .equals(RuleVersion.getDefaultInstance())) {
                    ruleInfos.addAll(
                        apiProtectionRuleInfoProvider.getApiProtectRuleInfo(
                            anomalyRuleTypeVersion.getRuleVersion(),
                            AnomalyEventFamily.ANOMALY_EVENT_FAMILY_CREDENTIAL_STUFFING));
                  } else {
                    ruleInfos.addAll(
                        credentialStuffingRulesRegistry.getCredentialStuffingRuleInfos().values());
                    ruleInfos.addAll(
                        accountTakeoverRulesRegistry.getAccountTakeoverRuleInfos().values());
                  }
                  break;
                case ANOMALY_EVENT_FAMILY_GEN_AI:
                  ruleInfos.addAll(aiAppRuleInfoProvider.getAiAppRuleInfo());
                  break;
                default:
                  throw new IllegalArgumentException(
                      "Registry info provider not implemented for the type!");
              }
            });
    return ImmutableList.copyOf(ruleInfos);
  }
}
