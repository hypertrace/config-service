package ai.traceable.anomaly.config.service.global.ruleinfo;

import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.registry.credentialstuffing.CredentialStuffingRulesRegistry;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.registry.volumetric.VolumetricRulesRegistry;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import java.util.HashSet;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class AnomalyRuleInfoManagerImpl implements RuleInfoManager {
  private final ApiDefinitionRegistry apiDefinitionRegistry;
  private final ModsecRulesRegistry modsecRulesRegistry;
  private final SessionRulesRegistry sessionRulesRegistry;
  private final VolumetricRulesRegistry volumetricRulesRegistry;
  private final CredentialStuffingRulesRegistry credentialStuffingRulesRegistry;

  @Inject
  AnomalyRuleInfoManagerImpl(
      ApiDefinitionRegistry apiDefinitionRegistry,
      ModsecRulesRegistry modsecRulesRegistry,
      SessionRulesRegistry sessionRulesRegistry,
      VolumetricRulesRegistry volumetricRulesRegistry,
      CredentialStuffingRulesRegistry credentialStuffingRulesRegistry) {
    this.apiDefinitionRegistry = apiDefinitionRegistry;
    this.modsecRulesRegistry = modsecRulesRegistry;
    this.sessionRulesRegistry = sessionRulesRegistry;
    this.volumetricRulesRegistry = volumetricRulesRegistry;
    this.credentialStuffingRulesRegistry = credentialStuffingRulesRegistry;
  }

  @Override
  public List<AnomalyRuleInfo> getAnomalyRuleInfos(
      RequestContext requestContext, List<AnomalyEventFamily> eventFamilies) {
    // Duplicate entires in both request and response are removed
    HashSet<AnomalyRuleInfo> ruleInfos = new HashSet<>();
    new HashSet<>(eventFamilies)
        .forEach(
            eventFamily -> {
              switch (eventFamily) {
                case ANOMALY_EVENT_FAMILY_API_DEF:
                  ruleInfos.addAll(apiDefinitionRegistry.getApiDefRuleInfos().values());
                  break;
                case ANOMALY_EVENT_FAMILY_MODSEC:
                  ruleInfos.addAll(modsecRulesRegistry.getModsecRuleInfos().values());
                  break;
                case ANOMALY_EVENT_FAMILY_SESSION:
                  ruleInfos.addAll(sessionRulesRegistry.getSessionRuleInfos().values());
                  break;
                case ANOMALY_EVENT_FAMILY_VOLUMETRIC:
                  ruleInfos.addAll(volumetricRulesRegistry.getVolumetricRuleInfos().values());
                  break;
                case ANOMALY_EVENT_FAMILY_CREDENTIAL_STUFFING:
                  ruleInfos.addAll(
                      credentialStuffingRulesRegistry.getCredentialStuffingRuleInfos().values());
                  break;
                default:
                  throw new IllegalArgumentException(
                      "Registry info provider not implemented for the type!");
              }
            });
    return ImmutableList.copyOf(ruleInfos);
  }
}
