package ai.traceable.anomaly.config.service.modsec.rules;

import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesData;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ModsecManagerImpl implements ModsecManager {
  private final ModsecRulesRegistry modsecRulesRegistry;

  @Inject
  public ModsecManagerImpl(ModsecRulesRegistry modsecRulesRegistry) {
    this.modsecRulesRegistry = modsecRulesRegistry;
  }

  @Override
  public List<ModsecCrsRulesData> getModsecCrsRules(
      RequestContext requestContext, List<AnomalySubRuleType> requestTypes) {
    return new HashSet<>(requestTypes)
        .stream()
            .map(
                requestType ->
                    ModsecCrsRulesData.newBuilder()
                        .setSubRuleType(requestType)
                        .setModsecCrsRulesBlob(
                            modsecRulesRegistry.getModsecCrsRulesBlob(requestType))
                        .build())
            .collect(Collectors.toUnmodifiableList());
  }
}
