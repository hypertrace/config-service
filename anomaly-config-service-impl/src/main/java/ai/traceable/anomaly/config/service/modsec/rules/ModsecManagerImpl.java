package ai.traceable.anomaly.config.service.modsec.rules;

import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesData;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesType;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
      RequestContext requestContext, Set<ModsecCrsRulesType> requestTypes) {
    List<ModsecCrsRulesData> modsecCrsRulesData = new ArrayList<>();
    for (ModsecCrsRulesType type : requestTypes) {
      switch (type) {
        case MODSEC_CRS_RULES_TYPE_SAFE:
          modsecCrsRulesData.add(
              ModsecCrsRulesData.newBuilder()
                  .setModsecCrsRulesType(ModsecCrsRulesType.MODSEC_CRS_RULES_TYPE_SAFE)
                  .setModsecCrsRulesBlob(modsecRulesRegistry.getModsecSafeCrsRulesBlob())
                  .build());
          break;
        case MODSEC_CRS_RULES_TYPE_REGULAR:
          modsecCrsRulesData.add(
              ModsecCrsRulesData.newBuilder()
                  .setModsecCrsRulesType(ModsecCrsRulesType.MODSEC_CRS_RULES_TYPE_REGULAR)
                  .setModsecCrsRulesBlob(modsecRulesRegistry.getModsecRegularCrsRulesBlob())
                  .build());
          break;
        default:
          throw new IllegalArgumentException("Unknown modsec crs rule type:" + type);
      }
    }
    return ImmutableList.copyOf(modsecCrsRulesData);
  }
}
