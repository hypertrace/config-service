package ai.traceable.anomaly.config.service.modsec.rules;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesData;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesType;
import java.util.List;
import java.util.Set;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ModsecManager {
  List<ModsecCrsRulesData> getModsecCrsRules(
      RequestContext requestContext, Set<ModsecCrsRulesType> requestTypes);
}
