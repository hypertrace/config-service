package ai.traceable.detection.exclusion.config.service.v1.rules.modsec;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import java.util.List;
import lombok.Value;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ModsecClauseConverter {
  ModsecClauseResult convert(RequestContext requestContext, DetectionExclusionRule rule);

  @Value
  class ModsecClauseResult {
    List<Clause> clauses;
    List<ServiceDetail> serviceDetails;
  }

  @Value
  class ServiceDetail {
    String serviceName;
    boolean exclude;
  }
}
