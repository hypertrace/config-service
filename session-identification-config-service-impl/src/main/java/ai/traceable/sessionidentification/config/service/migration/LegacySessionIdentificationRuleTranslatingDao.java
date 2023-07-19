package ai.traceable.sessionidentification.config.service.migration;

import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface LegacySessionIdentificationRuleTranslatingDao {
  List<SessionIdentificationRule> getSessionIdentificationRulesFromOldStore(
      RequestContext requestContext);

  boolean deleteSessionIdentificationRuleFromOldStoreIfFound(
      RequestContext requestContext, String ruleId);

  boolean isSessionIdentificationRuleFromOldStore(RequestContext requestContext, String ruleId);
}
