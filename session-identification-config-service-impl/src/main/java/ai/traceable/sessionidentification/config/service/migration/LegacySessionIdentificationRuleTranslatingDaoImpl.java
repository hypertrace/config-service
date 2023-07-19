package ai.traceable.sessionidentification.config.service.migration;

import static java.util.concurrent.TimeUnit.SECONDS;

import ai.traceable.sensitivedata.config.service.v1.DeleteRedactionRuleRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class LegacySessionIdentificationRuleTranslatingDaoImpl
    implements LegacySessionIdentificationRuleTranslatingDao {
  private static final int DEFAULT_DEADLINE_SECONDS = 10;
  private final SessionIdentificationRuleConverter ruleConverter;
  private final SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub
      sensitiveDataConfigServiceBlockingStub;

  @Inject
  public LegacySessionIdentificationRuleTranslatingDaoImpl(
      SessionIdentificationRuleConverter ruleConverter,
      SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub
          sensitiveDataConfigServiceBlockingStub) {
    this.ruleConverter = ruleConverter;
    this.sensitiveDataConfigServiceBlockingStub = sensitiveDataConfigServiceBlockingStub;
  }

  @Override
  public List<SessionIdentificationRule> getSessionIdentificationRulesFromOldStore(
      RequestContext requestContext) {
    List<RedactionRule> redactionRuleList = fetchRedactionRules(requestContext);
    return redactionRuleList.stream()
        .filter(RedactionRule::getSessionIdentifier)
        .map(ruleConverter::convert)
        .flatMap(Optional::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public boolean deleteSessionIdentificationRuleFromOldStoreIfFound(
      RequestContext requestContext, String ruleId) {
    try {
      deleteRedactionRule(requestContext, ruleId);
      return true;
    } catch (RuntimeException e) {
      log.debug("Unable to delete RuleId: {} with request context: {}", ruleId, requestContext);
    }
    return false;
  }

  @Override
  public boolean isSessionIdentificationRuleFromOldStore(
      RequestContext requestContext, String ruleId) {
    return fetchRedactionRules(requestContext).stream()
        .anyMatch(rule -> rule.getId().equals(ruleId));
  }

  private List<RedactionRule> fetchRedactionRules(RequestContext requestContext) {
    return requestContext.call(
        () ->
            this.sensitiveDataConfigServiceBlockingStub
                .withDeadlineAfter(DEFAULT_DEADLINE_SECONDS, SECONDS)
                .getAllRedactionRules(GetAllRedactionRulesRequest.getDefaultInstance())
                .getRedactionRulesList());
  }

  private void deleteRedactionRule(RequestContext requestContext, String ruleId) {
    requestContext.call(
        () ->
            this.sensitiveDataConfigServiceBlockingStub
                .withDeadlineAfter(DEFAULT_DEADLINE_SECONDS, SECONDS)
                .deleteRedactionRule(
                    DeleteRedactionRuleRequest.newBuilder().setRedactionRuleId(ruleId).build()));
  }
}
