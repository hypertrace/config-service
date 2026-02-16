package ai.traceable.customsignature.config.service.rules;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EventType;

/**
 * Utility class to check if clauses have exclude enabled. This is used for validation and runtime
 * filtering since libtraceable doesn't support exclude for ALLOW/BLOCK event types yet.
 *
 * <p>TODO: Remove this utility class when support for exclude gets added in libtraceable.
 */
public final class ClauseExcludeChecker {

  private ClauseExcludeChecker() {}

  /**
   * Checks if a rule has ALLOW or BLOCK event type and contains any clause with exclude enabled.
   */
  public static boolean hasExcludeEnabledForAllowOrBlockRule(CustomSignatureRule rule) {
    EventType eventType = rule.getEffect().getEventType();
    if (eventType != EventType.EVENT_TYPE_ALLOW
        && eventType != EventType.EVENT_TYPE_DETECTION_AND_BLOCKING) {
      return false;
    }
    return hasAnyClauseWithExcludeEnabled(rule.getDefinition().getClauseGroup());
  }

  /** Checks if any clause in the clause group has exclude enabled. */
  public static boolean hasAnyClauseWithExcludeEnabled(ClauseGroup clauseGroup) {
    return clauseGroup.getClausesList().stream().anyMatch(ClauseExcludeChecker::hasExcludeEnabled);
  }

  /** Checks if a single clause has exclude enabled. */
  public static boolean hasExcludeEnabled(Clause clause) {
    switch (clause.getClauseCase()) {
      case IP_ADDRESS_EXPRESSION:
        return clause.getIpAddressExpression().getExclude();
      case REGION_EXPRESSION:
        return clause.getRegionExpression().getExclude();
      case IP_TYPE_EXPRESSION:
        return clause.getIpTypeExpression().getExclude();
      case IP_ASN_EXPRESSION:
        return clause.getIpAsnExpression().getExclude();
      case IP_CONNECTION_TYPE_EXPRESSION:
        return clause.getIpConnectionTypeExpression().getExclude();
      case IP_ORGANISATION_EXPRESSION:
        return clause.getIpOrganisationExpression().getExclude();
      case SCOPE_EXPRESSION:
        return clause.getScopeExpression().getExclude();
      case USER_AGENT_EXPRESSION:
        return clause.getUserAgentExpression().getExclude();
      case USER_ID_EXPRESSION:
        return clause.getUserIdExpression().getExclude();
      case EMAIL_DOMAIN_EXPRESSION:
        return clause.getEmailDomainExpression().getExclude();
      default:
        return false;
    }
  }
}
