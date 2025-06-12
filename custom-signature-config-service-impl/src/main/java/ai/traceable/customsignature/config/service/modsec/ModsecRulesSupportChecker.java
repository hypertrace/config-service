package ai.traceable.customsignature.config.service.modsec;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import java.util.Objects;

public final class ModsecRulesSupportChecker {
  private static final String COOKIE_KEYWORD = "COOKIE";

  private ModsecRulesSupportChecker() {}

  public static boolean isInlineRuleMappingSupported(ClauseGroup clauseGroup) {
    if (Objects.isNull(clauseGroup)) {
      return true;
    }
    for (Clause clause : clauseGroup.getClausesList()) {
      // following clauses cannot be converted to inline rule.
      if (clause.hasAttributeKeyValueExpression()
          || clause.hasIpReputationExpression()
          || clause.hasIpConnectionTypeExpression()
          || clause.hasIpOrganisationExpression()
          || clause.hasIpAsnExpression()
          || clause.hasIpAbuseVelocityExpression()
          || clause.hasUserIdExpression()
          || clause.hasEmailDomainExpression()
          || clause.hasUserAgentExpression()
          || clause.hasRequestScannerTypeExpression()
          || clause.hasClauseGroup()) {
        return false;
      }
      // response-cookie metadata is not supported in modsec
      if (clause
              .getKeyValueExpression()
              .getMatchCategory()
              .equals(MatchCategory.MATCH_CATEGORY_RESPONSE)
          && clause.getKeyValueExpression().getTag().name().contains(COOKIE_KEYWORD)) {
        return false;
      }
      if (clause
              .getMatchExpression()
              .getMatchCategory()
              .equals(MatchCategory.MATCH_CATEGORY_RESPONSE)
          && clause.getMatchExpression().getMatchKey().name().contains(COOKIE_KEYWORD)) {
        return false;
      }
    }
    return true;
  }
}
