package ai.traceable.customsignature.config.service.modsec;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.ScopeExpression;
import java.util.Objects;

public final class ModsecRulesSupportChecker {
  private static final String COOKIE_KEYWORD = "COOKIE";

  private ModsecRulesSupportChecker() {}

  /**
   * V2 implementation that recursively checks nested clause groups for inline rule mapping support.
   */
  public static boolean isInlineRuleMappingSupportedV2(ClauseGroup clauseGroup) {
    if (Objects.isNull(clauseGroup)) {
      return true;
    }

    for (Clause clause : clauseGroup.getClausesList()) {
      // Check if this clause itself is unsupported
      if (!isClauseSupported(clause)) {
        return false;
      }

      // Recursively check nested clause groups
      if (clause.hasClauseGroup() && !isInlineRuleMappingSupportedV2(clause.getClauseGroup())) {
        return false;
      }
    }

    return true;
  }

  public static boolean isInlineRuleMappingSupported(ClauseGroup clauseGroup) {
    if (Objects.isNull(clauseGroup)) {
      return true;
    }

    for (Clause clause : clauseGroup.getClausesList()) {
      if (!isClauseSupported(clause)) {
        return false;
      }
    }

    return true;
  }

  /** Checks if a single clause is supported for inline rule mapping. Shared by both V1 and V2. */
  private static boolean isClauseSupported(Clause clause) {
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
        || clause.hasClauseGroup()
        || clause.hasLhsRhsKeysExpression()) {
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
    if (clause.getMatchExpression().getMatchCategory().equals(MatchCategory.MATCH_CATEGORY_RESPONSE)
        && clause.getMatchExpression().getMatchKey().name().contains(COOKIE_KEYWORD)) {
      return false;
    }

    // API scope is not supported in modsec
    if (clause.getScopeExpression().getEntityScope().getEntityType()
        == ScopeExpression.EntityType.ENTITY_TYPE_API) {
      return false;
    }
    if (clause.getScopeExpression().getLabelScope().getLabelType()
        == ScopeExpression.LabelType.LABEL_TYPE_API) {
      return false;
    }

    return true;
  }
}
