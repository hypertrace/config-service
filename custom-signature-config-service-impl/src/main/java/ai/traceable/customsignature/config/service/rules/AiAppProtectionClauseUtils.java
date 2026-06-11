package ai.traceable.customsignature.config.service.rules;

import ai.traceable.customsignature.config.service.v1.Category;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import java.util.Set;
import lombok.experimental.UtilityClass;

@UtilityClass
final class AiAppProtectionClauseUtils {

  private static final Set<String> ALLOWED_GENAI_ATTRIBUTE_KEYS =
      Set.of("GENAI_MODELS", "GENAI_PROVIDERS", "genAiPromptSize");

  static boolean isAiAppProtectionCategory(Category category) {
    return category == Category.CATEGORY_AI_APP_PROTECTION;
  }

  static boolean isExemptFromAttributeAndBlockingEventTypeCheck(
      Category category, ClauseGroup clauseGroup) {
    if (category != Category.CATEGORY_AI_APP_PROTECTION) {
      return false;
    }
    if (hasResponseCategoryClause(clauseGroup)) {
      return false;
    }
    return clauseGroup.getClausesList().stream()
        .filter(Clause::hasAttributeKeyValueExpression)
        .allMatch(
            clause ->
                isAllowedGenAiAttributeKey(
                    clause.getAttributeKeyValueExpression().getKeyCondition().getValue()));
  }

  private static boolean hasResponseCategoryClause(ClauseGroup clauseGroup) {
    return clauseGroup.getClausesList().stream()
        .anyMatch(AiAppProtectionClauseUtils::isResponseClause);
  }

  private static boolean isResponseClause(Clause clause) {
    return clause
            .getMatchExpression()
            .getMatchCategory()
            .equals(MatchCategory.MATCH_CATEGORY_RESPONSE)
        || clause
            .getKeyValueExpression()
            .getMatchCategory()
            .equals(MatchCategory.MATCH_CATEGORY_RESPONSE);
  }

  private static boolean isAllowedGenAiAttributeKey(String attributeKey) {
    return ALLOWED_GENAI_ATTRIBUTE_KEYS.contains(attributeKey);
  }
}
