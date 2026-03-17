package ai.traceable.risk.config.service.v2.factors;

import static ai.traceable.risk.config.service.v2.EntityType.ENTITY_TYPE_API;
import static ai.traceable.risk.config.service.v2.EntityType.ENTITY_TYPE_MCP_TOOL;
import static ai.traceable.risk.config.service.v2.RiskFactorCategory.RISK_FACTOR_CATEGORY_BLAST_RADIUS;
import static ai.traceable.risk.config.service.v2.RiskFactorCategory.RISK_FACTOR_CATEGORY_EASE_OF_ACCESS;
import static ai.traceable.risk.config.service.v2.RiskFactorCategory.RISK_FACTOR_CATEGORY_EASE_OF_RESOURCE_DISCOVERY;
import static ai.traceable.risk.config.service.v2.RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS;
import static ai.traceable.risk.config.service.v2.RiskFactorCategory.RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE;
import static ai.traceable.risk.config.service.v2.RiskFactorCategory.RISK_FACTOR_CATEGORY_VULNERABILITY;

import ai.traceable.risk.config.service.v2.EntityType;
import ai.traceable.risk.config.service.v2.RiskFactorCategory;
import java.util.Map;
import java.util.Set;

public class RiskFactorCategoryFilter {

  private RiskFactorCategoryFilter() {}

  private static final Set<RiskFactorCategory> API_CATEGORIES =
      Set.of(
          RISK_FACTOR_CATEGORY_EASE_OF_ACCESS,
          RISK_FACTOR_CATEGORY_VULNERABILITY,
          RISK_FACTOR_CATEGORY_EASE_OF_RESOURCE_DISCOVERY,
          RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE,
          RISK_FACTOR_CATEGORY_BLAST_RADIUS,
          RISK_FACTOR_CATEGORY_LABELS);

  private static final Set<RiskFactorCategory> MCP_TOOL_CATEGORIES =
      Set.of(
          RISK_FACTOR_CATEGORY_EASE_OF_ACCESS,
          RISK_FACTOR_CATEGORY_VULNERABILITY,
          RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE,
          RISK_FACTOR_CATEGORY_LABELS);

  private static final Map<EntityType, Set<RiskFactorCategory>> SUPPORTED_CATEGORIES =
      Map.of(
          ENTITY_TYPE_API, API_CATEGORIES,
          ENTITY_TYPE_MCP_TOOL, MCP_TOOL_CATEGORIES);

  public static Set<RiskFactorCategory> getSupportedCategories(EntityType entityType) {
    return SUPPORTED_CATEGORIES.getOrDefault(entityType, API_CATEGORIES);
  }

  public static boolean isCategorySupported(EntityType entityType, RiskFactorCategory category) {
    return getSupportedCategories(entityType).contains(category);
  }
}
