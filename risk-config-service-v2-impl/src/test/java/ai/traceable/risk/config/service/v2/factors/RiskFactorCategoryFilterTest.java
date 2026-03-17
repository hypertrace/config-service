package ai.traceable.risk.config.service.v2.factors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.risk.config.service.v2.EntityType;
import ai.traceable.risk.config.service.v2.RiskFactorCategory;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class RiskFactorCategoryFilterTest {

  @Test
  void testSupportedCategoriesForApi() {
    // Given
    var categories = RiskFactorCategoryFilter.getSupportedCategories(EntityType.ENTITY_TYPE_API);
    // Then
    assertEquals(6, categories.size());
    assertTrue(categories.contains(RiskFactorCategory.RISK_FACTOR_CATEGORY_EASE_OF_ACCESS));
    assertTrue(categories.contains(RiskFactorCategory.RISK_FACTOR_CATEGORY_VULNERABILITY));
    assertTrue(
        categories.contains(RiskFactorCategory.RISK_FACTOR_CATEGORY_EASE_OF_RESOURCE_DISCOVERY));
    assertTrue(
        categories.contains(RiskFactorCategory.RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE));
    assertTrue(categories.contains(RiskFactorCategory.RISK_FACTOR_CATEGORY_BLAST_RADIUS));
    assertTrue(categories.contains(RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS));
  }

  @Test
  void testSupportedCategoriesForMcpTool() {
    // Given
    var categories =
        RiskFactorCategoryFilter.getSupportedCategories(EntityType.ENTITY_TYPE_MCP_TOOL);
    // Then
    assertEquals(4, categories.size());
    assertTrue(categories.contains(RiskFactorCategory.RISK_FACTOR_CATEGORY_EASE_OF_ACCESS));
    assertTrue(categories.contains(RiskFactorCategory.RISK_FACTOR_CATEGORY_VULNERABILITY));
    assertTrue(
        categories.contains(RiskFactorCategory.RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE));
    assertTrue(categories.contains(RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS));
    assertFalse(
        categories.contains(RiskFactorCategory.RISK_FACTOR_CATEGORY_EASE_OF_RESOURCE_DISCOVERY));
    assertFalse(categories.contains(RiskFactorCategory.RISK_FACTOR_CATEGORY_BLAST_RADIUS));
  }

  @Test
  void testSupportedCategoriesForUnspecified() {
    assertEquals(
        6,
        RiskFactorCategoryFilter.getSupportedCategories(EntityType.ENTITY_TYPE_UNSPECIFIED).size());
  }

  @ParameterizedTest
  @MethodSource("mcpToolCategorySupport")
  void testIsCategorySupportedForMcpTool(RiskFactorCategory category, boolean expected) {
    assertEquals(
        expected,
        RiskFactorCategoryFilter.isCategorySupported(EntityType.ENTITY_TYPE_MCP_TOOL, category));
  }

  static Stream<Arguments> mcpToolCategorySupport() {
    return Stream.of(
        Arguments.of(RiskFactorCategory.RISK_FACTOR_CATEGORY_EASE_OF_ACCESS, true),
        Arguments.of(RiskFactorCategory.RISK_FACTOR_CATEGORY_VULNERABILITY, true),
        Arguments.of(RiskFactorCategory.RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE, true),
        Arguments.of(RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS, true),
        Arguments.of(RiskFactorCategory.RISK_FACTOR_CATEGORY_EASE_OF_RESOURCE_DISCOVERY, false),
        Arguments.of(RiskFactorCategory.RISK_FACTOR_CATEGORY_BLAST_RADIUS, false));
  }
}
