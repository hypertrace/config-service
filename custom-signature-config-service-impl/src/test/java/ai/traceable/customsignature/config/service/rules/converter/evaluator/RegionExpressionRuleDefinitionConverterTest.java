package ai.traceable.customsignature.config.service.rules.converter.evaluator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.CountryRegionIdentifier;
import ai.traceable.customsignature.config.service.v1.RegionExpression;
import ai.traceable.customsignature.config.service.v1.RegionIdentifier;
import ai.traceable.customsignature.config.service.v1.StateRegionIdentifier;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;
import org.junit.jupiter.api.Test;

class RegionExpressionRuleDefinitionConverterTest {

  private final RegionExpressionRuleDefinitionConverter converter =
      new RegionExpressionRuleDefinitionConverter();

  @Test
  void buildRuleDefinition_withRegionsField() {
    Clause clause =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setCountry(CountryRegionIdentifier.newBuilder().setIsoCode("US")))
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setCountry(CountryRegionIdentifier.newBuilder().setIsoCode("IN"))))
            .build();

    CustomSignatureRuleDefinition result = converter.buildCustomSignatureRuleDefinition(clause);

    assertNotNull(result);
    assertTrue(
        result
            .getCustomSignatureConditionExpression()
            .getConditionExpressionEvaluationIdentifier()
            .startsWith("region-"));
  }

  @Test
  void buildRuleDefinition_throwsWhenRegionsEmpty() {
    Clause clause = Clause.newBuilder().setRegionExpression(RegionExpression.newBuilder()).build();

    assertThrows(
        IllegalArgumentException.class, () -> converter.buildCustomSignatureRuleDefinition(clause));
  }

  @Test
  void buildRuleDefinition_skipsNonCountryRegions() {
    Clause clause =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setState(StateRegionIdentifier.newBuilder().setName("California")))
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setCountry(CountryRegionIdentifier.newBuilder().setIsoCode("US"))))
            .build();

    CustomSignatureRuleDefinition result = converter.buildCustomSignatureRuleDefinition(clause);

    assertNotNull(result);
  }

  @Test
  void getClauseCase_returnsRegionExpression() {
    assertEquals(Clause.ClauseCase.REGION_EXPRESSION, converter.getClauseCase());
  }
}
