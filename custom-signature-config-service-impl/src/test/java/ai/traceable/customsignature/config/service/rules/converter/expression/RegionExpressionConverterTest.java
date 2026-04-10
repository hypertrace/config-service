package ai.traceable.customsignature.config.service.rules.converter.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.customsignature.config.service.v1.CityRegionIdentifier;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.CountryRegionIdentifier;
import ai.traceable.customsignature.config.service.v1.RegionExpression;
import ai.traceable.customsignature.config.service.v1.RegionIdentifier;
import ai.traceable.customsignature.config.service.v1.StateRegionIdentifier;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.MatchOperator;
import org.junit.jupiter.api.Test;

class RegionExpressionConverterTest {

  private final RegionExpressionConverter converter = new RegionExpressionConverter();

  @Test
  void buildMatchCondition_withSingleCountry() {
    Clause clause =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setCountry(CountryRegionIdentifier.newBuilder().setIsoCode("US"))))
            .build();

    MatchCondition condition = converter.buildMatchCondition(clause);

    assertFalse(condition.getNegate());
    assertEqCondition(condition, "US");
  }

  @Test
  void buildMatchCondition_withMultipleCountries() {
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

    MatchCondition condition = converter.buildMatchCondition(clause);

    assertFalse(condition.getNegate());
    assertTrue(condition.hasLogicalMatchCondition());
    assertEquals(
        LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR,
        condition.getLogicalMatchCondition().getOperator());
    assertEquals(2, condition.getLogicalMatchCondition().getConditionsCount());
    assertEqCondition(condition.getLogicalMatchCondition().getConditions(0), "US");
    assertEqCondition(condition.getLogicalMatchCondition().getConditions(1), "IN");
  }

  @Test
  void buildMatchCondition_withExclude() {
    Clause clause =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .setExclude(true)
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setCountry(CountryRegionIdentifier.newBuilder().setIsoCode("CN"))))
            .build();

    MatchCondition condition = converter.buildMatchCondition(clause);

    assertTrue(condition.getNegate());
    assertEqCondition(condition, "CN");
  }

  @Test
  void buildMatchCondition_withCountryAndState() {
    Clause clause =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setCountry(CountryRegionIdentifier.newBuilder().setIsoCode("US")))
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setState(StateRegionIdentifier.newBuilder().setName("California"))))
            .build();

    MatchCondition condition = converter.buildMatchCondition(clause);

    assertFalse(condition.getNegate());
    assertTrue(condition.hasLogicalMatchCondition());
    assertEquals(
        LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR,
        condition.getLogicalMatchCondition().getOperator());
    assertEquals(2, condition.getLogicalMatchCondition().getConditionsCount());
  }

  @Test
  void buildMatchCondition_withSimpleStateName() {
    Clause clause =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setState(StateRegionIdentifier.newBuilder().setName("California"))))
            .build();

    MatchCondition condition = converter.buildMatchCondition(clause);

    assertEqCondition(condition, "California");
  }

  @Test
  void buildMatchCondition_withStateAndCountryConstraint() {
    Clause clause =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setState(
                                StateRegionIdentifier.newBuilder()
                                    .setName("California")
                                    .setCountry(
                                        CountryRegionIdentifier.newBuilder().setIsoCode("US")))))
            .build();

    MatchCondition condition = converter.buildMatchCondition(clause);

    assertTrue(condition.hasLogicalMatchCondition());
    assertEquals(
        LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND,
        condition.getLogicalMatchCondition().getOperator());
    assertEquals(2, condition.getLogicalMatchCondition().getConditionsCount());
    assertEqCondition(condition.getLogicalMatchCondition().getConditions(0), "California");
    assertEqCondition(condition.getLogicalMatchCondition().getConditions(1), "US");
  }

  @Test
  void buildMatchCondition_withStateNameRegex() {
    Clause clause =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setState(StateRegionIdentifier.newBuilder().setNameRegex("Cal.*"))))
            .build();

    MatchCondition condition = converter.buildMatchCondition(clause);

    assertTrue(condition.hasStructuredMatchCondition());
    assertEquals(
        MatchOperator.MATCH_OPERATOR_LIKE,
        condition.getStructuredMatchCondition().getBinaryOperator().getMatchOperator());
    assertEquals("Cal.*", condition.getStructuredMatchCondition().getBinaryOperator().getRegex());
  }

  @Test
  void buildMatchCondition_withSimpleCityName() {
    Clause clause =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setCity(CityRegionIdentifier.newBuilder().setName("Mumbai"))))
            .build();

    MatchCondition condition = converter.buildMatchCondition(clause);

    assertEqCondition(condition, "Mumbai");
  }

  @Test
  void buildMatchCondition_withCityAndStateAndCountryConstraint() {
    Clause clause =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setCity(
                                CityRegionIdentifier.newBuilder()
                                    .setName("San Francisco")
                                    .setState(
                                        StateRegionIdentifier.newBuilder()
                                            .setName("California")
                                            .setCountry(
                                                CountryRegionIdentifier.newBuilder()
                                                    .setIsoCode("US"))))))
            .build();

    MatchCondition condition = converter.buildMatchCondition(clause);

    assertTrue(condition.hasLogicalMatchCondition());
    assertEquals(
        LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND,
        condition.getLogicalMatchCondition().getOperator());
    assertEquals(3, condition.getLogicalMatchCondition().getConditionsCount());
    assertEqCondition(condition.getLogicalMatchCondition().getConditions(0), "San Francisco");
    assertEqCondition(condition.getLogicalMatchCondition().getConditions(1), "California");
    assertEqCondition(condition.getLogicalMatchCondition().getConditions(2), "US");
  }

  @Test
  void buildMatchCondition_withCityNameRegex() {
    Clause clause =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setCity(CityRegionIdentifier.newBuilder().setNameRegex("San.*"))))
            .build();

    MatchCondition condition = converter.buildMatchCondition(clause);

    assertTrue(condition.hasStructuredMatchCondition());
    assertEquals(
        MatchOperator.MATCH_OPERATOR_LIKE,
        condition.getStructuredMatchCondition().getBinaryOperator().getMatchOperator());
    assertEquals("San.*", condition.getStructuredMatchCondition().getBinaryOperator().getRegex());
  }

  @Test
  void buildMatchCondition_withMixedRegionTypes() {
    Clause clause =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setCountry(CountryRegionIdentifier.newBuilder().setIsoCode("US")))
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setState(StateRegionIdentifier.newBuilder().setName("Bavaria")))
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setCity(CityRegionIdentifier.newBuilder().setName("Tokyo"))))
            .build();

    MatchCondition condition = converter.buildMatchCondition(clause);

    assertTrue(condition.hasLogicalMatchCondition());
    assertEquals(
        LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR,
        condition.getLogicalMatchCondition().getOperator());
    assertEquals(3, condition.getLogicalMatchCondition().getConditionsCount());
  }

  @Test
  void buildMatchCondition_withExcludeAndMixedTypes() {
    Clause clause =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .setExclude(true)
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setCountry(CountryRegionIdentifier.newBuilder().setIsoCode("CN")))
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setState(StateRegionIdentifier.newBuilder().setName("Xinjiang"))))
            .build();

    MatchCondition condition = converter.buildMatchCondition(clause);

    assertTrue(condition.getNegate());
    assertTrue(condition.hasLogicalMatchCondition());
    assertEquals(
        LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR,
        condition.getLogicalMatchCondition().getOperator());
  }

  @Test
  void buildMatchCondition_deprecatedFieldIsIgnored() {
    Clause clause =
        Clause.newBuilder()
            .setRegionExpression(
                RegionExpression.newBuilder()
                    .addRegionIdentifiers(
                        RegionExpression.Region.newBuilder().setCountryIsoCode("US"))
                    .addRegions(
                        RegionIdentifier.newBuilder()
                            .setCountry(CountryRegionIdentifier.newBuilder().setIsoCode("US"))))
            .build();

    MatchCondition condition = converter.buildMatchCondition(clause);

    assertEqCondition(condition, "US");
  }

  private void assertEqCondition(MatchCondition condition, String expectedValue) {
    assertTrue(condition.hasStructuredMatchCondition());
    assertEquals(
        MatchOperator.MATCH_OPERATOR_EQ,
        condition.getStructuredMatchCondition().getBinaryOperator().getMatchOperator());
    assertEquals(
        expectedValue,
        condition.getStructuredMatchCondition().getBinaryOperator().getStringValue());
  }
}
