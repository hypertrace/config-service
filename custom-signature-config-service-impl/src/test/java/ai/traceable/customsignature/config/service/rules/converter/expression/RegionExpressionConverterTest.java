package ai.traceable.customsignature.config.service.rules.converter.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.CountryRegionIdentifier;
import ai.traceable.customsignature.config.service.v1.RegionExpression;
import ai.traceable.customsignature.config.service.v1.RegionIdentifier;
import ai.traceable.customsignature.config.service.v1.StateRegionIdentifier;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.MatchOperator;
import com.google.protobuf.Value;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class RegionExpressionConverterTest {

  private final RegionExpressionConverter converter = new RegionExpressionConverter();

  @Test
  void buildMatchCondition_withRegionsField() {
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

    List<String> isoCodes = extractIsoCodes(condition);
    assertEquals(List.of("US", "IN"), isoCodes);
    assertEquals(
        MatchOperator.MATCH_OPERATOR_IN,
        condition.getStructuredMatchCondition().getBinaryOperator().getMatchOperator());
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

    assertEquals(
        MatchOperator.MATCH_OPERATOR_NOT_IN,
        condition.getStructuredMatchCondition().getBinaryOperator().getMatchOperator());
  }

  @Test
  void buildMatchCondition_skipsNonCountryRegions() {
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

    List<String> isoCodes = extractIsoCodes(condition);
    assertEquals(List.of("US"), isoCodes);
  }

  @Test
  void buildMatchCondition_withBothFieldsSameCountry_noDuplicates() {
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

    List<String> isoCodes = extractIsoCodes(condition);
    assertEquals(List.of("US"), isoCodes);
  }

  private List<String> extractIsoCodes(MatchCondition condition) {
    return condition
        .getStructuredMatchCondition()
        .getBinaryOperator()
        .getListValue()
        .getValuesList()
        .stream()
        .map(Value::getStringValue)
        .collect(Collectors.toList());
  }
}
