package ai.traceable.customsignature.config.service.rules.converter.expression;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.edge.decision.converter.utils.Constants.VAR_REGION_CITY_NAME;
import static ai.traceable.edge.decision.converter.utils.Constants.VAR_REGION_COUNTRY_ISO;
import static ai.traceable.edge.decision.converter.utils.Constants.VAR_REGION_STATE_NAME;

import ai.traceable.customsignature.config.service.v1.CityRegionIdentifier;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.Clause.ClauseCase;
import ai.traceable.customsignature.config.service.v1.CountryRegionIdentifier;
import ai.traceable.customsignature.config.service.v1.RegionExpression;
import ai.traceable.customsignature.config.service.v1.RegionIdentifier;
import ai.traceable.customsignature.config.service.v1.StateRegionIdentifier;
import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.BinaryOperator;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.MatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.StructuredMatchCondition;
import ai.traceable.edge.decision.converter.utils.ConverterUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class RegionExpressionConverter implements CustomSignatureExpressionConverter {

  @Override
  public MatchCondition buildMatchCondition(Clause clause) {
    RegionExpression regionExpression = clause.getRegionExpression();

    List<MatchCondition> conditions =
        regionExpression.getRegionsList().stream()
            .map(this::buildRegionMatchCondition)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    if (conditions.isEmpty()) {
      return MatchCondition.getDefaultInstance();
    }

    return ConverterUtils.buildOrMatchConditions(conditions)
        .setNegate(regionExpression.getExclude())
        .build();
  }

  private MatchCondition buildRegionMatchCondition(RegionIdentifier regionIdentifier) {
    switch (regionIdentifier.getRegionCase()) {
      case COUNTRY:
        return buildCountryMatchCondition(regionIdentifier.getCountry());
      case STATE:
        return buildStateMatchCondition(regionIdentifier.getState());
      case CITY:
        return buildCityMatchCondition(regionIdentifier.getCity());
      default:
        return null;
    }
  }

  private MatchCondition buildCountryMatchCondition(CountryRegionIdentifier country) {
    switch (country.getCountryCase()) {
      case ISO_CODE:
        return buildEqCondition(VAR_REGION_COUNTRY_ISO, country.getIsoCode());
      default:
        return null;
    }
  }

  private MatchCondition buildStateMatchCondition(StateRegionIdentifier state) {
    List<MatchCondition> conditions = new ArrayList<>();
    addStateConstraints(conditions, state);
    return conditions.isEmpty() ? null : combineWithAnd(conditions);
  }

  private MatchCondition buildCityMatchCondition(CityRegionIdentifier city) {
    List<MatchCondition> conditions = new ArrayList<>();

    switch (city.getCityCase()) {
      case NAME:
        conditions.add(buildEqCondition(VAR_REGION_CITY_NAME, city.getName()));
        break;
      case NAME_REGEX:
        conditions.add(buildLikeCondition(VAR_REGION_CITY_NAME, city.getNameRegex()));
        break;
      default:
        break;
    }

    if (city.hasState()) {
      addStateConstraints(conditions, city.getState());
    }

    return conditions.isEmpty() ? null : combineWithAnd(conditions);
  }

  private void addStateConstraints(List<MatchCondition> conditions, StateRegionIdentifier state) {
    switch (state.getStateCase()) {
      case NAME:
        conditions.add(buildEqCondition(VAR_REGION_STATE_NAME, state.getName()));
        break;
      case NAME_REGEX:
        conditions.add(buildLikeCondition(VAR_REGION_STATE_NAME, state.getNameRegex()));
        break;
      default:
        break;
    }

    if (state.hasCountry()) {
      MatchCondition countryCondition = buildCountryMatchCondition(state.getCountry());
      if (countryCondition != null) {
        conditions.add(countryCondition);
      }
    }
  }

  private MatchCondition combineWithAnd(List<MatchCondition> conditions) {
    if (conditions.size() == 1) {
      return conditions.get(0);
    }
    return MatchCondition.newBuilder()
        .setLogicalMatchCondition(
            LogicalMatchCondition.newBuilder()
                .setOperator(LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND)
                .addAllConditions(conditions))
        .build();
  }

  private MatchCondition buildEqCondition(String variableName, String value) {
    return MatchCondition.newBuilder()
        .setStructuredMatchCondition(
            StructuredMatchCondition.newBuilder()
                .setLhs(buildLhs(variableName))
                .setBinaryOperator(
                    BinaryOperator.newBuilder()
                        .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQ)
                        .setStringValue(value)))
        .build();
  }

  private MatchCondition buildLikeCondition(String variableName, String regex) {
    return MatchCondition.newBuilder()
        .setStructuredMatchCondition(
            StructuredMatchCondition.newBuilder()
                .setLhs(buildLhs(variableName))
                .setBinaryOperator(
                    BinaryOperator.newBuilder()
                        .setMatchOperator(MatchOperator.MATCH_OPERATOR_LIKE)
                        .setRegex(regex)))
        .build();
  }

  private AttributeDerivationMapping buildLhs(String variableName) {
    return AttributeDerivationMapping.newBuilder()
        .setName(variableName)
        .setType(FIELD_TYPE_STR)
        .build();
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return ClauseCase.REGION_EXPRESSION;
  }
}
