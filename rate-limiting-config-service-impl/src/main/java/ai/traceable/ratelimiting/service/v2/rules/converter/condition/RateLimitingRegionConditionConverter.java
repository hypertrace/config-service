package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.BinaryOperator;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.MatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.StructuredMatchCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.RegionCondition;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingRegionConditionConverter implements RateLimitingConditionConverter {

  private static final String COUNTRY_ISO_CODE_JEXL_EXP =
      "$s.getIpIntelligenceData().getCountry().getIsoCode()";

  @Override
  public MatchCondition buildMatchCondition(
      final RequestContext requestContext, LeafCondition leafCondition) {
    RegionCondition regionCondition = leafCondition.getRegionCondition();

    ListValue.Builder countryIsoCodes = ListValue.newBuilder();
    regionCondition.getRegionIdentifiersList().stream()
        .map(RegionCondition.Region::getCountryIsoCode)
        .map(isoCode -> Value.newBuilder().setStringValue(isoCode).build())
        .forEach(countryIsoCodes::addValues);
    StructuredMatchCondition structuredMatchCondition =
        StructuredMatchCondition.newBuilder()
            .setLhs(
                AttributeDerivationMapping.newBuilder()
                    .setName("lhs")
                    .setType(FIELD_TYPE_STR)
                    .addRules(
                        DerivationRule.newBuilder()
                            .setTransformationConfig(
                                DataTransformationConfig.newBuilder()
                                    .setJexlExpression(
                                        JexlExpressionConfig.newBuilder()
                                            .setJexlExpression(COUNTRY_ISO_CODE_JEXL_EXP)))))
            .setBinaryOperator(
                BinaryOperator.newBuilder()
                    .setMatchOperator(
                        regionCondition.getExclude()
                            ? MatchOperator.MATCH_OPERATOR_NOT_IN
                            : MatchOperator.MATCH_OPERATOR_IN)
                    .setListValue(countryIsoCodes))
            .build();
    return MatchCondition.newBuilder()
        .setStructuredMatchCondition(structuredMatchCondition)
        .build();
  }

  @Override
  public LeafCondition.ConditionCase getConditionCase() {
    return LeafCondition.ConditionCase.REGION_CONDITION;
  }
}
