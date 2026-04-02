package ai.traceable.customsignature.config.service.rules.converter.expression;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.edge.decision.converter.utils.Constants.ATTRIBUTE_NAME_LHS;
import static ai.traceable.edge.decision.converter.utils.Constants.COUNTRY_ISO_CODE_JEXL_EXP;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.Clause.ClauseCase;
import ai.traceable.customsignature.config.service.v1.RegionExpression;
import ai.traceable.customsignature.config.service.v1.RegionIdentifier;
import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.BinaryOperator;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.MatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.StructuredMatchCondition;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;

public class RegionExpressionConverter implements CustomSignatureExpressionConverter {

  @Override
  public MatchCondition buildMatchCondition(Clause clause) {
    RegionExpression regionExpression = clause.getRegionExpression();

    // TODO(AAP-11983): Support state/city region matching for edge conversion.
    ListValue.Builder countryIsoCodes = ListValue.newBuilder();
    regionExpression.getRegionsList().stream()
        .filter(RegionIdentifier::hasCountry)
        .map(regionIdentifier -> regionIdentifier.getCountry().getIsoCode())
        .filter(isoCode -> !isoCode.isEmpty())
        .map(isoCode -> Value.newBuilder().setStringValue(isoCode).build())
        .forEach(countryIsoCodes::addValues);
    StructuredMatchCondition structuredMatchCondition =
        StructuredMatchCondition.newBuilder()
            .setLhs(
                AttributeDerivationMapping.newBuilder()
                    .setName(ATTRIBUTE_NAME_LHS)
                    .setType(FIELD_TYPE_STR)
                    .addRules(
                        DerivationRule.newBuilder()
                            .setTransformationConfig(
                                DataTransformationConfig.newBuilder()
                                    .setOutputType(FIELD_TYPE_STR)
                                    .setJexlExpression(
                                        JexlExpressionConfig.newBuilder()
                                            .setJexlExpression(COUNTRY_ISO_CODE_JEXL_EXP)))))
            .setBinaryOperator(
                BinaryOperator.newBuilder()
                    .setMatchOperator(
                        regionExpression.getExclude()
                            ? MatchOperator.MATCH_OPERATOR_NOT_IN
                            : MatchOperator.MATCH_OPERATOR_IN)
                    .setListValue(countryIsoCodes))
            .build();
    return MatchCondition.newBuilder()
        .setStructuredMatchCondition(structuredMatchCondition)
        .build();
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return ClauseCase.REGION_EXPRESSION;
  }
}
