package ai.traceable.customsignature.config.service.rules.converter.expression;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.edge.decision.converter.utils.Constants.ATTRIBUTE_NAME_LHS;
import static ai.traceable.edge.decision.converter.utils.Constants.IP_ORGANISATION_JEXL_EXP;
import static ai.traceable.edge.decision.converter.utils.ConverterUtils.buildLikeOperatorMatchCondition;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.Clause.ClauseCase;
import ai.traceable.customsignature.config.service.v1.IpOrganisationExpression;
import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;

public class IpOrganisationExpressionConverter implements CustomSignatureExpressionConverter {
  @Override
  public MatchCondition buildMatchCondition(Clause clause) {
    IpOrganisationExpression ipOrganisationExpression = clause.getIpOrganisationExpression();

    AttributeDerivationMapping attributeDerivationMapping =
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
                                    .setJexlExpression(IP_ORGANISATION_JEXL_EXP))))
            .build();

    return buildLikeOperatorMatchCondition(
            attributeDerivationMapping, ipOrganisationExpression.getIpOrganisationRegexesList())
        .setNegate(ipOrganisationExpression.getExclude())
        .build();
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return ClauseCase.IP_ORGANISATION_EXPRESSION;
  }
}
