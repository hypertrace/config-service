package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.ConverterUtils.buildLikeOperatorMatchCondition;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.ratelimiting.config.service.v2.IpOrganisationCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingIpOrganisationConditionConverter
    implements RateLimitingConditionConverter {

  private static final String IP_ORGANISATION_JEXL_EXP =
      "$s.getIpIntelligenceData().getIspData().getOrganisation()";

  @Override
  public MatchCondition buildMatchCondition(
      RequestContext requestContext, LeafCondition leafCondition) {
    IpOrganisationCondition ipOrganisationCondition = leafCondition.getIpOrganisationCondition();

    AttributeDerivationMapping attributeDerivationMapping =
        AttributeDerivationMapping.newBuilder()
            .setName("lhs")
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
            attributeDerivationMapping, ipOrganisationCondition.getIpOrganisationRegexesList())
        .setNegate(ipOrganisationCondition.getExclude())
        .build();
  }

  @Override
  public LeafCondition.ConditionCase getConditionCase() {
    return LeafCondition.ConditionCase.IP_ORGANISATION_CONDITION;
  }
}
