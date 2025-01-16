package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.ConverterUtils.buildLikeOperatorMatchCondition;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.ratelimiting.config.service.v2.IpAsnCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingIpAsnConditionConverter implements RateLimitingConditionConverter {

  private static final String IP_ASN_JEXL_EXP = "$s.getIpIntelligenceData().getNetwork().getAsn()";

  @Override
  public MatchCondition buildMatchCondition(
      RequestContext requestContext, LeafCondition leafCondition) {
    IpAsnCondition ipAsnCondition = leafCondition.getIpAsnCondition();

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
                                    .setJexlExpression(IP_ASN_JEXL_EXP))))
            .build();

    return buildLikeOperatorMatchCondition(
            attributeDerivationMapping, ipAsnCondition.getIpAsnRegexesList())
        .setNegate(ipAsnCondition.getExclude())
        .build();
  }

  @Override
  public LeafCondition.ConditionCase getConditionCase() {
    return LeafCondition.ConditionCase.IP_ASN_CONDITION;
  }
}
