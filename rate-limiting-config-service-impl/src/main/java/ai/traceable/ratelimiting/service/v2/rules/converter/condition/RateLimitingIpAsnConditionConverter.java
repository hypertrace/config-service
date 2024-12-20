package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.edge.decision.config.service.v1.BinaryOperator;
import ai.traceable.edge.decision.config.service.v1.MatchCondition;
import ai.traceable.edge.decision.config.service.v1.MatchOperator;
import ai.traceable.edge.decision.config.service.v1.StructuredMatchCondition;
import ai.traceable.ratelimiting.config.service.v2.IpAsnCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingIpAsnConditionConverter implements RateLimitingConditionConverter {

  private static final String IP_ASN_JEXL_EXP = "$s.getIpIntelligenceData().getNetwork().getAsn()";

  @Override
  public MatchCondition buildMatchCondition(
      RequestContext requestContext, LeafCondition leafCondition) {
    IpAsnCondition ipAsnCondition = leafCondition.getIpAsnCondition();

    ListValue.Builder ipAsnRegexes = ListValue.newBuilder();
    ipAsnCondition.getIpAsnRegexesList().stream()
        .map(ipAsn -> Value.newBuilder().setStringValue(ipAsn))
        .forEach(ipAsnRegexes::addValues);

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
                                            .setJexlExpression(IP_ASN_JEXL_EXP)))))
            .setBinaryOperator(
                BinaryOperator.newBuilder()
                    .setMatchOperator(
                        ipAsnCondition.getExclude()
                            ? MatchOperator.MATCH_OPERATOR_NOT_LIKE
                            : MatchOperator.MATCH_OPERATOR_LIKE)
                    .setListValue(ipAsnRegexes))
            .build();
    return MatchCondition.newBuilder()
        .setStructuredMatchCondition(structuredMatchCondition)
        .build();
  }

  @Override
  public LeafCondition.ConditionCase getConditionCase() {
    return LeafCondition.ConditionCase.IP_ASN_CONDITION;
  }
}
