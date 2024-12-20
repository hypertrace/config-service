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
import ai.traceable.ratelimiting.config.service.v2.IpOrganisationCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingIpOrganisationConditionConverter
    implements RateLimitingConditionConverter {

  private static final String IP_ORGANISATION_JEXL_EXP =
      "$s.getIpIntelligenceData().getIspData().getOrganisation()";

  @Override
  public MatchCondition buildMatchCondition(
      RequestContext requestContext, LeafCondition leafCondition) {
    IpOrganisationCondition ipOrganisationCondition = leafCondition.getIpOrganisationCondition();

    ListValue.Builder ipOrganisationRegexes = ListValue.newBuilder();
    ipOrganisationCondition.getIpOrganisationRegexesList().stream()
        .map(ipOrg -> Value.newBuilder().setStringValue(ipOrg))
        .forEach(ipOrganisationRegexes::addValues);

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
                                            .setJexlExpression(IP_ORGANISATION_JEXL_EXP)))))
            .setBinaryOperator(
                BinaryOperator.newBuilder()
                    .setMatchOperator(
                        ipOrganisationCondition.getExclude()
                            ? MatchOperator.MATCH_OPERATOR_NOT_LIKE
                            : MatchOperator.MATCH_OPERATOR_LIKE)
                    .setListValue(ipOrganisationRegexes))
            .build();
    return MatchCondition.newBuilder()
        .setStructuredMatchCondition(structuredMatchCondition)
        .build();
  }

  @Override
  public LeafCondition.ConditionCase getConditionCase() {
    return LeafCondition.ConditionCase.IP_ORGANISATION_CONDITION;
  }
}
