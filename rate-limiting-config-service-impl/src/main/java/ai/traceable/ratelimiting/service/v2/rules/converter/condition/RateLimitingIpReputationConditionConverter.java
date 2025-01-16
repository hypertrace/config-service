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
import ai.traceable.platform.traceenricher.constants.TraceEnricherConstants;
import ai.traceable.platform.traceenricher.constants.v1.IpReputationLevel;
import ai.traceable.ratelimiting.config.service.v2.IpReputationCondition;
import ai.traceable.ratelimiting.config.service.v2.IpReputationSeverity;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingIpReputationConditionConverter implements RateLimitingConditionConverter {

  private static final String IP_REPUTATION_LEVEL_JEXL_EXP =
      "$s.getIpIntelligenceData().getIpReputationLevel()";

  @Override
  public MatchCondition buildMatchCondition(
      RequestContext requestContext, LeafCondition leafCondition) {
    IpReputationCondition ipReputationCondition = leafCondition.getIpReputationCondition();

    ListValue.Builder applicableIpReputationSeverities = ListValue.newBuilder();
    getApplicableIpReputationSeverities(ipReputationCondition.getMinIpReputationSeverity()).stream()
        .map(ipReputationSeverity -> Value.newBuilder().setStringValue(ipReputationSeverity))
        .forEach(applicableIpReputationSeverities::addValues);

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
                                    .setOutputType(FIELD_TYPE_STR)
                                    .setJexlExpression(
                                        JexlExpressionConfig.newBuilder()
                                            .setJexlExpression(IP_REPUTATION_LEVEL_JEXL_EXP)))))
            .setBinaryOperator(
                BinaryOperator.newBuilder()
                    .setMatchOperator(MatchOperator.MATCH_OPERATOR_IN)
                    .setListValue(applicableIpReputationSeverities))
            .build();
    return MatchCondition.newBuilder()
        .setStructuredMatchCondition(structuredMatchCondition)
        .build();
  }

  /**
   * @param minIpReputationSeverity min ip reputation severity applicable in the rule
   * @return list of applicable ip reputation severities corresponding to the
   *     minIpReputationSeverity, for eg. minIpReputationSeverity of low translates to low, medium,
   *     high and critical as applicable severities
   */
  private List<String> getApplicableIpReputationSeverities(
      IpReputationSeverity minIpReputationSeverity) {
    List<String> applicableIpReputationSeverities = new ArrayList<>();
    switch (minIpReputationSeverity) {
      case IP_REPUTATION_SEVERITY_LOW:
        applicableIpReputationSeverities.add(
            TraceEnricherConstants.getValue(IpReputationLevel.IP_REPUTATION_LEVEL_TYPE_LOW));
      case IP_REPUTATION_SEVERITY_MEDIUM:
        applicableIpReputationSeverities.add(
            TraceEnricherConstants.getValue(IpReputationLevel.IP_REPUTATION_LEVEL_TYPE_MEDIUM));
      case IP_REPUTATION_SEVERITY_HIGH:
        applicableIpReputationSeverities.add(
            TraceEnricherConstants.getValue(IpReputationLevel.IP_REPUTATION_LEVEL_TYPE_HIGH));
      case IP_REPUTATION_SEVERITY_CRITICAL:
        applicableIpReputationSeverities.add(
            TraceEnricherConstants.getValue(IpReputationLevel.IP_REPUTATION_LEVEL_TYPE_CRITICAL));
        break;
      default:
        throw new IllegalArgumentException(
            "Invalid ipReputationSeverity : " + minIpReputationSeverity);
    }
    return applicableIpReputationSeverities;
  }

  @Override
  public LeafCondition.ConditionCase getConditionCase() {
    return LeafCondition.ConditionCase.IP_REPUTATION_CONDITION;
  }
}
