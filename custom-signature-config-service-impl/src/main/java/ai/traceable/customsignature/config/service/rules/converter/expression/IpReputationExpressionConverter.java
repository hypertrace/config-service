package ai.traceable.customsignature.config.service.rules.converter.expression;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.edge.decision.converter.utils.Constants.ATTRIBUTE_NAME_LHS;
import static ai.traceable.edge.decision.converter.utils.Constants.IP_REPUTATION_LEVEL_JEXL_EXP;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.Clause.ClauseCase;
import ai.traceable.customsignature.config.service.v1.IpReputationExpression;
import ai.traceable.customsignature.config.service.v1.IpReputationSeverity;
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
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.List;

public class IpReputationExpressionConverter implements CustomSignatureExpressionConverter {
  @Override
  public MatchCondition buildMatchCondition(Clause clause) {
    IpReputationExpression ipReputationExpression = clause.getIpReputationExpression();

    ListValue.Builder applicableIpReputationSeverities = ListValue.newBuilder();
    getApplicableIpReputationSeverities(ipReputationExpression.getMinIpReputationSeverity())
        .stream()
        .map(ipReputationSeverity -> Value.newBuilder().setStringValue(ipReputationSeverity))
        .forEach(applicableIpReputationSeverities::addValues);

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

  @Override
  public Clause.ClauseCase getClauseCase() {
    return ClauseCase.IP_REPUTATION_EXPRESSION;
  }

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
}
