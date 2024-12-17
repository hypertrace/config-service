package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher;

import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;

public class AgentAttributeJexlGenerator {
  public DerivationRule generateJexlExpression(AttributeRule rule) {
    return DerivationRule.newBuilder()
        .setTransformationConfig(
            DataTransformationConfig.newBuilder()
                .setJexlExpression(
                    JexlExpressionConfig.newBuilder()
                        .setJexlExpression("$s.getHeaders().get('authorization')"))
                .setOutputType(FieldType.FIELD_TYPE_STR))
        .setConditionExpression(JexlExpressionConfig.newBuilder().build())
        .build();
  }
}
