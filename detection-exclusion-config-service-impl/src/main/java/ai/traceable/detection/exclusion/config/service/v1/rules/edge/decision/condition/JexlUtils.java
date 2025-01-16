package ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision.condition;

import ai.traceable.datamodel.data.transformation.config.v1.GenericMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;

public class JexlUtils {

  public static MatchCondition getMatchCondition(String jexlExpression) {
    return MatchCondition.newBuilder()
        .setGenericMatchCondition(
            GenericMatchCondition.newBuilder()
                .setJexlExpression(
                    JexlExpressionConfig.newBuilder().setJexlExpression(jexlExpression)))
        .build();
  }

  public static String getNotJexlExpression(String jexlExpression) {
    return String.format("!(%s)", jexlExpression);
  }
}
