package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

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
