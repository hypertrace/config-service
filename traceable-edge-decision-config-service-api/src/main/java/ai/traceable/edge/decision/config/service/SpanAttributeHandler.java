package ai.traceable.edge.decision.config.service;

import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategory;
import ai.traceable.edge.decision.config.service.v1.SpanAttributeDecoration;
import com.google.protobuf.Value;
import java.util.List;

public class SpanAttributeHandler {
  private static final String TRACEABLEAI_PREFIX = "traceableai.blocked.";
  private static final String VIOLATIONS_KEYWORD = "violations_";
  private static final String EXEMPTIONS_KEYWORD = "exemptions_";
  private static final String CATEGORY_SUFFIX = ".category";
  private static final String INFO_SUFFIX = ".info";

  public static List<SpanAttributeDecoration> getSpanAttributeDecorations(
      String id,
      boolean isExemption,
      EdgeDecisionRuleCategory edgeDecisionRuleCategory,
      String info) {
    SpanAttributeDecoration categoryAttribute =
        SpanAttributeDecoration.newBuilder()
            .setSpanAttributeKey(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(
                        Value.newBuilder()
                            .setStringValue(getCategorySpanAttribute(id, isExemption)))
                    .setOutputType(FieldType.FIELD_TYPE_STR))
            .setSpanAttributeValue(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(
                        Value.newBuilder().setStringValue(edgeDecisionRuleCategory.toString()))
                    .setOutputType(FieldType.FIELD_TYPE_STR))
            .build();

    SpanAttributeDecoration infoAttribute =
        SpanAttributeDecoration.newBuilder()
            .setSpanAttributeKey(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(
                        Value.newBuilder().setStringValue(getInfoSpanAttribute(id, isExemption)))
                    .setOutputType(FieldType.FIELD_TYPE_STR))
            .setSpanAttributeValue(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(Value.newBuilder().setStringValue(info))
                    .setOutputType(FieldType.FIELD_TYPE_STR))
            .build();

    return List.of(categoryAttribute, infoAttribute);
  }

  private static String getCategorySpanAttribute(String id, boolean isExemption) {
    return TRACEABLEAI_PREFIX
        + (isExemption ? EXEMPTIONS_KEYWORD : VIOLATIONS_KEYWORD)
        + id
        + CATEGORY_SUFFIX;
  }

  private static String getInfoSpanAttribute(String id, boolean isExemption) {
    return TRACEABLEAI_PREFIX
        + (isExemption ? EXEMPTIONS_KEYWORD : VIOLATIONS_KEYWORD)
        + id
        + INFO_SUFFIX;
  }
}
