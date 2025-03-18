package ai.traceable.edge.decision.config.service;

import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategory;
import ai.traceable.edge.decision.config.service.v1.SpanAttributeDecoration;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SpanAttributeHandler {
  private static final String TRACEABLEAI_PREFIX = "traceableai.blocked.";
  private static final String VIOLATIONS_KEYWORD = "violations_";
  private static final String EXEMPTIONS_KEYWORD = "exemptions_";
  private static final String CATEGORY_SUFFIX = ".category";
  private static final String INFO_SUFFIX = ".info";
  private static final String THRESHOLD_DETAILS_SUFFIX = ".thresholdDetails";
  private static final String MATCHED_ATTRIBUTE_SUFFIX = ".matched_attribute";
  private static final String BLOCK_ACTION_SUFFIX = ".block_action";

  public static List<SpanAttributeDecoration> getSpanAttributeDecorations(
      String id,
      boolean isExemption,
      EdgeDecisionRuleCategory edgeDecisionRuleCategory,
      String info,
      String thresholdDetails,
      String matchedAttribute,
      Optional<String> blockAction) {
    List<SpanAttributeDecoration> spanAttributeDecorations =
        new ArrayList<>(
            getSpanAttributeDecorations(id, isExemption, edgeDecisionRuleCategory, info));

    SpanAttributeDecoration thresholdDetailsAttribute =
        SpanAttributeDecoration.newBuilder()
            .setSpanAttributeKey(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(
                        Value.newBuilder()
                            .setStringValue(getThresholdDetailsSpanAttribute(id, isExemption)))
                    .setOutputType(FieldType.FIELD_TYPE_STR))
            .setSpanAttributeValue(
                DataTransformationConfig.newBuilder()
                    .setStaticValue(Value.newBuilder().setStringValue(thresholdDetails))
                    .setOutputType(FieldType.FIELD_TYPE_STR))
            .build();
    spanAttributeDecorations.add(thresholdDetailsAttribute);
    if (matchedAttribute != null && !matchedAttribute.isBlank()) {
      SpanAttributeDecoration spanMatchedAttribute =
          SpanAttributeDecoration.newBuilder()
              .setSpanAttributeKey(
                  DataTransformationConfig.newBuilder()
                      .setStaticValue(
                          Value.newBuilder().setStringValue(getSpanMatchedAttribute(id)))
                      .setOutputType(FieldType.FIELD_TYPE_STR))
              .setSpanAttributeValue(
                  DataTransformationConfig.newBuilder()
                      .setStaticValue(Value.newBuilder().setStringValue(matchedAttribute))
                      .setOutputType(FieldType.FIELD_TYPE_STR))
              .build();
      spanAttributeDecorations.add(spanMatchedAttribute);
    }

    if (blockAction.isPresent()) {
      SpanAttributeDecoration blockActionAttribute =
          SpanAttributeDecoration.newBuilder()
              .setSpanAttributeKey(
                  DataTransformationConfig.newBuilder()
                      .setStaticValue(
                          Value.newBuilder().setStringValue(getBlockActionAttribute(id)))
                      .setOutputType(FieldType.FIELD_TYPE_STR))
              .setSpanAttributeValue(
                  DataTransformationConfig.newBuilder()
                      .setStaticValue(Value.newBuilder().setStringValue(blockAction.get()))
                      .setOutputType(FieldType.FIELD_TYPE_STR))
              .build();
      spanAttributeDecorations.add(blockActionAttribute);
    }

    return spanAttributeDecorations;
  }

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

  private static String getThresholdDetailsSpanAttribute(String id, boolean isExemption) {
    return TRACEABLEAI_PREFIX
        + (isExemption ? EXEMPTIONS_KEYWORD : VIOLATIONS_KEYWORD)
        + id
        + THRESHOLD_DETAILS_SUFFIX;
  }

  private static String getSpanMatchedAttribute(String id) {
    return TRACEABLEAI_PREFIX + VIOLATIONS_KEYWORD + id + MATCHED_ATTRIBUTE_SUFFIX;
  }

  private static String getBlockActionAttribute(String id) {
    return TRACEABLEAI_PREFIX + VIOLATIONS_KEYWORD + id + BLOCK_ACTION_SUFFIX;
  }
}
