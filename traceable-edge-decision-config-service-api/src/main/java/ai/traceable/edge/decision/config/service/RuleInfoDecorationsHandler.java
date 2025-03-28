package ai.traceable.edge.decision.config.service;

import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.edge.decision.config.service.v1.RuleInfoDecoration;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RuleInfoDecorationsHandler {

  public static final String MATCHED_ATTRIBUTE = "matched_attribute";
  public static final String BLOCK_ACTION = "block_action";
  public static final String THRESHOLD_DETAILS = "threshold_details";
  public static final String SEVERITY = "severity";
  public static final String LABELS = "labels";
  public static final String ACTOR_ENTITY_ID = "actor_entity_id";

  public static List<RuleInfoDecoration> getRuleInfoDecorations(Map<String, String> decorations) {
    return decorations.entrySet().stream()
        .map(
            entry ->
                RuleInfoDecoration.newBuilder()
                    .setRuleInfoKey(
                        DataTransformationConfig.newBuilder()
                            .setStaticValue(Value.newBuilder().setStringValue(entry.getKey()))
                            .setOutputType(FieldType.FIELD_TYPE_STR))
                    .setRuleInfoValue(
                        DataTransformationConfig.newBuilder()
                            .setStaticValue(Value.newBuilder().setStringValue(entry.getValue()))
                            .setOutputType(FieldType.FIELD_TYPE_STR))
                    .build())
        .collect(Collectors.toList());
  }
}
