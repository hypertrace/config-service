package ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision.condition;

import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_QUERY_PARAMETER;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_REQUEST_BODY_PARAMETER;
import static ai.traceable.edge.decision.converter.utils.Constants.ATTRIBUTE_NAME_LHS;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.BinaryOperator;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.datamodel.data.transformation.config.v1.GenericMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.StructuredMatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.KeyMetadata;
import ai.traceable.detection.exclusion.config.service.v1.SpanAttributeMatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionConditionValidator;
import java.util.Set;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DetectionExclusionRuleAttributeMatchConditionConverter
    implements DetectionExclusionRuleConditionConverter {
  private static final Set<KeyMetadata> LIST_VALUE_MAP_KEY_METADATA_TYPES =
      Set.of(KEY_METADATA_QUERY_PARAMETER, KEY_METADATA_REQUEST_BODY_PARAMETER);
  private static final Set<ai.traceable.detection.exclusion.config.service.v1.MatchOperator>
      INT_MATCH_OPERATORS =
          Set.of(
              ai.traceable.detection.exclusion.config.service.v1.MatchOperator
                  .MATCH_OPERATOR_GREATER_THAN,
              ai.traceable.detection.exclusion.config.service.v1.MatchOperator
                  .MATCH_OPERATOR_LESS_THAN);
  private static final Set<ai.traceable.detection.exclusion.config.service.v1.MatchOperator>
      REGEX_MATCH_OPERATORS =
          Set.of(
              ai.traceable.detection.exclusion.config.service.v1.MatchOperator
                  .MATCH_OPERATOR_MATCHES_REGEX,
              ai.traceable.detection.exclusion.config.service.v1.MatchOperator
                  .MATCH_OPERATOR_NOT_MATCH_REGEX);
  private static final Set<ai.traceable.detection.exclusion.config.service.v1.MatchOperator>
      ALL_MATCH_OPERATORS =
          Set.of(
              ai.traceable.detection.exclusion.config.service.v1.MatchOperator
                  .MATCH_OPERATOR_NOT_EQUAL,
              ai.traceable.detection.exclusion.config.service.v1.MatchOperator
                  .MATCH_OPERATOR_NOT_MATCH_REGEX,
              ai.traceable.detection.exclusion.config.service.v1.MatchOperator
                  .MATCH_OPERATOR_GREATER_THAN,
              ai.traceable.detection.exclusion.config.service.v1.MatchOperator
                  .MATCH_OPERATOR_LESS_THAN);

  @Override
  public MatchCondition buildMatchCondition(
      RequestContext requestContext, DetectionExclusionCondition condition) {
    SpanAttributeMatchCondition attributeMatchCondition = condition.getAttributeMatchCondition();
    KeyMetadata keyMetadata = attributeMatchCondition.getKeyMatchCondition().getMetadata();
    if (DetectionExclusionConditionValidator.KEY_NULL_METADATA.contains(keyMetadata)) {
      ai.traceable.detection.exclusion.config.service.v1.MatchCondition matchCondition =
          attributeMatchCondition.getValueMatchCondition();
      boolean isKeyMetadataCaseInsensitive =
          DetectionExclusionRuleConditionConverterUtils.isKeyMetadataCaseInsensitive(keyMetadata);
      BinaryOperator.Builder builder =
          BinaryOperator.newBuilder()
              .setMatchOperator(
                  DetectionExclusionRuleConditionConverterUtils.getMatchOperator(
                      isKeyMetadataCaseInsensitive, matchCondition.getOperator()));
      FieldType fieldType = FieldType.FIELD_TYPE_STR;
      if (INT_MATCH_OPERATORS.contains(matchCondition.getOperator())) {
        builder.setNumberValue(Double.parseDouble(matchCondition.getValue().getStringValue()));
        fieldType = FieldType.FIELD_TYPE_INT;
      } else if (REGEX_MATCH_OPERATORS.contains(matchCondition.getOperator())) {
        builder.setRegex(matchCondition.getValue().getStringValue());
      } else {
        builder.setStringValue(matchCondition.getValue().getStringValue());
      }
      StructuredMatchCondition structuredMatchCondition =
          StructuredMatchCondition.newBuilder()
              .setLhs(
                  AttributeDerivationMapping.newBuilder()
                      .setName(ATTRIBUTE_NAME_LHS)
                      .setType(fieldType)
                      .addRules(
                          DerivationRule.newBuilder()
                              .setTransformationConfig(
                                  DataTransformationConfig.newBuilder()
                                      .setJexlExpression(
                                          JexlExpressionConfig.newBuilder()
                                              .setJexlExpression(
                                                  DetectionExclusionRuleConditionConverterUtils
                                                      .getJexlExpressionForAttributeMatchConditionKeyMetadata(
                                                          keyMetadata))))))
              .setBinaryOperator(builder)
              .build();
      return MatchCondition.newBuilder()
          .setStructuredMatchCondition(structuredMatchCondition)
          .build();
    } else {
      // types supporting both key and value condition are stored as Map<String, String> or
      // Map<String, List<String>> in the edge-decision-service
      String jexlExp;
      if (attributeMatchCondition.getKeyMatchCondition().hasMatchCondition()
          && attributeMatchCondition.hasValueMatchCondition()) {
        jexlExp =
            LIST_VALUE_MAP_KEY_METADATA_TYPES.contains(keyMetadata)
                ? String.format(
                    "map:match(%s, %s, %s, %s)",
                    DetectionExclusionRuleConditionConverterUtils
                        .getJexlExpressionForAttributeMatchConditionKeyMetadata(keyMetadata),
                    DetectionExclusionRuleConditionConverterUtils.getPredicateJexlExpression(
                        attributeMatchCondition.getKeyMatchCondition().getMatchCondition()),
                    DetectionExclusionRuleConditionConverterUtils.getPredicateJexlExpression(
                        attributeMatchCondition.getValueMatchCondition()),
                    ALL_MATCH_OPERATORS.contains(
                        attributeMatchCondition.getValueMatchCondition().getOperator()))
                : String.format(
                    "map:match(%s, %s, %s)",
                    DetectionExclusionRuleConditionConverterUtils
                        .getJexlExpressionForAttributeMatchConditionKeyMetadata(keyMetadata),
                    DetectionExclusionRuleConditionConverterUtils.getPredicateJexlExpression(
                        attributeMatchCondition.getKeyMatchCondition().getMatchCondition()),
                    DetectionExclusionRuleConditionConverterUtils.getPredicateJexlExpression(
                        attributeMatchCondition.getValueMatchCondition()));
      } else {
        jexlExp =
            String.format(
                "map:match(%s, %s, %s)",
                DetectionExclusionRuleConditionConverterUtils
                    .getJexlExpressionForAttributeMatchConditionKeyMetadata(keyMetadata),
                DetectionExclusionRuleConditionConverterUtils.getPredicateJexlExpression(
                    attributeMatchCondition.getKeyMatchCondition().getMatchCondition()),
                ALL_MATCH_OPERATORS.contains(
                    attributeMatchCondition
                        .getKeyMatchCondition()
                        .getMatchCondition()
                        .getOperator()));
      }
      return MatchCondition.newBuilder()
          .setGenericMatchCondition(
              GenericMatchCondition.newBuilder()
                  .setJexlExpression(JexlExpressionConfig.newBuilder().setJexlExpression(jexlExp)))
          .build();
    }
  }

  @Override
  public DetectionExclusionCondition.ConditionCase getConditionCase() {
    return DetectionExclusionCondition.ConditionCase.ATTRIBUTE_MATCH_CONDITION;
  }
}
