package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_INT;
import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.edge.decision.converter.utils.Constants.ATTRIBUTE_NAME_LHS;
import static ai.traceable.ratelimiting.service.v2.rules.ValidatorUtils.KEY_NULL_CONDITION_TYPES;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.RateLimitingConditionConverterUtils.ALL_MATCH_OPERATORS;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.RateLimitingConditionConverterUtils.INT_MATCH_OPERATORS;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.RateLimitingConditionConverterUtils.LIST_VALUE_MAP_TYPES;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.RateLimitingConditionConverterUtils.REGEX_MATCH_OPERATORS;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.RateLimitingConditionConverterUtils.getJexlExpForType;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.RateLimitingConditionConverterUtils.getJexlExpressionForKeyValueConditionType;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.RateLimitingConditionConverterUtils.getMatchOperator;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.RateLimitingConditionConverterUtils.getPredicateJexlExpression;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.BinaryOperator;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.datamodel.data.transformation.config.v1.GenericMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.StructuredMatchCondition;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import java.util.Collections;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingKeyValueConditionConverter implements RateLimitingConditionConverter {
  @Override
  public MatchConditionDetails buildMatchCondition(
      RequestContext requestContext, LeafCondition leafCondition) {
    KeyValueCondition keyValueCondition = leafCondition.getKeyValueCondition();
    if (keyValueCondition.hasLhsRhsCondition()) {
      return buildLhsRhsMatchCondition(keyValueCondition.getLhsRhsCondition());
    }
    return keyValueCondition.hasStaticValueCondition()
        ? buildMatchCondition(keyValueCondition)
        : buildDeprecatedMatchCondition(keyValueCondition);
  }

  @Override
  public LeafCondition.ConditionCase getConditionCase() {
    return LeafCondition.ConditionCase.KEY_VALUE_CONDITION;
  }

  MatchConditionDetails buildLhsRhsMatchCondition(
      KeyValueCondition.LhsRhsKeysCondition lhsRhsKeysCondition) {
    KeyValueCondition.KeyCondition lhsKeyCondition = lhsRhsKeysCondition.getLhsKeyCondition();
    KeyValueCondition.KeyCondition rhsKeyCondition = lhsRhsKeysCondition.getRhsKeyCondition();
    KeyValueCondition.MatchOperator lhsRhsMatchOperator =
        lhsRhsKeysCondition.getLhsRhsMatchOperator();
    KeyValueCondition.Type lhsKeyType = lhsKeyCondition.getKeyType();
    KeyValueCondition.Type rhsKeyType = rhsKeyCondition.getKeyType();

    String jexlExpression;

    if (KEY_NULL_CONDITION_TYPES.contains(lhsKeyType)
        && KEY_NULL_CONDITION_TYPES.contains(rhsKeyType)) {
      jexlExpression =
          String.format(
              "map:match(%s, %s, %s)",
              getValueForKeyNullTypes(lhsKeyType),
              getValueForKeyNullTypes(rhsKeyType),
              lhsRhsMatchOperator.name());

    } else if (KEY_NULL_CONDITION_TYPES.contains(lhsKeyType)) {
      String rhsMapJexlExpression = getJexlExpressionForKeyValueConditionType(rhsKeyType);
      KeyValueCondition.MatchOperatorCondition rhsMatchOperatorCondition =
          rhsKeyCondition.getKeyMatchOperatorCondition();
      KeyValueCondition.MatchOperator rhsOperator = rhsMatchOperatorCondition.getOperator();
      String rhsValue = rhsMatchOperatorCondition.getValue().getStringValue();
      String rhsPredicateJexlExpression = getPredicateJexlExpression(rhsOperator, rhsValue);

      jexlExpression =
          String.format(
              "map:match(%s, %s, %s)",
              getValueForKeyNullTypes(lhsKeyType),
              getExtractedValuesForKeyValueConditionTypes(
                  rhsMapJexlExpression, rhsPredicateJexlExpression),
              lhsRhsMatchOperator.name());

    } else if (KEY_NULL_CONDITION_TYPES.contains(rhsKeyType)) {
      String lhsMapJexlExpression = getJexlExpressionForKeyValueConditionType(lhsKeyType);
      KeyValueCondition.MatchOperatorCondition lhsMatchOperatorCondition =
          lhsKeyCondition.getKeyMatchOperatorCondition();
      KeyValueCondition.MatchOperator lhsOperator = lhsMatchOperatorCondition.getOperator();
      String lhsValue = lhsMatchOperatorCondition.getValue().getStringValue();
      String lhsPredicateJexlExpression = getPredicateJexlExpression(lhsOperator, lhsValue);

      jexlExpression =
          String.format(
              "map:match(%s, %s, %s)",
              getExtractedValuesForKeyValueConditionTypes(
                  lhsMapJexlExpression, lhsPredicateJexlExpression),
              getValueForKeyNullTypes(rhsKeyType),
              lhsRhsMatchOperator.name());

    } else {
      String lhsMapJexlExpression = getJexlExpressionForKeyValueConditionType(lhsKeyType);
      String rhsMapJexlExpression = getJexlExpressionForKeyValueConditionType(rhsKeyType);
      KeyValueCondition.MatchOperatorCondition lhsMatchOperatorCondition =
          lhsKeyCondition.getKeyMatchOperatorCondition();
      KeyValueCondition.MatchOperatorCondition rhsMatchOperatorCondition =
          rhsKeyCondition.getKeyMatchOperatorCondition();
      KeyValueCondition.MatchOperator lhsOperator = lhsMatchOperatorCondition.getOperator();
      KeyValueCondition.MatchOperator rhsOperator = rhsMatchOperatorCondition.getOperator();
      String lhsValue = lhsMatchOperatorCondition.getValue().getStringValue();
      String rhsValue = rhsMatchOperatorCondition.getValue().getStringValue();
      String lhsPredicateJexlExpression = getPredicateJexlExpression(lhsOperator, lhsValue);
      String rhsPredicateJexlExpression = getPredicateJexlExpression(rhsOperator, rhsValue);

      jexlExpression =
          String.format(
              "map:match(%s, %s, %s)",
              getExtractedValuesForKeyValueConditionTypes(
                  lhsMapJexlExpression, lhsPredicateJexlExpression),
              getExtractedValuesForKeyValueConditionTypes(
                  rhsMapJexlExpression, rhsPredicateJexlExpression),
              lhsRhsMatchOperator.name());
    }

    return new MatchConditionDetails(
        MatchCondition.newBuilder()
            .setGenericMatchCondition(
                GenericMatchCondition.newBuilder()
                    .setJexlExpression(
                        JexlExpressionConfig.newBuilder().setJexlExpression(jexlExpression)))
            .build(),
        Collections.emptyList(),
        Collections.emptyList());
  }

  MatchConditionDetails buildMatchCondition(KeyValueCondition keyValueCondition) {
    KeyValueCondition.StaticValueCondition staticValueCondition =
        keyValueCondition.getStaticValueCondition();
    KeyValueCondition.Type type = staticValueCondition.getKeyCondition().getKeyType();
    boolean hasKeyCondition = staticValueCondition.getKeyCondition().hasKeyMatchOperatorCondition();
    boolean hasValueCondition = staticValueCondition.hasValueMatchOperatorCondition();
    KeyValueCondition.MatchOperator keyOperator =
        hasKeyCondition
            ? staticValueCondition.getKeyCondition().getKeyMatchOperatorCondition().getOperator()
            : null;
    KeyValueCondition.MatchOperator valueOperator =
        hasValueCondition
            ? staticValueCondition.getValueMatchOperatorCondition().getOperator()
            : null;
    String value =
        hasValueCondition
            ? staticValueCondition.getValueMatchOperatorCondition().getValue().getStringValue()
            : null;
    String key =
        hasKeyCondition
            ? staticValueCondition
                .getKeyCondition()
                .getKeyMatchOperatorCondition()
                .getValue()
                .getStringValue()
            : null;
    return buildConditionalMatchOperatorCondition(
        type, hasKeyCondition, hasValueCondition, keyOperator, valueOperator, key, value);
  }

  @Deprecated
  MatchConditionDetails buildDeprecatedMatchCondition(KeyValueCondition keyValueCondition) {
    KeyValueCondition.Type type = keyValueCondition.getType();
    boolean hasKeyCondition = keyValueCondition.hasKeyCondition();
    boolean hasValueCondition = keyValueCondition.hasValueCondition();
    KeyValueCondition.MatchOperator keyOperator =
        hasKeyCondition ? keyValueCondition.getKeyCondition().getOperator() : null;
    KeyValueCondition.MatchOperator valueOperator =
        hasValueCondition ? keyValueCondition.getValueCondition().getOperator() : null;
    String value = hasValueCondition ? keyValueCondition.getValueCondition().getValue() : null;
    String key = hasKeyCondition ? keyValueCondition.getKeyCondition().getValue() : null;
    return buildConditionalMatchOperatorCondition(
        type, hasKeyCondition, hasValueCondition, keyOperator, valueOperator, key, value);
  }

  MatchConditionDetails buildConditionalMatchOperatorCondition(
      KeyValueCondition.Type type,
      boolean hasKeyCondition,
      boolean hasValueCondition,
      KeyValueCondition.MatchOperator keyOperator,
      KeyValueCondition.MatchOperator valueOperator,
      String key,
      String value) {
    if (hasValueCondition && KEY_NULL_CONDITION_TYPES.contains(type)) {
      BinaryOperator.Builder builder =
          BinaryOperator.newBuilder().setMatchOperator(getMatchOperator(type, valueOperator));
      FieldType fieldType = FIELD_TYPE_STR;
      if (INT_MATCH_OPERATORS.contains(valueOperator)) {
        builder.setNumberValue(Double.parseDouble(value));
        fieldType = FIELD_TYPE_INT;
      } else if (REGEX_MATCH_OPERATORS.contains(valueOperator)) {
        builder.setRegex(value);
      } else {
        builder.setStringValue(value);
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
                                      .setOutputType(fieldType)
                                      .setJexlExpression(
                                          JexlExpressionConfig.newBuilder()
                                              .setJexlExpression(getJexlExpForType(type))))))
              .setBinaryOperator(builder)
              .build();
      MatchCondition.Builder matchConditionBuilder =
          MatchCondition.newBuilder().setStructuredMatchCondition(structuredMatchCondition);
      // no first class support of not contains currently
      if (valueOperator.equals(KeyValueCondition.MatchOperator.MATCH_OPERATOR_NOT_CONTAIN)) {
        matchConditionBuilder.setNegate(true);
      }
      return new MatchConditionDetails(
          matchConditionBuilder.build(), Collections.emptyList(), Collections.emptyList());
    } else {
      // types supporting both key and value condition are stored as Map<String, String> or
      // Map<String, List<String>> in the edge-decision-service
      String jexlExp = "";
      if (hasKeyCondition && hasValueCondition) {
        jexlExp =
            LIST_VALUE_MAP_TYPES.contains(type)
                ? String.format(
                    "map:match(%s, %s, %s, %s)",
                    getJexlExpForType(type),
                    getPredicateJexlExpression(keyOperator, key),
                    getPredicateJexlExpression(valueOperator, value),
                    ALL_MATCH_OPERATORS.contains(valueOperator))
                : String.format(
                    "map:match(%s, %s, %s)",
                    getJexlExpForType(type),
                    getPredicateJexlExpression(keyOperator, key),
                    getPredicateJexlExpression(valueOperator, value));
      } else if (hasKeyCondition) {
        jexlExp =
            String.format(
                "map:match(%s, %s, %s)",
                getJexlExpForType(type),
                getPredicateJexlExpression(keyOperator, key),
                ALL_MATCH_OPERATORS.contains(keyOperator));
      }
      return new MatchConditionDetails(
          MatchCondition.newBuilder()
              .setGenericMatchCondition(
                  GenericMatchCondition.newBuilder()
                      .setJexlExpression(
                          JexlExpressionConfig.newBuilder().setJexlExpression(jexlExp)))
              .build(),
          Collections.emptyList(),
          Collections.emptyList());
    }
  }

  private String getExtractedValuesForKeyValueConditionTypes(
      String mapJexlExpression, String predicateJexlExpression) {
    return "map:extractValues(" + mapJexlExpression + ", " + predicateJexlExpression + ")";
  }

  private String getValueForKeyNullTypes(KeyValueCondition.Type type) {
    return "Set.of(" + getJexlExpForType(type) + ")";
  }
}
