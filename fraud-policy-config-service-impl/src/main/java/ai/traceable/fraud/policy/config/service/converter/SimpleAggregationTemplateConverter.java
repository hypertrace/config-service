package ai.traceable.fraud.policy.config.service.converter;

import static ai.traceable.edge.decision.config.service.v1.EdgeInputKind.EDGE_INPUT_KIND_HTTP_REQUEST;
import static ai.traceable.fraud.policy.config.service.converter.EdgeDecisionConverterUtils.buildJexlSpanAttribute;
import static ai.traceable.fraud.policy.config.service.converter.EdgeDecisionConverterUtils.buildStaticSpanAttribute;
import static ai.traceable.fraud.policy.config.service.converter.EdgeDecisionConverterUtils.buildVariable;
import static ai.traceable.fraud.policy.config.service.converter.EdgeDecisionConverterUtils.buildVariableRefAttribute;
import static ai.traceable.fraud.policy.config.service.converter.EdgeDecisionConverterUtils.convertAggregationFunctionType;
import static ai.traceable.fraud.policy.config.service.converter.EdgeDecisionConverterUtils.convertThresholdOperator;
import static ai.traceable.fraud.policy.config.service.converter.JexlExpressionUtils.escapeJexlString;

import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.edge.decision.config.service.v1.AggregateThresholdRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleDefinition;
import ai.traceable.edge.decision.config.service.v1.SpanAttributeDecoration;
import ai.traceable.edge.decision.config.service.v1.ValueAggregateThreshold;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyData;
import ai.traceable.fraud.policy.config.service.v1.AbuseSimpleAggregationTemplateConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseThresholdConfig;
import com.google.protobuf.Duration;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Converts abuse policies with {@code simple_aggregation_template} into EDS rule components.
 *
 * <p>Handles entity ID collection, aggregate threshold rule building, and span attribute decoration
 * for the simple aggregation template type.
 */
@Slf4j
@Singleton
public class SimpleAggregationTemplateConverter implements TemplateEdgeDecisionConverter {

  private final DetectionFilterConverter detectionFilterConverter;

  @Inject
  public SimpleAggregationTemplateConverter(DetectionFilterConverter detectionFilterConverter) {
    this.detectionFilterConverter = detectionFilterConverter;
  }

  @Override
  public Set<String> getRequiredDerivedEntityIds(AbusePolicyData data) {
    Set<String> ids = new HashSet<>();
    AbuseSimpleAggregationTemplateConfig template = data.getSimpleAggregationTemplate();
    if (template.hasGroupBy() && !template.getGroupBy().getDerivedEntityId().isEmpty()) {
      ids.add(template.getGroupBy().getDerivedEntityId());
    }
    if (template.hasAggregation() && !template.getAggregation().getDerivedEntityId().isEmpty()) {
      ids.add(template.getAggregation().getDerivedEntityId());
    }
    template.getFiltersList().stream()
        .map(EdgeDecisionConverterUtils::getFilterDerivedEntityIds)
        .forEach(ids::addAll);
    return ids;
  }

  @Override
  public EdgeDecisionRuleDefinition buildRuleDefinition(
      AbusePolicyData data,
      Map<String, List<DerivationRule>> entityRulesMap,
      Map<String, String> entityVariableNames) {
    AbuseSimpleAggregationTemplateConfig template = data.getSimpleAggregationTemplate();

    EdgeDecisionRuleDefinition.Builder defBuilder =
        EdgeDecisionRuleDefinition.newBuilder().setEdgeInputKind(EDGE_INPUT_KIND_HTTP_REQUEST);

    // Only include entities required by this policy as rule_variables
    Set<String> requiredEntityIds = getRequiredDerivedEntityIds(data);
    requiredEntityIds.forEach(
        entityId -> {
          List<DerivationRule> rules = entityRulesMap.getOrDefault(entityId, List.of());
          if (!rules.isEmpty()) {
            String varName = entityVariableNames.getOrDefault(entityId, entityId);
            defBuilder.addRuleVariables(buildVariable(varName, rules));
          }
        });

    defBuilder.setAggregateThresholdRule(
        buildAggregateThresholdRule(template, entityRulesMap, entityVariableNames));

    return defBuilder.build();
  }

  @Override
  public List<SpanAttributeDecoration> buildSpanAttributes(
      AbusePolicyData data, Map<String, String> entityVariableNames) {
    AbuseSimpleAggregationTemplateConfig template = data.getSimpleAggregationTemplate();

    // detection_signature: "simple_aggregation - {policy_name} - {group_by_variable}"
    String escapedName = escapeJexlString(data.getName());
    String groupByEntityId = "";
    if (template.hasGroupBy() && !template.getGroupBy().getDerivedEntityId().isEmpty()) {
      groupByEntityId = template.getGroupBy().getDerivedEntityId();
    }
    String groupByVarName = entityVariableNames.getOrDefault(groupByEntityId, groupByEntityId);
    SpanAttributeDecoration detectionSignature =
        !groupByEntityId.isEmpty()
            ? buildJexlSpanAttribute(
                "detection_signature",
                "'simple_aggregation - " + escapedName + " - ' + " + groupByVarName)
            : buildStaticSpanAttribute(
                "detection_signature", "simple_aggregation - " + data.getName());

    return List.of(
        buildJexlSpanAttribute("actor", "$s.getIpAddress()"),
        buildStaticSpanAttribute("actor_type", "IP ADDRESS"),
        buildJexlSpanAttribute("account", "$context.get('enduser.id')"),
        buildStaticSpanAttribute("account_type", "USER ID"),
        detectionSignature);
  }

  private AggregateThresholdRule buildAggregateThresholdRule(
      AbuseSimpleAggregationTemplateConfig template,
      Map<String, List<DerivationRule>> entityRulesMap,
      Map<String, String> entityVariableNames) {
    AggregateThresholdRule.Builder builder = AggregateThresholdRule.newBuilder();

    // Group-by dimensions — reference variable name
    if (template.hasGroupBy() && !template.getGroupBy().getDerivedEntityId().isEmpty()) {
      String groupByEntityId = template.getGroupBy().getDerivedEntityId();
      if (entityRulesMap.containsKey(groupByEntityId)
          && !entityRulesMap.get(groupByEntityId).isEmpty()) {
        String varName = entityVariableNames.getOrDefault(groupByEntityId, groupByEntityId);
        builder.addGroupByDimensions(buildVariableRefAttribute(varName));
      } else {
        throw new IllegalStateException(
            "Group-by entity '"
                + groupByEntityId
                + "' has no valid derivation rules. "
                + "Cannot generate a rule without group-by — "
                + "it would create a shared counter blocking all requests.");
      }
    }

    // Value aggregate threshold
    builder.setValueAggregateThreshold(
        buildValueAggregateThreshold(template, entityRulesMap, entityVariableNames));

    // Time window
    if (template.hasTimeWindow() && template.getTimeWindow().hasLookbackDuration()) {
      Duration lookback = template.getTimeWindow().getLookbackDuration();
      builder.setTimeWindow(
          Duration.newBuilder()
              .setSeconds(lookback.getSeconds())
              .setNanos(lookback.getNanos())
              .build());
    }

    // Match condition from detection filters — filter entities are now variables
    if (!template.getFiltersList().isEmpty()) {
      detectionFilterConverter
          .convert(template.getFiltersList(), entityRulesMap, entityVariableNames)
          .ifPresent(builder::setMatchCondition);
    }

    return builder.build();
  }

  private ValueAggregateThreshold buildValueAggregateThreshold(
      AbuseSimpleAggregationTemplateConfig template,
      Map<String, List<DerivationRule>> entityRulesMap,
      Map<String, String> entityVariableNames) {
    ValueAggregateThreshold.Builder builder = ValueAggregateThreshold.newBuilder();

    builder.setAggregationType(
        convertAggregationFunctionType(template.getAggregation().getAggregationFunction()));

    // Set dimension — reference variable name for the aggregation entity
    String aggregationEntityId = template.getAggregation().getDerivedEntityId();
    boolean hasDimensionRules =
        !aggregationEntityId.isEmpty()
            && entityRulesMap.containsKey(aggregationEntityId)
            && !entityRulesMap.get(aggregationEntityId).isEmpty();

    if (hasDimensionRules) {
      String varName = entityVariableNames.getOrDefault(aggregationEntityId, aggregationEntityId);
      builder.setDimension(buildVariableRefAttribute(varName));
    } else if (!aggregationEntityId.isEmpty()) {
      throw new IllegalStateException(
          "Aggregation entity '"
              + aggregationEntityId
              + "' has no valid derivation rules. "
              + "Cannot generate a "
              + template.getAggregation().getAggregationFunction()
              + " rule without the aggregation entity.");
    }

    if (template.hasThreshold()) {
      AbuseThresholdConfig threshold = template.getThreshold();
      builder.setStaticThreshold(threshold.getValue());
      builder.setThresholdOperator(convertThresholdOperator(threshold.getOperator()));
    }

    return builder.build();
  }
}
