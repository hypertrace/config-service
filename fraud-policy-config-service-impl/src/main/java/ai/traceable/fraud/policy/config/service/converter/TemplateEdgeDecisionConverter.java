package ai.traceable.fraud.policy.config.service.converter;

import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleDefinition;
import ai.traceable.edge.decision.config.service.v1.SpanAttributeDecoration;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyData;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interface for converting a specific abuse policy template type into EDS rule components.
 *
 * <p>Each template type (simple_aggregation, predefined, etc.) should have its own implementation.
 */
public interface TemplateEdgeDecisionConverter {

  /** Returns all derived entity IDs required by this template for batch JEXL resolution. */
  Set<String> getRequiredDerivedEntityIds(AbusePolicyData data);

  /** Builds the EdgeDecisionRuleDefinition for this template type. */
  EdgeDecisionRuleDefinition buildRuleDefinition(
      AbusePolicyData data,
      Map<String, List<DerivationRule>> entityRulesMap,
      Map<String, String> entityVariableNames);

  /** Builds span attribute decorations specific to this template type. */
  List<SpanAttributeDecoration> buildSpanAttributes(
      AbusePolicyData data, Map<String, String> entityVariableNames);
}
