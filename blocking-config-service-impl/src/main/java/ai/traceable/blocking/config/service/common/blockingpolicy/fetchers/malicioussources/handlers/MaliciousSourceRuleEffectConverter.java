package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.malicioussources.handlers;

import ai.traceable.blocking.config.service.v2.AttributeScope;
import ai.traceable.blocking.config.service.v2.InlineModification;
import ai.traceable.blocking.config.service.v2.RuleAction;
import ai.traceable.malicioussources.config.service.v1.AgentModification;
import ai.traceable.malicioussources.config.service.v1.AgentRuleEffect;
import ai.traceable.malicioussources.config.service.v1.FieldValue;
import ai.traceable.malicioussources.config.service.v1.HeaderInjection;
import ai.traceable.malicioussources.config.service.v1.PredicateLocation;
import ai.traceable.malicioussources.config.service.v1.RuleEffectWithModifications;
import java.util.ArrayList;
import java.util.List;

public class MaliciousSourceRuleEffectConverter {
  public static RuleAction convert(
      List<RuleEffectWithModifications> ruleEffectWithModificationsList) {
    if (ruleEffectWithModificationsList.isEmpty()) {
      return null;
    }
    RuleAction.Builder ruleActionBuilder = RuleAction.newBuilder();
    List<InlineModification> inlineModifications = new ArrayList<>();

    for (RuleEffectWithModifications ruleEffectWithModifications :
        ruleEffectWithModificationsList) {
      if (ruleEffectWithModifications.hasAgentRuleEffect()) {
        AgentRuleEffect agentRuleEffect = ruleEffectWithModifications.getAgentRuleEffect();
        agentRuleEffect.getAgentModificationsList().stream()
            .map(MaliciousSourceRuleEffectConverter::convertAgentModification)
            .forEach(inlineModifications::add);
      }
    }

    ruleActionBuilder.addAllInlineModifications(inlineModifications);
    return ruleActionBuilder.build();
  }

  private static InlineModification convertAgentModification(AgentModification agentModification) {
    InlineModification.Builder inlineModificationBuilder = InlineModification.newBuilder();

    if (agentModification.hasHeaderInjection()) {
      ai.traceable.blocking.config.service.v2.HeaderInjection.Builder headerInjectionBuilder =
          ai.traceable.blocking.config.service.v2.HeaderInjection.newBuilder();
      HeaderInjection headerInjection = agentModification.getHeaderInjection();

      headerInjectionBuilder.setScope(convert(headerInjection.getHeaderLocation()));
      headerInjectionBuilder.setHeaderName(headerInjection.getHeaderName());
      headerInjectionBuilder.setValue(convertFieldValue(headerInjection.getValue()));

      inlineModificationBuilder.setHeaderInjection(headerInjectionBuilder);
    }

    return inlineModificationBuilder.build();
  }

  private static AttributeScope convert(PredicateLocation location) {
    switch (location) {
      case PREDICATE_LOCATION_REQUEST:
        return AttributeScope.ATTRIBUTE_SCOPE_REQUEST;
      case PREDICATE_LOCATION_RESPONSE:
        return AttributeScope.ATTRIBUTE_SCOPE_RESPONSE;
      default:
        return AttributeScope.ATTRIBUTE_SCOPE_UNSPECIFIED;
    }
  }

  private static ai.traceable.blocking.config.service.v2.FieldValue convertFieldValue(
      FieldValue fieldValue) {
    ai.traceable.blocking.config.service.v2.FieldValue.Builder fieldValueBuilder =
        ai.traceable.blocking.config.service.v2.FieldValue.newBuilder();

    if (fieldValue.hasStaticValue()) {
      fieldValueBuilder.setStaticValue(fieldValue.getStaticValue());
    }

    return fieldValueBuilder.build();
  }
}
