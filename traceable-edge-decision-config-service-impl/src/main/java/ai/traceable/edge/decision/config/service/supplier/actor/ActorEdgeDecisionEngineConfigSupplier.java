package ai.traceable.edge.decision.config.service.supplier.actor;

import static ai.traceable.edge.decision.config.service.RuleInfoDecorationsHandler.ACTOR_ENTITY_ID;
import static ai.traceable.edge.decision.config.service.VariableConstants.USER_ATTRIBUTION_VARIABLE_NAME;
import static ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder.getEncodedThreatActorViolationInfo;

import ai.traceable.datamodel.data.transformation.config.v1.GenericMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.edge.decision.config.service.RuleInfoDecorationsHandler;
import ai.traceable.edge.decision.config.service.SpanAttributeHandler;
import ai.traceable.edge.decision.config.service.supplier.EdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.v1.ConfigTtl;
import ai.traceable.edge.decision.config.service.v1.EdgeDecision;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategory;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleDefinition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleStatus;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleStatus.Builder;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionType;
import ai.traceable.edge.decision.config.service.v1.EdgeInputKind;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionConfigsFilter;
import ai.traceable.edge.decision.config.service.v1.PolicyKind;
import ai.traceable.edge.decision.config.service.v1.SignatureRule;
import ai.traceable.platform.actor.v1.Status;
import com.google.protobuf.Timestamp;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.NonNull;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ActorEdgeDecisionEngineConfigSupplier implements EdgeDecisionEngineConfigSupplier {
  private static final Function<String, String> USER_ATTRIBUTION_JEXL_GENERATOR =
      userId -> String.format("%s == '%s'", USER_ATTRIBUTION_VARIABLE_NAME.getValue(), userId);
  private static final Set<Status> ALLOWED_ACTOR_STATUSES =
      Set.of(Status.STATUS_ALWAYS_ALLOWED, Status.STATUS_SNOOZED);
  private final ActorDataCache actorDataCache;

  @Inject
  public ActorEdgeDecisionEngineConfigSupplier(ActorDataCache actorDataCache) {
    this.actorDataCache = actorDataCache;
  }

  @Override
  public String getName() {
    return ActorEdgeDecisionEngineConfigSupplier.class.getSimpleName();
  }

  @Override
  public EdgeDecisionEngineConfig get(
      RequestContext requestContext, GetEdgeDecisionConfigsFilter filter) {
    if (filter != null) {
      List<EdgeInputKind> inputKinds = filter.getEdgeInputKindsList();
      if (!inputKinds.isEmpty()
          && !inputKinds.contains(EdgeInputKind.EDGE_INPUT_KIND_HTTP_REQUEST)) {
        // currently, this supplier doesn't provide any other input kind.
        return EdgeDecisionEngineConfig.getDefaultInstance();
      }
    }
    Optional<String> environment = Optional.empty();
    List<ActorData> actorDataList =
        actorDataCache.getActorData(requestContext.buildInternalContextualKey(environment));

    return EdgeDecisionEngineConfig.newBuilder()
        .setId("actor-rule-id-" + requestContext.getTenantId().orElse("") + environment.orElse(""))
        .setName("user-based-actor-blocking")
        .addAllDecisionRules(
            actorDataList.stream()
                .map(this::getEdgeDecisionRule)
                .collect(Collectors.toUnmodifiableList()))
        .build();
  }

  @NonNull
  private EdgeDecisionRule getEdgeDecisionRule(ActorData actorData) {
    // Build the TTL only if expiration is non-zero
    Builder ruleStatusBuilder = EdgeDecisionRuleStatus.newBuilder();
    if (actorData.getExpirationTimestampMillis() > 0) {
      ruleStatusBuilder.setTtl(
          ConfigTtl.newBuilder()
              .setExpiresAt(
                  Timestamp.newBuilder()
                      .setSeconds(actorData.getExpirationTimestampMillis() / 1000)));
    }

    EdgeDecisionType edgeDecisionType = EdgeDecisionType.EDGE_DECISION_TYPE_BLOCK;
    boolean isExemption = ALLOWED_ACTOR_STATUSES.contains(actorData.getStatus());
    if (isExemption) {
      edgeDecisionType = EdgeDecisionType.EDGE_DECISION_TYPE_ALLOW;
    }

    return EdgeDecisionRule.newBuilder()
        .setId(actorData.getEntityId())
        .setName(actorData.getActorId())
        .setRuleCategory(EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_THREAT_ACTOR)
        .setRuleStatus(ruleStatusBuilder)
        .setRuleDefinition(
            EdgeDecisionRuleDefinition.newBuilder()
                .setEdgeInputKind(EdgeInputKind.EDGE_INPUT_KIND_HTTP_REQUEST)
                .setSignatureRule(
                    SignatureRule.newBuilder()
                        .setMatchCondition(
                            MatchCondition.newBuilder()
                                .setGenericMatchCondition(
                                    GenericMatchCondition.newBuilder()
                                        .setJexlExpression(
                                            JexlExpressionConfig.newBuilder()
                                                .setJexlExpression(
                                                    USER_ATTRIBUTION_JEXL_GENERATOR.apply(
                                                        actorData.getActorId())))))))
        .setRuleDecision(
            EdgeDecision.newBuilder()
                .setEdgeDecisionType(edgeDecisionType)
                .addAllSpanAttributes(
                    SpanAttributeHandler.getSpanAttributeDecorations(
                        actorData.getEntityId(),
                        isExemption,
                        EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_THREAT_ACTOR,
                        getEncodedThreatActorViolationInfo(actorData.getEntityId())))
                .addAllRuleInfoDecorations(
                    RuleInfoDecorationsHandler.getRuleInfoDecorations(
                        Map.of(ACTOR_ENTITY_ID, actorData.getEntityId()))))
        .setPolicyKind(PolicyKind.POLICY_KIND_WAF)
        .build();
  }
}
