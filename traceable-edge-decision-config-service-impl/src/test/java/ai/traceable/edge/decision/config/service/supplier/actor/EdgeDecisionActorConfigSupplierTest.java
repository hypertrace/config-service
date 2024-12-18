package ai.traceable.edge.decision.config.service.supplier.actor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.platform.actor.v1.Status;
import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class ActorEdgeDecisionEngineConfigSupplierTest {

  @Mock private ActorDataCache actorDataCache;
  private ActorEdgeDecisionEngineConfigSupplier configSupplier;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    configSupplier = new ActorEdgeDecisionEngineConfigSupplier(actorDataCache);
  }

  @Test
  void testGetEdgeDecisionActorConfig_withValidActorData() {
    RequestContext requestContext = RequestContext.forTenantId("test-tenant");

    // Mock actor data
    ActorData actor1 =
        ActorData.builder()
            .entityId("actor-1")
            .actorId("user-1")
            .expirationTimestampMillis(500000L)
            .ipAddresses(List.of("1.2.3.4"))
            .status(Status.STATUS_SNOOZED)
            .build();

    ActorData actor2 =
        ActorData.builder()
            .entityId("actor-2")
            .actorId("user-2")
            .expirationTimestampMillis(0L)
            .ipAddresses(List.of("1.2.3.14"))
            .status(Status.STATUS_ALWAYS_DENIED)
            .build();

    List<ActorData> actorDataList = List.of(actor1, actor2);
    when(actorDataCache.getActorData(requestContext.buildInternalContextualKey(Optional.empty())))
        .thenReturn(actorDataList);

    EdgeDecisionEngineConfig config = configSupplier.get(requestContext);

    assertEquals("actor-rule-id-test-tenant", config.getId());
    assertEquals("user-based-actor-blocking", config.getName());
    assertEquals(2, config.getDecisionRulesCount());

    // Verify rule 1
    var rule1 = config.getDecisionRules(0);
    assertEquals("actor-1", rule1.getId());
    assertEquals("user-1", rule1.getName());
    assertEquals(
        Timestamp.newBuilder().setSeconds(500).build(),
        rule1.getRuleStatus().getTtl().getExpiresAt());
    assertEquals(
        "TRACEABLE_USER_ID == 'user-1'",
        rule1
            .getRuleDefinition()
            .getSignatureRule()
            .getMatchCondition()
            .getGenericMatchCondition()
            .getJexlExpression()
            .getJexlExpression());
    assertEquals("EDGE_DECISION_TYPE_ALLOW", rule1.getRuleDecision().getEdgeDecisionType().name());

    // Verify rule 2
    var rule2 = config.getDecisionRules(1);
    assertEquals("actor-2", rule2.getId());
    assertEquals("user-2", rule2.getName());
    assertEquals("EDGE_DECISION_TYPE_BLOCK", rule2.getRuleDecision().getEdgeDecisionType().name());
    assertEquals(
        "TRACEABLE_USER_ID == 'user-2'",
        rule2
            .getRuleDefinition()
            .getSignatureRule()
            .getMatchCondition()
            .getGenericMatchCondition()
            .getJexlExpression()
            .getJexlExpression());
    assertFalse(rule2.getRuleStatus().hasTtl());
  }

  @Test
  void testGetEdgeDecisionActorConfig_withEmptyActorData() {
    RequestContext requestContext = RequestContext.forTenantId("tenant");
    when(actorDataCache.getActorData(requestContext.buildInternalContextualKey(Optional.empty())))
        .thenReturn(List.of());

    EdgeDecisionEngineConfig config = configSupplier.get(requestContext);

    assertEquals("actor-rule-id-tenant", config.getId());
    assertEquals("user-based-actor-blocking", config.getName());
    assertEquals(0, config.getDecisionRulesCount());
  }
}
