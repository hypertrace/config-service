package ai.traceable.edge.decision.config.service.supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.edge.decision.config.service.store.EdgeAttributionRuleStoreManager;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionConfigStoreManager;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionRuleStoreManager;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionSpecStoreManager;
import ai.traceable.edge.decision.config.service.v1.ConfigTtl;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleRecord;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleStatus;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionRulesResponse;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class StoredEdgeDecisionEngineConfigSupplierTest {

  @Mock private EdgeDecisionConfigStoreManager configStoreManager;
  @Mock private EdgeDecisionRuleStoreManager ruleStoreManager;
  @Mock private EdgeDecisionSpecStoreManager specStoreManager;
  @Mock private EdgeAttributionRuleStoreManager attributionRuleStoreManager;
  private StoredEdgeDecisionEngineConfigSupplier configSupplier;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    configSupplier =
        new StoredEdgeDecisionEngineConfigSupplier(
            configStoreManager, ruleStoreManager, specStoreManager, attributionRuleStoreManager);
  }

  @Test
  void testGetStoredRules() {
    RequestContext requestContext = RequestContext.forTenantId("tenant");

    EdgeDecisionRule activeRule =
        EdgeDecisionRule.newBuilder()
            .setId("rule1")
            .setRuleStatus(EdgeDecisionRuleStatus.newBuilder().setDisabled(false).build())
            .build();

    EdgeDecisionRule disabledRule =
        EdgeDecisionRule.newBuilder()
            .setId("rule2")
            .setRuleStatus(EdgeDecisionRuleStatus.newBuilder().setDisabled(true).build())
            .build();

    EdgeDecisionRule futureExpiryRule =
        EdgeDecisionRule.newBuilder()
            .setId("rule3")
            .setRuleStatus(
                EdgeDecisionRuleStatus.newBuilder()
                    .setDisabled(false)
                    .setTtl(
                        ConfigTtl.newBuilder()
                            .setExpiresAt(
                                Timestamp.newBuilder()
                                    .setSeconds(Instant.now().plusSeconds(3600).getEpochSecond())
                                    .build())
                            .build())
                    .build())
            .build();

    EdgeDecisionRule expiredRule =
        EdgeDecisionRule.newBuilder()
            .setId("rule4")
            .setRuleStatus(
                EdgeDecisionRuleStatus.newBuilder()
                    .setDisabled(false)
                    .setTtl(
                        ConfigTtl.newBuilder()
                            .setExpiresAt(
                                Timestamp.newBuilder()
                                    .setSeconds(Instant.now().minusSeconds(3600).getEpochSecond())
                                    .build())
                            .build())
                    .build())
            .build();

    when(ruleStoreManager.getAll(eq(requestContext), any()))
        .thenReturn(
            GetAllEdgeDecisionRulesResponse.newBuilder()
                .addAllEdgeDecisionRuleRecords(
                    Arrays.asList(
                        EdgeDecisionRuleRecord.newBuilder().setRule(activeRule).build(),
                        EdgeDecisionRuleRecord.newBuilder().setRule(disabledRule).build(),
                        EdgeDecisionRuleRecord.newBuilder().setRule(futureExpiryRule).build(),
                        EdgeDecisionRuleRecord.newBuilder().setRule(expiredRule).build()))
                .build());

    List<EdgeDecisionRule> storedRules = configSupplier.getStoredRules(requestContext);

    assertEquals(2, storedRules.size());
    assertTrue(storedRules.stream().anyMatch(rule -> rule.getId().equals("rule1")));
    assertFalse(storedRules.stream().anyMatch(rule -> rule.getId().equals("rule2")));
    assertTrue(storedRules.stream().anyMatch(rule -> rule.getId().equals("rule3")));
    assertFalse(storedRules.stream().anyMatch(rule -> rule.getId().equals("rule4")));

    verify(ruleStoreManager).getAll(eq(requestContext), any());
  }
}
