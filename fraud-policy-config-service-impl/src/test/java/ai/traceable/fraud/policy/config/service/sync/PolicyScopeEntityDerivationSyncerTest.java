package ai.traceable.fraud.policy.config.service.sync;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.DeleteEntityDerivationConfigResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityCategory;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigData;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigServiceGrpc.EntityDerivationConfigServiceBlockingStub;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityType;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EnvironmentScope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EventDerivationConfigDetails;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.Scope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanProjection;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.UpdateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.UpdateEntityDerivationConfigResponse;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.policy.config.service.v1.AbuseApiIds;
import ai.traceable.fraud.policy.config.service.v1.AbuseApiLabels;
import ai.traceable.fraud.policy.config.service.v1.AbuseApiScope;
import ai.traceable.fraud.policy.config.service.v1.AbuseEnvironmentScope;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicy;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyData;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyScope;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PolicyScopeEntityDerivationSyncerTest {

  @Mock private EntityDerivationConfigServiceBlockingStub stub;

  private PolicyScopeEntityDerivationSyncer syncer;
  private RequestContext ctx;

  @BeforeEach
  void setUp() {
    syncer = new PolicyScopeEntityDerivationSyncer(stub);
    ctx = RequestContext.forTenantId("test-tenant");
  }

  @Test
  void onPolicyCreated_noDummyEntity_createsOneWithFixedId() {
    stubGetReturnsEmpty();
    when(stub.createEntityDerivationConfig(any()))
        .thenReturn(
            CreateEntityDerivationConfigResponse.newBuilder()
                .setEntityDerivationConfig(
                    EntityDerivationConfig.newBuilder()
                        .setId(PolicyScopeEntityDerivationSyncer.DUMMY_ENTITY_ID))
                .build());

    callInContext(() -> syncer.onPolicyCreated(ctx, policy("p1", "env-1", "api-1")));

    var captor = ArgumentCaptor.forClass(CreateEntityDerivationConfigRequest.class);
    verify(stub).createEntityDerivationConfig(captor.capture());

    assertEquals(PolicyScopeEntityDerivationSyncer.DUMMY_ENTITY_ID, captor.getValue().getId());
    var data = captor.getValue().getData();
    assertTrue(data.getInternal());
    assertFalse(data.getDisabled());
    assertEquals(1, data.getSpanProjection().getEventDerivationConfigsCount());

    var entry = data.getSpanProjection().getEventDerivationConfigs(0);
    assertEquals("p1", entry.getName());
    assertEquals("env-1", entry.getScope().getEnvironmentScope().getEnvironments(0));
    assertEquals(EntityType.ENTITY_TYPE_API, entry.getScope().getEntityScope().getEntityType());
    assertEquals("api-1", entry.getScope().getEntityScope().getEntityIds(0));
  }

  @Test
  void onPolicyCreated_dummyExists_addsEntry() {
    stubGetReturns(dummyEntity(List.of(derivationEntry("existing", "env-1"))));
    when(stub.updateEntityDerivationConfig(any()))
        .thenReturn(UpdateEntityDerivationConfigResponse.getDefaultInstance());

    callInContext(() -> syncer.onPolicyCreated(ctx, policy("p2", "env-2", "api-2")));

    var captor = ArgumentCaptor.forClass(UpdateEntityDerivationConfigRequest.class);
    verify(stub).updateEntityDerivationConfig(captor.capture());
    verify(stub, never()).createEntityDerivationConfig(any());
    assertEquals(PolicyScopeEntityDerivationSyncer.DUMMY_ENTITY_ID, captor.getValue().getId());
    assertEquals(
        2, captor.getValue().getData().getSpanProjection().getEventDerivationConfigsCount());
  }

  @Test
  void onPolicyUpdated_scopeChanged_replacesEntry() {
    stubGetReturns(dummyEntity(List.of(derivationEntry("p1", "env-1"))));
    when(stub.updateEntityDerivationConfig(any()))
        .thenReturn(UpdateEntityDerivationConfigResponse.getDefaultInstance());

    callInContext(
        () ->
            syncer.onPolicyUpdated(
                ctx, policy("p1", "env-1", "api-1"), policy("p1", "env-2", "api-2")));

    var captor = ArgumentCaptor.forClass(UpdateEntityDerivationConfigRequest.class);
    verify(stub).updateEntityDerivationConfig(captor.capture());
    var entries = captor.getValue().getData().getSpanProjection().getEventDerivationConfigsList();
    assertEquals(1, entries.size());
    assertEquals("p1", entries.get(0).getName());
    assertEquals("env-2", entries.get(0).getScope().getEnvironmentScope().getEnvironments(0));
  }

  @Test
  void onPolicyUpdated_scopeUnchanged_skips() {
    callInContext(
        () ->
            syncer.onPolicyUpdated(
                ctx, policy("p1", "env-1", "api-1"), policy("p1", "env-1", "api-1")));

    verify(stub, never()).getEntityDerivationConfigs(any());
    verify(stub, never()).updateEntityDerivationConfig(any());
  }

  @Test
  void onPoliciesDeleted_lastEntry_deletesEntity() {
    stubGetReturns(dummyEntity(List.of(derivationEntry("p1", "env-1"))));
    when(stub.deleteEntityDerivationConfig(any()))
        .thenReturn(DeleteEntityDerivationConfigResponse.getDefaultInstance());

    callInContext(() -> syncer.onPoliciesDeleted(ctx, List.of(policyStub("p1"))));

    verify(stub).deleteEntityDerivationConfig(any());
    verify(stub, never()).updateEntityDerivationConfig(any());
  }

  @Test
  void onPoliciesDeleted_otherEntriesRemain_updatesEntity() {
    stubGetReturns(
        dummyEntity(List.of(derivationEntry("p1", "env-1"), derivationEntry("p2", "env-2"))));
    when(stub.updateEntityDerivationConfig(any()))
        .thenReturn(UpdateEntityDerivationConfigResponse.getDefaultInstance());

    callInContext(() -> syncer.onPoliciesDeleted(ctx, List.of(policyStub("p1"))));

    verify(stub, never()).deleteEntityDerivationConfig(any());
    var captor = ArgumentCaptor.forClass(UpdateEntityDerivationConfigRequest.class);
    verify(stub).updateEntityDerivationConfig(captor.capture());
    var entries = captor.getValue().getData().getSpanProjection().getEventDerivationConfigsList();
    assertEquals(1, entries.size());
    assertEquals("p2", entries.get(0).getName());
  }

  @Test
  void onPoliciesDeleted_noDummyEntity_skips() {
    stubGetReturnsEmpty();
    callInContext(() -> syncer.onPoliciesDeleted(ctx, List.of(policyStub("p1"))));
    verify(stub, never()).deleteEntityDerivationConfig(any());
    verify(stub, never()).updateEntityDerivationConfig(any());
  }

  @Test
  void scopeMapping_apiIds() {
    var scope =
        PolicyScopeEntityDerivationSyncer.mapPolicyScopeToDerivationScope(
            AbusePolicyScope.newBuilder()
                .setEnvironmentScope(
                    AbuseEnvironmentScope.newBuilder()
                        .addEnvironmentIds("e1")
                        .addEnvironmentIds("e2"))
                .setApiScope(
                    AbuseApiScope.newBuilder()
                        .setApiIds(AbuseApiIds.newBuilder().addIds("a1").addIds("a2")))
                .build());

    assertEquals(List.of("e1", "e2"), scope.getEnvironmentScope().getEnvironmentsList());
    assertEquals(EntityType.ENTITY_TYPE_API, scope.getEntityScope().getEntityType());
    assertEquals(List.of("a1", "a2"), scope.getEntityScope().getEntityIdsList());
  }

  @Test
  void scopeMapping_apiLabels_omitsEntityScope() {
    var scope =
        PolicyScopeEntityDerivationSyncer.mapPolicyScopeToDerivationScope(
            AbusePolicyScope.newBuilder()
                .setEnvironmentScope(AbuseEnvironmentScope.newBuilder().addEnvironmentIds("e1"))
                .setApiScope(
                    AbuseApiScope.newBuilder()
                        .setApiLabels(AbuseApiLabels.newBuilder().addLabels("l1")))
                .build());

    assertTrue(scope.hasEnvironmentScope());
    assertFalse(scope.hasEntityScope());
  }

  // -- helpers --

  private void callInContext(Runnable action) {
    ctx.call(
        () -> {
          action.run();
          return null;
        });
  }

  private void stubGetReturnsEmpty() {
    when(stub.getEntityDerivationConfigs(any()))
        .thenReturn(GetEntityDerivationConfigsResponse.getDefaultInstance());
  }

  private void stubGetReturns(EntityDerivationConfig entity) {
    when(stub.getEntityDerivationConfigs(any()))
        .thenReturn(
            GetEntityDerivationConfigsResponse.newBuilder()
                .addEntityDerivationConfigs(entity)
                .build());
  }

  private static AbusePolicy policy(String id, String envId, String apiId) {
    return AbusePolicy.newBuilder()
        .setId(id)
        .setData(
            AbusePolicyData.newBuilder()
                .setScope(
                    AbusePolicyScope.newBuilder()
                        .setEnvironmentScope(
                            AbuseEnvironmentScope.newBuilder().addEnvironmentIds(envId))
                        .setApiScope(
                            AbuseApiScope.newBuilder()
                                .setApiIds(AbuseApiIds.newBuilder().addIds(apiId)))))
        .build();
  }

  private static AbusePolicy policyStub(String id) {
    return AbusePolicy.newBuilder().setId(id).setData(AbusePolicyData.getDefaultInstance()).build();
  }

  private static EntityDerivationConfig dummyEntity(List<EventDerivationConfigDetails> entries) {
    return EntityDerivationConfig.newBuilder()
        .setId(PolicyScopeEntityDerivationSyncer.DUMMY_ENTITY_ID)
        .setData(
            EntityDerivationConfigData.newBuilder()
                .setDisplayName(PolicyScopeEntityDerivationSyncer.DUMMY_ENTITY_DISPLAY_NAME)
                .setInternal(true)
                .setCategory(EntityCategory.ENTITY_CATEGORY_CUSTOM)
                .setEventKind(
                    ComplexDataModelEventKind.newBuilder()
                        .setKindId(PolicyScopeEntityDerivationSyncer.EVENT_KIND_STRING))
                .setSpanProjection(
                    SpanProjection.newBuilder().addAllEventDerivationConfigs(entries)))
        .build();
  }

  private static EventDerivationConfigDetails derivationEntry(String policyId, String envId) {
    return EventDerivationConfigDetails.newBuilder()
        .setName(policyId)
        .setJexlExpression(PolicyScopeEntityDerivationSyncer.JEXL_CONSTANT)
        .setScope(
            Scope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironments(envId)))
        .build();
  }
}
