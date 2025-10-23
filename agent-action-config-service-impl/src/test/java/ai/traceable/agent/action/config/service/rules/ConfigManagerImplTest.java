package ai.traceable.agent.action.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.agent.action.config.service.v1.AgentAction;
import ai.traceable.agent.action.config.service.v1.AgentActionDetails;
import ai.traceable.agent.action.config.service.v1.AgentActionInput;
import ai.traceable.agent.action.config.service.v1.AgentScope;
import ai.traceable.agent.action.config.service.v1.CompositeFilter;
import ai.traceable.agent.action.config.service.v1.ConfigMutationAction;
import ai.traceable.agent.action.config.service.v1.CreateAgentActionRequest;
import ai.traceable.agent.action.config.service.v1.FetchDebugInformationAction;
import ai.traceable.agent.action.config.service.v1.Filter;
import ai.traceable.agent.action.config.service.v1.GetAgentActionsFilter;
import ai.traceable.agent.action.config.service.v1.LeafFilter;
import ai.traceable.agent.action.config.service.v1.RestartAgentAction;
import ai.traceable.agent.action.config.service.v1.UpdateAgentActionRequest;
import ai.traceable.config.utils.UuidGenerator;
import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.DeletedContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ConfigManagerImplTest {

  private static final AgentAction defaultAction =
      AgentAction.newBuilder()
          .setId("rule-id")
          .setExpirationTimestamp(Timestamp.newBuilder().setSeconds(1697164800).build())
          .setScope(
              AgentScope.newBuilder()
                  .setModuleName("traceable-agent")
                  .setDeploymentName("test-depl")
                  .setServiceInstanceId("test-sid")
                  .setModuleVersion("test-version"))
          .addActionDetails(
              AgentActionDetails.newBuilder()
                  .setRestartAgentAction(RestartAgentAction.newBuilder()))
          .addActionDetails(
              AgentActionDetails.newBuilder()
                  .setFetchDebugInformationAction(FetchDebugInformationAction.newBuilder()))
          .addActionDetails(
              AgentActionDetails.newBuilder()
                  .setUpdateConfigAction(
                      ConfigMutationAction.newBuilder()
                          .putEnvironmentVariables("TA_ENVIRONMENT", "test-env")))
          .build();

  private static final CompositeFilter defaultFilter =
      CompositeFilter.newBuilder()
          .addFilters(
              Filter.newBuilder()
                  .setLeaf(
                      LeafFilter.newBuilder()
                          .setDeploymentName("test-depl")
                          .setModuleName("traceable-agent")
                          .setModuleVersion("test-version")
                          .setServiceInstanceId("test-sid"))
                  .build())
          .build();

  private static final List<AgentAction> defaultActions = List.of(defaultAction);

  @Test
  void getAgentActions() {
    RequestContext ctx = RequestContext.CURRENT.get();
    AgentActionStore store = Mockito.mock(AgentActionStore.class);
    ConfigManagerImpl configManager = new ConfigManagerImpl(store, new UuidGenerator());

    Mockito.when(store.getAllConfigData(ctx)).thenReturn(defaultActions);
    List<AgentAction> agentActions =
        configManager.getAgentActions(ctx, GetAgentActionsFilter.getDefaultInstance());
    assertEquals(1, agentActions.size());
    assertEquals(defaultActions, agentActions);
    Mockito.verify(store).getAllConfigData(ctx);

    GetAgentActionsFilter customFilter =
        GetAgentActionsFilter.newBuilder().setFilter(defaultFilter).build();
    Mockito.when(store.getAllConfigData(ctx, customFilter))
        .thenReturn(List.of(defaultAction, defaultAction));
    agentActions = configManager.getAgentActions(ctx, customFilter);
    assertEquals(2, agentActions.size());
    assertEquals(defaultAction, agentActions.get(0));
    assertEquals(defaultAction, agentActions.get(1));
    Mockito.verify(store).getAllConfigData(ctx, customFilter);
  }

  @Test
  @SuppressWarnings("unchecked")
  void createAgentAction() {
    RequestContext ctx = RequestContext.CURRENT.get();
    AgentActionStore store = Mockito.mock(AgentActionStore.class);
    UuidGenerator generator = Mockito.mock(UuidGenerator.class);
    ConfigManagerImpl configManager = new ConfigManagerImpl(store, generator);

    Mockito.when(generator.generateRandomId()).thenReturn("rule-id");
    CreateAgentActionRequest request =
        CreateAgentActionRequest.newBuilder()
            .setAction(
                AgentActionInput.newBuilder()
                    .addAllDetails(defaultAction.getActionDetailsList())
                    .setScope(defaultAction.getScope())
                    .setExpirationTimestamp(defaultAction.getExpirationTimestamp()))
            .build();

    ContextualConfigObject<AgentAction> storeResponse =
        (ContextualConfigObject<AgentAction>) Mockito.mock(ContextualConfigObject.class);
    Mockito.when(store.upsertObject(Mockito.eq(ctx), Mockito.any())).thenReturn(storeResponse);
    Mockito.when(storeResponse.getData()).thenReturn(defaultAction);
    Mockito.when(storeResponse.getContext()).thenReturn("rule-id");

    AgentAction createdAction = configManager.createAgentAction(ctx, request);
    assertEquals(defaultAction, createdAction);
    Mockito.verify(store).upsertObject(ctx, defaultAction);
  }

  @Test
  @SuppressWarnings("unchecked")
  void updateAgentAction() {
    RequestContext ctx = RequestContext.CURRENT.get();
    AgentActionStore store = Mockito.mock(AgentActionStore.class);
    ConfigManagerImpl configManager = new ConfigManagerImpl(store, null);

    AgentScope updatedScope = AgentScope.newBuilder().setModuleName("dotnetagent").build();
    UpdateAgentActionRequest request =
        UpdateAgentActionRequest.newBuilder()
            .setId("rule-id")
            .setAction(
                AgentActionInput.newBuilder()
                    .addAllDetails(defaultAction.getActionDetailsList())
                    .setScope(updatedScope)
                    .setExpirationTimestamp(defaultAction.getExpirationTimestamp()))
            .build();

    AgentAction updatedAction =
        AgentAction.newBuilder()
            .setId("rule-id")
            .setScope(updatedScope)
            .addAllActionDetails(defaultAction.getActionDetailsList())
            .setExpirationTimestamp(defaultAction.getExpirationTimestamp())
            .build();
    ContextualConfigObject<AgentAction> storeResponse =
        (ContextualConfigObject<AgentAction>) Mockito.mock(ContextualConfigObject.class);
    Mockito.when(store.getData(ctx, "rule-id")).thenReturn(Optional.of(defaultAction));
    Mockito.when(store.upsertObject(Mockito.eq(ctx), Mockito.any())).thenReturn(storeResponse);
    Mockito.when(storeResponse.getData()).thenReturn(updatedAction);
    Mockito.when(storeResponse.getContext()).thenReturn("rule-id");

    AgentAction createdAction = configManager.updateAgentAction(ctx, request);
    assertEquals(updatedAction, createdAction);
    Mockito.verify(store).upsertObject(ctx, updatedAction);
  }

  @Test
  void doesAgentActionExist() {
    RequestContext ctx = Mockito.mock(RequestContext.class);
    AgentActionStore store = Mockito.mock(AgentActionStore.class);
    ConfigManagerImpl configManager = new ConfigManagerImpl(store, null);

    AgentScope updatedScope = AgentScope.newBuilder().setModuleName("dotnetagent").build();
    UpdateAgentActionRequest request =
        UpdateAgentActionRequest.newBuilder()
            .setId("rule-id")
            .setAction(
                AgentActionInput.newBuilder()
                    .addAllDetails(defaultAction.getActionDetailsList())
                    .setScope(updatedScope)
                    .setExpirationTimestamp(defaultAction.getExpirationTimestamp()))
            .build();
    assertThrows(RuntimeException.class, () -> configManager.updateAgentAction(ctx, request));
  }

  @Test
  @SuppressWarnings("unchecked")
  void deleteAgentAction() {
    RequestContext ctx = RequestContext.CURRENT.get();
    AgentActionStore store = Mockito.mock(AgentActionStore.class);
    ConfigManagerImpl configManager = new ConfigManagerImpl(store, null);

    DeletedContextualConfigObject<AgentAction> deletedResponse =
        (DeletedContextualConfigObject<AgentAction>)
            Mockito.mock(DeletedContextualConfigObject.class);
    Mockito.when(store.deleteObject(ctx, "rule-id")).thenReturn(Optional.of(deletedResponse));
    configManager.deleteAgentAction(ctx, "rule-id");
    Mockito.verify(store).deleteObject(ctx, "rule-id");
  }

  @Test
  void deleteAgentActionNotFound() {
    RequestContext ctx = Mockito.mock(RequestContext.class);
    AgentActionStore store = Mockito.mock(AgentActionStore.class);
    ConfigManagerImpl configManager = new ConfigManagerImpl(store, null);

    assertThrows(RuntimeException.class, () -> configManager.deleteAgentAction(ctx, "rule-id"));
  }
}
