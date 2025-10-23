package ai.traceable.agent.action.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.agent.action.config.service.v1.AgentAction;
import ai.traceable.agent.action.config.service.v1.AgentActionDetails;
import ai.traceable.agent.action.config.service.v1.AgentScope;
import ai.traceable.agent.action.config.service.v1.CompositeFilter;
import ai.traceable.agent.action.config.service.v1.ConfigMutationAction;
import ai.traceable.agent.action.config.service.v1.FetchDebugInformationAction;
import ai.traceable.agent.action.config.service.v1.Filter;
import ai.traceable.agent.action.config.service.v1.GetAgentActionsFilter;
import ai.traceable.agent.action.config.service.v1.LeafFilter;
import ai.traceable.agent.action.config.service.v1.RestartAgentAction;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class AgentActionStoreTest {

  static class AgentActionAndFilter {
    AgentAction action;
    GetAgentActionsFilter filter;
    boolean matched;

    AgentActionAndFilter(AgentAction action, GetAgentActionsFilter filter, boolean matched) {
      this.action = action;
      this.filter = filter;
      this.matched = matched;
    }
  }

  static Stream<AgentActionAndFilter> provideAgentActionsAndFilter() {
    return Stream.of(
        // base case of no scope and no filter
        new AgentActionAndFilter(
            AgentAction.newBuilder().build(), GetAgentActionsFilter.newBuilder().build(), true),
        // base case of no scope
        new AgentActionAndFilter(
            AgentAction.newBuilder().build(),
            GetAgentActionsFilter.newBuilder().setFilter(CompositeFilter.newBuilder()).build(),
            true),
        // base case of no filter
        new AgentActionAndFilter(
            AgentAction.newBuilder().setScope(AgentScope.newBuilder()).build(),
            GetAgentActionsFilter.newBuilder().build(),
            true),
        // scope present, but filter absent
        new AgentActionAndFilter(
            AgentAction.newBuilder()
                .setScope(
                    AgentScope.newBuilder()
                        .setDeploymentName("test-depl")
                        .setModuleName("dotnetagent"))
                .build(),
            GetAgentActionsFilter.newBuilder().setFilter(CompositeFilter.newBuilder()).build(),
            true),
        // scope absent, but filter present
        new AgentActionAndFilter(
            AgentAction.newBuilder().build(),
            GetAgentActionsFilter.newBuilder()
                .setFilter(
                    CompositeFilter.newBuilder()
                        .addFilters(
                            Filter.newBuilder()
                                .setLeaf(LeafFilter.newBuilder().setModuleName("dotnetagent"))))
                .build(),
            true),
        // partial matching failure case
        new AgentActionAndFilter(
            AgentAction.newBuilder()
                .setScope(
                    AgentScope.newBuilder()
                        .setDeploymentName("test-depl")
                        .setModuleName("dotnetagent"))
                .build(),
            GetAgentActionsFilter.newBuilder()
                .setFilter(
                    CompositeFilter.newBuilder()
                        .addFilters(
                            Filter.newBuilder()
                                .setLeaf(LeafFilter.newBuilder().setDeploymentName("test-depl"))))
                .build(),
            false),
        // partial matching failure case
        new AgentActionAndFilter(
            AgentAction.newBuilder()
                .setScope(
                    AgentScope.newBuilder()
                        .setDeploymentName("test-depl")
                        .setModuleName("dotnetagent"))
                .build(),
            GetAgentActionsFilter.newBuilder()
                .setFilter(
                    CompositeFilter.newBuilder()
                        .addFilters(
                            Filter.newBuilder()
                                .setLeaf(LeafFilter.newBuilder().setModuleName("dotnetagent"))))
                .build(),
            false),
        // submatching filters
        new AgentActionAndFilter(
            AgentAction.newBuilder()
                .setScope(
                    AgentScope.newBuilder()
                        .setDeploymentName("test-depl")
                        .setModuleName("dotnetagent"))
                .build(),
            GetAgentActionsFilter.newBuilder()
                .setFilter(
                    CompositeFilter.newBuilder()
                        .addFilters(
                            Filter.newBuilder()
                                .setLeaf(
                                    LeafFilter.newBuilder()
                                        .setModuleName("dotnetagent")
                                        .setDeploymentName("test-depl")))
                        .setOperator(CompositeFilter.LogicalOperator.LOGICAL_OPERATOR_AND))
                .build(),
            true),
        new AgentActionAndFilter(
            AgentAction.newBuilder()
                .setScope(
                    AgentScope.newBuilder()
                        .setDeploymentName("test-depl")
                        .setModuleName("dotnetagent"))
                .build(),
            GetAgentActionsFilter.newBuilder()
                .setFilter(
                    CompositeFilter.newBuilder()
                        .addFilters(
                            Filter.newBuilder()
                                .setLeaf(
                                    LeafFilter.newBuilder()
                                        .setModuleName("dotnetagent")
                                        .setDeploymentName("test-depl")
                                        .setServiceInstanceId("test-sid")))
                        .setOperator(CompositeFilter.LogicalOperator.LOGICAL_OPERATOR_AND))
                .build(),
            true),
        new AgentActionAndFilter(
            AgentAction.newBuilder()
                .setScope(
                    AgentScope.newBuilder()
                        .setDeploymentName("test-depl")
                        .setModuleName("dotnetagent"))
                .build(),
            GetAgentActionsFilter.newBuilder()
                .setFilter(
                    CompositeFilter.newBuilder()
                        .addFilters(
                            Filter.newBuilder()
                                .setLeaf(
                                    LeafFilter.newBuilder()
                                        .setModuleName("dotnetagent")
                                        .setDeploymentName("test-depl")
                                        .setServiceInstanceId("test-sid")
                                        .setModuleVersion("test-version")))
                        .setOperator(CompositeFilter.LogicalOperator.LOGICAL_OPERATOR_AND))
                .build(),
            true),
        // version match
        new AgentActionAndFilter(
            AgentAction.newBuilder()
                .setScope(
                    AgentScope.newBuilder()
                        .setDeploymentName("test-depl")
                        .setModuleName("dotnetagent")
                        .setModuleVersion("test-version"))
                .build(),
            GetAgentActionsFilter.newBuilder()
                .setFilter(
                    CompositeFilter.newBuilder()
                        .addFilters(
                            Filter.newBuilder()
                                .setLeaf(
                                    LeafFilter.newBuilder()
                                        .setModuleName("dotnetagent")
                                        .setDeploymentName("test-depl")
                                        .setModuleVersion("test-version")))
                        .setOperator(CompositeFilter.LogicalOperator.LOGICAL_OPERATOR_AND))
                .build(),
            true),
        // version mismatch
        new AgentActionAndFilter(
            AgentAction.newBuilder()
                .setScope(
                    AgentScope.newBuilder()
                        .setDeploymentName("test-depl")
                        .setModuleName("dotnetagent")
                        .setModuleVersion("test-version"))
                .build(),
            GetAgentActionsFilter.newBuilder()
                .setFilter(
                    CompositeFilter.newBuilder()
                        .addFilters(
                            Filter.newBuilder()
                                .setLeaf(
                                    LeafFilter.newBuilder()
                                        .setModuleName("dotnetagent")
                                        .setDeploymentName("test-depl")
                                        .setModuleVersion("random-version")))
                        .setOperator(CompositeFilter.LogicalOperator.LOGICAL_OPERATOR_AND))
                .build(),
            false),
        // service instance id match
        new AgentActionAndFilter(
            AgentAction.newBuilder()
                .setScope(
                    AgentScope.newBuilder()
                        .setDeploymentName("test-depl")
                        .setModuleName("dotnetagent")
                        .setServiceInstanceId("test-sid"))
                .build(),
            GetAgentActionsFilter.newBuilder()
                .setFilter(
                    CompositeFilter.newBuilder()
                        .addFilters(
                            Filter.newBuilder()
                                .setLeaf(
                                    LeafFilter.newBuilder()
                                        .setModuleName("dotnetagent")
                                        .setDeploymentName("test-depl")
                                        .setServiceInstanceId("test-sid")))
                        .setOperator(CompositeFilter.LogicalOperator.LOGICAL_OPERATOR_AND))
                .build(),
            true),
        // service instance id mismatch
        new AgentActionAndFilter(
            AgentAction.newBuilder()
                .setScope(
                    AgentScope.newBuilder()
                        .setDeploymentName("test-depl")
                        .setModuleName("dotnetagent")
                        .setServiceInstanceId("test-sid"))
                .build(),
            GetAgentActionsFilter.newBuilder()
                .setFilter(
                    CompositeFilter.newBuilder()
                        .addFilters(
                            Filter.newBuilder()
                                .setLeaf(
                                    LeafFilter.newBuilder()
                                        .setModuleName("dotnetagent")
                                        .setDeploymentName("test-depl")
                                        .setServiceInstanceId("unknown-sid")))
                        .setOperator(CompositeFilter.LogicalOperator.LOGICAL_OPERATOR_AND))
                .build(),
            false),
        // composite filter matching
        new AgentActionAndFilter(
            AgentAction.newBuilder()
                .setScope(
                    AgentScope.newBuilder()
                        .setDeploymentName("test-depl")
                        .setModuleName("dotnetagent"))
                .build(),
            GetAgentActionsFilter.newBuilder()
                .setFilter(
                    CompositeFilter.newBuilder()
                        .addFilters(
                            Filter.newBuilder()
                                .setLeaf(
                                    LeafFilter.newBuilder()
                                        .setModuleName("dotnetagent")
                                        .setDeploymentName("test-depl")
                                        .setServiceInstanceId("test-sid")))
                        .addFilters(
                            Filter.newBuilder()
                                .setLeaf(
                                    LeafFilter.newBuilder()
                                        .setModuleName("dotnetagent")
                                        .setDeploymentName("test-depl")
                                        .setServiceInstanceId("unknown-sid")))
                        .setOperator(CompositeFilter.LogicalOperator.LOGICAL_OPERATOR_AND))
                .build(),
            true),
        new AgentActionAndFilter(
            AgentAction.newBuilder()
                .setScope(
                    AgentScope.newBuilder()
                        .setDeploymentName("test-depl")
                        .setModuleName("dotnetagent")
                        .setServiceInstanceId("test-sid"))
                .build(),
            GetAgentActionsFilter.newBuilder()
                .setFilter(
                    CompositeFilter.newBuilder()
                        .addFilters(
                            Filter.newBuilder()
                                .setLeaf(
                                    LeafFilter.newBuilder()
                                        .setModuleName("dotnetagent")
                                        .setDeploymentName("test-depl")
                                        .setServiceInstanceId("test-sid")))
                        .addFilters(
                            Filter.newBuilder()
                                .setLeaf(
                                    LeafFilter.newBuilder()
                                        .setModuleName("dotnetagent")
                                        .setDeploymentName("test-depl")
                                        .setServiceInstanceId("unknown-sid")))
                        .setOperator(CompositeFilter.LogicalOperator.LOGICAL_OPERATOR_AND))
                .build(),
            false),
        new AgentActionAndFilter(
            AgentAction.newBuilder()
                .setScope(
                    AgentScope.newBuilder()
                        .setDeploymentName("test-depl")
                        .setModuleName("dotnetagent")
                        .setServiceInstanceId("test-sid"))
                .build(),
            GetAgentActionsFilter.newBuilder()
                .setFilter(
                    CompositeFilter.newBuilder()
                        .addFilters(
                            Filter.newBuilder()
                                .setLeaf(
                                    LeafFilter.newBuilder()
                                        .setModuleName("dotnetagent")
                                        .setDeploymentName("test-depl")
                                        .setServiceInstanceId("test-sid")))
                        .addFilters(
                            Filter.newBuilder()
                                .setLeaf(
                                    LeafFilter.newBuilder()
                                        .setModuleName("dotnetagent")
                                        .setDeploymentName("test-depl")
                                        .setServiceInstanceId("unknown-sid")))
                        .setOperator(CompositeFilter.LogicalOperator.LOGICAL_OPERATOR_OR))
                .build(),
            true));
  }

  @ParameterizedTest
  @MethodSource("provideAgentActionsAndFilter")
  void filterConfigData(AgentActionAndFilter tc) {
    AgentActionStore agentActionStore = new AgentActionStore(null, null);
    Optional<AgentAction> result = agentActionStore.filterConfigData(tc.action, tc.filter);
    if (tc.matched) {
      assertTrue(result.isPresent());
      assertEquals(tc.action, result.get());
    } else {
      assertTrue(result.isEmpty());
    }
  }

  @Test
  void buildDataFromValue() throws InvalidProtocolBufferException {
    AgentActionStore agentActionStore = new AgentActionStore(null, null);
    AgentAction agentAction =
        AgentAction.newBuilder()
            .setId("rule-id")
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

    Value value = agentActionStore.buildValueFromData(agentAction);
    Value.Builder expectedValueBuilder = Value.newBuilder();
    String expectedJson =
        "{\n"
            + "  \"id\": \"rule-id\",\n"
            + "  \"actionDetails\": [{\n"
            + "    \"restartAgentAction\": {\n"
            + "    }\n"
            + "  }, {\n"
            + "    \"fetchDebugInformationAction\": {\n"
            + "    }\n"
            + "  }, {\n"
            + "    \"updateConfigAction\": {\n"
            + "      \"environmentVariables\": {\n"
            + "        \"TA_ENVIRONMENT\": \"test-env\"\n"
            + "      }\n"
            + "    }\n"
            + "  }],\n"
            + "  \"scope\": {\n"
            + "    \"deploymentName\": \"test-depl\",\n"
            + "    \"serviceInstanceId\": \"test-sid\",\n"
            + "    \"moduleName\": \"traceable-agent\",\n"
            + "    \"moduleVersion\": \"test-version\"\n"
            + "  }\n"
            + "}";
    JsonFormat.parser().ignoringUnknownFields().merge(expectedJson, expectedValueBuilder);
    assertEquals(expectedValueBuilder.build(), value);

    Optional<AgentAction> action = agentActionStore.buildDataFromValue(value);
    assertTrue(action.isPresent());
    assertEquals(agentAction, action.get());
  }

  @Test
  void getContextFromData() {
    AgentActionStore agentActionStore = new AgentActionStore(null, null);
    AgentAction agentAction = AgentAction.newBuilder().setId("rule-id").build();
    assertEquals("rule-id", agentActionStore.getContextFromData(agentAction));
  }
}
