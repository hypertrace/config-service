package ai.traceable.fraud.policy.config.service.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionType;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunctionType;
import ai.traceable.fraud.policy.config.service.v1.AbuseActionConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseActionType;
import ai.traceable.fraud.policy.config.service.v1.AbuseAggregationConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseApiIds;
import ai.traceable.fraud.policy.config.service.v1.AbuseApiLabels;
import ai.traceable.fraud.policy.config.service.v1.AbuseApiScope;
import ai.traceable.fraud.policy.config.service.v1.AbuseEnvironmentScope;
import ai.traceable.fraud.policy.config.service.v1.AbuseGroupByConfig;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicy;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyData;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyFilter;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyScope;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyTemplateConfigKind;
import ai.traceable.fraud.policy.config.service.v1.AbusePredefinedBrowserBypassPolicy;
import ai.traceable.fraud.policy.config.service.v1.AbusePredefinedTemplateConfig;
import ai.traceable.fraud.policy.config.service.v1.AbusePredefinedTemplateType;
import ai.traceable.fraud.policy.config.service.v1.AbuseRiskSeverity;
import ai.traceable.fraud.policy.config.service.v1.AbuseSimpleAggregationTemplateConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseThresholdConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseThresholdOperator;
import ai.traceable.fraud.policy.config.service.v1.AbuseTimeWindow;
import ai.traceable.fraud.policy.config.service.v1.BrowserBypassTriageConfig;
import ai.traceable.fraud.policy.config.service.v1.GetAbusePoliciesRequest;
import com.google.protobuf.Duration;
import com.google.protobuf.Value;
import java.util.Optional;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AbusePolicyConfigStoreTest {

  private static final String POLICY_ID = "policy-1";
  private static final String ENV_ID = "env-a";
  private static final String API_ID = "api-a";

  @Mock private ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;
  @Mock private ConfigChangeEventGenerator configChangeEventGenerator;

  private AbusePolicyConfigStore store;

  @BeforeEach
  void setUp() {
    store = new AbusePolicyConfigStore(configServiceBlockingStub, configChangeEventGenerator);
  }

  @Test
  void filterConfigData_withoutFilter_returnsData() {
    AbusePolicy policy = policy(POLICY_ID, simpleAggregationData());

    Optional<AbusePolicy> result =
        store.filterConfigData(policy, GetAbusePoliciesRequest.getDefaultInstance());

    assertTrue(result.isPresent());
    assertEquals(policy, result.get());
  }

  @Test
  void filterConfigData_enabled_matchesWhenEqual() {
    AbusePolicy policy =
        policy(POLICY_ID, simpleAggregationData().toBuilder().setEnabled(true).build());

    assertTrue(
        store
            .filterConfigData(
                policy, requestWithFilter(AbusePolicyFilter.newBuilder().setEnabled(true).build()))
            .isPresent());
    assertFalse(
        store
            .filterConfigData(
                policy, requestWithFilter(AbusePolicyFilter.newBuilder().setEnabled(false).build()))
            .isPresent());
  }

  @Test
  void filterConfigData_policyIds_matchesWhenListed() {
    AbusePolicy policy = policy(POLICY_ID, simpleAggregationData());

    assertTrue(
        store
            .filterConfigData(
                policy,
                requestWithFilter(AbusePolicyFilter.newBuilder().addPolicyIds(POLICY_ID).build()))
            .isPresent());
    assertFalse(
        store
            .filterConfigData(
                policy,
                requestWithFilter(AbusePolicyFilter.newBuilder().addPolicyIds("other-id").build()))
            .isPresent());
  }

  @Test
  void filterConfigData_environmentIds_matchesWhenPolicyScopedToEnv() {
    AbusePolicy policy = policy(POLICY_ID, simpleAggregationData());

    assertTrue(
        store
            .filterConfigData(
                policy,
                requestWithFilter(AbusePolicyFilter.newBuilder().addEnvironmentIds(ENV_ID).build()))
            .isPresent());
    assertFalse(
        store
            .filterConfigData(
                policy,
                requestWithFilter(
                    AbusePolicyFilter.newBuilder().addEnvironmentIds("other-env").build()))
            .isPresent());
  }

  @Test
  void filterConfigData_environmentIds_policyWithoutEnvScope_passes() {
    AbusePolicy policy =
        policy(
            POLICY_ID,
            simpleAggregationData().toBuilder()
                .setScope(
                    AbusePolicyScope.newBuilder()
                        .setApiScope(
                            AbuseApiScope.newBuilder()
                                .setApiIds(AbuseApiIds.newBuilder().addIds(API_ID).build())
                                .build())
                        .build())
                .build());

    assertTrue(
        store
            .filterConfigData(
                policy,
                requestWithFilter(
                    AbusePolicyFilter.newBuilder().addEnvironmentIds("any-env").build()))
            .isPresent());
  }

  @Test
  void filterConfigData_severities_matchesWhenListed() {
    AbusePolicy policy = policy(POLICY_ID, simpleAggregationData());

    assertTrue(
        store
            .filterConfigData(
                policy,
                requestWithFilter(
                    AbusePolicyFilter.newBuilder()
                        .addSeverities(AbuseRiskSeverity.ABUSE_RISK_SEVERITY_HIGH)
                        .build()))
            .isPresent());
    assertFalse(
        store
            .filterConfigData(
                policy,
                requestWithFilter(
                    AbusePolicyFilter.newBuilder()
                        .addSeverities(AbuseRiskSeverity.ABUSE_RISK_SEVERITY_LOW)
                        .build()))
            .isPresent());
  }

  @Test
  void filterConfigData_apiIds_matchesWhenPolicyUsesApiIds() {
    AbusePolicy policy = policy(POLICY_ID, simpleAggregationData());

    assertTrue(
        store
            .filterConfigData(
                policy, requestWithFilter(AbusePolicyFilter.newBuilder().addApiIds(API_ID).build()))
            .isPresent());
    assertFalse(
        store
            .filterConfigData(
                policy,
                requestWithFilter(AbusePolicyFilter.newBuilder().addApiIds("other-api").build()))
            .isPresent());
  }

  @Test
  void filterConfigData_apiIds_policyWithoutApiIds_excluded() {
    AbusePolicy policy =
        policy(
            POLICY_ID,
            simpleAggregationData().toBuilder()
                .setScope(
                    AbusePolicyScope.newBuilder()
                        .setEnvironmentScope(
                            AbuseEnvironmentScope.newBuilder().addEnvironmentIds(ENV_ID).build())
                        .setApiScope(
                            AbuseApiScope.newBuilder()
                                .setApiLabels(
                                    AbuseApiLabels.newBuilder().addLabels("label1").build())
                                .build())
                        .build())
                .build());

    assertFalse(
        store
            .filterConfigData(
                policy, requestWithFilter(AbusePolicyFilter.newBuilder().addApiIds(API_ID).build()))
            .isPresent());
  }

  @Test
  void filterConfigData_actionTypes_matchesWhenListed() {
    AbusePolicy policy = policy(POLICY_ID, simpleAggregationData());

    assertTrue(
        store
            .filterConfigData(
                policy,
                requestWithFilter(
                    AbusePolicyFilter.newBuilder()
                        .addActionTypes(AbuseActionType.ABUSE_ACTION_TYPE_ALERT)
                        .build()))
            .isPresent());
    assertFalse(
        store
            .filterConfigData(
                policy,
                requestWithFilter(
                    AbusePolicyFilter.newBuilder()
                        .addActionTypes(AbuseActionType.ABUSE_ACTION_TYPE_BLOCK)
                        .build()))
            .isPresent());
  }

  @Test
  void filterConfigData_templateConfigKind_unspecified_doesNotRestrict() {
    AbusePolicy simple = policy("p1", simpleAggregationData());
    AbusePolicy predefined = policy("p2", predefinedData());

    AbusePolicyFilter filter =
        AbusePolicyFilter.newBuilder()
            .setTemplateConfigKind(
                AbusePolicyTemplateConfigKind.ABUSE_POLICY_TEMPLATE_CONFIG_KIND_UNSPECIFIED)
            .build();

    assertTrue(store.filterConfigData(simple, requestWithFilter(filter)).isPresent());
    assertTrue(store.filterConfigData(predefined, requestWithFilter(filter)).isPresent());
  }

  @Test
  void filterConfigData_templateConfigKind_simpleAggregation_matchesOnlyThatBranch() {
    AbusePolicy simple = policy("p1", simpleAggregationData());
    AbusePolicy predefined = policy("p2", predefinedData());

    AbusePolicyFilter filter =
        AbusePolicyFilter.newBuilder()
            .setTemplateConfigKind(
                AbusePolicyTemplateConfigKind
                    .ABUSE_POLICY_TEMPLATE_CONFIG_KIND_SIMPLE_AGGREGATION_TEMPLATE)
            .build();

    assertTrue(store.filterConfigData(simple, requestWithFilter(filter)).isPresent());
    assertFalse(store.filterConfigData(predefined, requestWithFilter(filter)).isPresent());
  }

  @Test
  void filterConfigData_templateConfigKind_predefined_matchesOnlyThatBranch() {
    AbusePolicy simple = policy("p1", simpleAggregationData());
    AbusePolicy predefined = policy("p2", predefinedData());

    AbusePolicyFilter filter =
        AbusePolicyFilter.newBuilder()
            .setTemplateConfigKind(
                AbusePolicyTemplateConfigKind.ABUSE_POLICY_TEMPLATE_CONFIG_KIND_PREDEFINED_TEMPLATE)
            .build();

    assertFalse(store.filterConfigData(simple, requestWithFilter(filter)).isPresent());
    assertTrue(store.filterConfigData(predefined, requestWithFilter(filter)).isPresent());
  }

  @Test
  void filterConfigData_templateConfigKind_browserBypass_matchesOnlyThatBranch() {
    AbusePolicy simple = policy("p1", simpleAggregationData());
    AbusePolicy browserBypass = policy("p2", browserBypassData());

    AbusePolicyFilter filter =
        AbusePolicyFilter.newBuilder()
            .setTemplateConfigKind(
                AbusePolicyTemplateConfigKind
                    .ABUSE_POLICY_TEMPLATE_CONFIG_KIND_PREDEFINED_BROWSER_BYPASS_POLICY)
            .build();

    assertFalse(store.filterConfigData(simple, requestWithFilter(filter)).isPresent());
    assertTrue(store.filterConfigData(browserBypass, requestWithFilter(filter)).isPresent());
  }

  @Test
  void filterConfigData_combinesMultipleCriteria() {
    AbusePolicy policy =
        policy(POLICY_ID, simpleAggregationData().toBuilder().setEnabled(false).build());

    AbusePolicyFilter matching =
        AbusePolicyFilter.newBuilder()
            .addPolicyIds(POLICY_ID)
            .setEnabled(false)
            .addSeverities(AbuseRiskSeverity.ABUSE_RISK_SEVERITY_HIGH)
            .addActionTypes(AbuseActionType.ABUSE_ACTION_TYPE_ALERT)
            .addApiIds(API_ID)
            .addEnvironmentIds(ENV_ID)
            .setTemplateConfigKind(
                AbusePolicyTemplateConfigKind
                    .ABUSE_POLICY_TEMPLATE_CONFIG_KIND_SIMPLE_AGGREGATION_TEMPLATE)
            .build();

    assertTrue(store.filterConfigData(policy, requestWithFilter(matching)).isPresent());

    AbusePolicyFilter wrongTemplate =
        matching.toBuilder()
            .setTemplateConfigKind(
                AbusePolicyTemplateConfigKind.ABUSE_POLICY_TEMPLATE_CONFIG_KIND_PREDEFINED_TEMPLATE)
            .build();

    assertFalse(store.filterConfigData(policy, requestWithFilter(wrongTemplate)).isPresent());
  }

  private static GetAbusePoliciesRequest requestWithFilter(AbusePolicyFilter filter) {
    return GetAbusePoliciesRequest.newBuilder().setFilter(filter).build();
  }

  private static AbusePolicy policy(String id, AbusePolicyData data) {
    return AbusePolicy.newBuilder().setId(id).setData(data).build();
  }

  private static AbusePolicyData simpleAggregationData() {
    return AbusePolicyData.newBuilder()
        .setName("test")
        .setScope(
            AbusePolicyScope.newBuilder()
                .setEnvironmentScope(
                    AbuseEnvironmentScope.newBuilder().addEnvironmentIds(ENV_ID).build())
                .setApiScope(
                    AbuseApiScope.newBuilder()
                        .setApiIds(AbuseApiIds.newBuilder().addIds(API_ID).build())
                        .build())
                .build())
        .setEnabled(true)
        .setSeverity(AbuseRiskSeverity.ABUSE_RISK_SEVERITY_HIGH)
        .setAction(
            AbuseActionConfig.newBuilder()
                .setActionType(AbuseActionType.ABUSE_ACTION_TYPE_ALERT)
                .build())
        .setSimpleAggregationTemplate(minimalSimpleAggregationTemplate())
        .setMessageFormat("fmt")
        .build();
  }

  private static AbusePolicyData predefinedData() {
    return simpleAggregationData().toBuilder()
        .clearSimpleAggregationTemplate()
        .setPredefinedTemplate(
            AbusePredefinedTemplateConfig.newBuilder()
                .setTemplateType(
                    AbusePredefinedTemplateType.ABUSE_PREDEFINED_TEMPLATE_TYPE_CREDENTIAL_STUFFING)
                .putParameters("k", Value.newBuilder().setStringValue("v").build())
                .build())
        .build();
  }

  private static AbusePolicyData browserBypassData() {
    return simpleAggregationData().toBuilder()
        .clearSimpleAggregationTemplate()
        .setAbusePredefinedBrowserBypassPolicy(
            AbusePredefinedBrowserBypassPolicy.newBuilder()
                .setTriageConfig(
                    BrowserBypassTriageConfig.newBuilder()
                        .setAction(EdgeDecisionType.EDGE_DECISION_TYPE_BLOCK)
                        .build())
                .build())
        .build();
  }

  private static AbuseSimpleAggregationTemplateConfig minimalSimpleAggregationTemplate() {
    return AbuseSimpleAggregationTemplateConfig.newBuilder()
        .setAggregation(
            AbuseAggregationConfig.newBuilder()
                .setAggregationFunction(AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_COUNT)
                .setDerivedEntityId("request_count")
                .build())
        .setGroupBy(AbuseGroupByConfig.newBuilder().setDerivedEntityId("user_id").build())
        .setThreshold(
            AbuseThresholdConfig.newBuilder()
                .setOperator(AbuseThresholdOperator.ABUSE_THRESHOLD_OPERATOR_GREATER_THAN)
                .setValue(1)
                .build())
        .setTimeWindow(
            AbuseTimeWindow.newBuilder()
                .setLookbackDuration(Duration.newBuilder().setSeconds(60).build())
                .build())
        .build();
  }
}
