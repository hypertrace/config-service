package ai.traceable.genai.system.discovery.config.service.v1.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.genai.system.discovery.config.service.v1.CreateGenAiSystemDiscoveryRuleRequest;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiSystemDiscoveryRule;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiSystemDiscoveryRuleData;
import ai.traceable.genai.system.discovery.config.service.v1.GetGenAiSystemDiscoveryRulesFilter;
import ai.traceable.genai.system.discovery.config.service.v1.UpdateGenAiSystemDiscoveryRuleRequest;
import ai.traceable.genai.system.discovery.config.service.v1.config.GenAiSystemDiscoveryConfig;
import ai.traceable.genai.system.discovery.config.service.v1.store.GenAiSystemDiscoveryRuleStore;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.DeletedContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GenAiSystemDiscoveryRuleManagerImplTest {

  private static final String DEFAULT_RULE_ID = "default-rule-1";
  private static final String DEFAULT_SERVER_SPAN_RULE_ID = "default-server-span-rule-1";
  private static final String CUSTOM_RULE_ID = "custom-rule-1";
  private static final String NEW_RULE_ID = "new-rule-1";

  private static final GenAiSystemDiscoveryRule DEFAULT_RULE =
      GenAiSystemDiscoveryRule.newBuilder()
          .setRuleId(DEFAULT_RULE_ID)
          .setGenAiSystemDiscoveryRuleData(
              GenAiSystemDiscoveryRuleData.newBuilder().setName("default").setEnabled(true).build())
          .build();

  private static final GenAiSystemDiscoveryRule DEFAULT_SERVER_SPAN_RULE =
      GenAiSystemDiscoveryRule.newBuilder()
          .setRuleId(DEFAULT_SERVER_SPAN_RULE_ID)
          .setGenAiSystemDiscoveryRuleData(
              GenAiSystemDiscoveryRuleData.newBuilder()
                  .setName("server-span")
                  .setEnabled(true)
                  .build())
          .build();

  private static final GenAiSystemDiscoveryRule CUSTOM_RULE =
      GenAiSystemDiscoveryRule.newBuilder()
          .setRuleId(CUSTOM_RULE_ID)
          .setGenAiSystemDiscoveryRuleData(
              GenAiSystemDiscoveryRuleData.newBuilder().setName("custom").setEnabled(true).build())
          .build();

  private static final GenAiSystemDiscoveryRule DISABLED_RULE =
      GenAiSystemDiscoveryRule.newBuilder()
          .setRuleId("disabled-rule")
          .setGenAiSystemDiscoveryRuleData(
              GenAiSystemDiscoveryRuleData.newBuilder()
                  .setName("disabled")
                  .setEnabled(false)
                  .build())
          .build();

  @Mock private UuidGenerator uuidGenerator;
  @Mock private GenAiSystemDiscoveryRuleStore store;
  @Mock private GenAiSystemDiscoveryConfig config;
  @Mock private RequestContext requestContext;
  @Mock private ContextualConfigObject<GenAiSystemDiscoveryRule> upsertedObject;
  @Mock private DeletedContextualConfigObject<GenAiSystemDiscoveryRule> deletedObject;

  private GenAiSystemDiscoveryRuleManagerImpl manager;

  @BeforeEach
  void setUp() {
    manager = new GenAiSystemDiscoveryRuleManagerImpl(uuidGenerator, store, config);
  }

  @Test
  void getRules_mergesDefaultsAndStoredRules_filtersByEnabled() {
    GetGenAiSystemDiscoveryRulesFilter filter =
        GetGenAiSystemDiscoveryRulesFilter.newBuilder().setEnabled(true).build();
    when(config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext))
        .thenReturn(
            Map.of(
                DEFAULT_RULE_ID,
                DEFAULT_RULE,
                DEFAULT_SERVER_SPAN_RULE_ID,
                DEFAULT_SERVER_SPAN_RULE));
    when(store.getAllConfigData(requestContext, filter))
        .thenReturn(List.of(CUSTOM_RULE, DISABLED_RULE));

    List<GenAiSystemDiscoveryRule> rules =
        manager.getGenAiSystemDiscoveryRules(requestContext, filter);

    assertEquals(3, rules.size());
    assertTrue(rules.contains(DEFAULT_RULE));
    assertTrue(rules.contains(DEFAULT_SERVER_SPAN_RULE));
    assertTrue(rules.contains(CUSTOM_RULE));
  }

  @Test
  void getRules_filterDisabled_returnsOnlyDisabledRules() {
    GetGenAiSystemDiscoveryRulesFilter filter =
        GetGenAiSystemDiscoveryRulesFilter.newBuilder().setEnabled(false).build();
    when(config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext))
        .thenReturn(Map.of(DEFAULT_RULE_ID, DEFAULT_RULE));
    when(store.getAllConfigData(requestContext, filter)).thenReturn(List.of(DISABLED_RULE));

    List<GenAiSystemDiscoveryRule> rules =
        manager.getGenAiSystemDiscoveryRules(requestContext, filter);

    assertEquals(List.of(DISABLED_RULE), rules);
  }

  @Test
  void getRules_storedRuleOverridesDefaultWithSameId() {
    GetGenAiSystemDiscoveryRulesFilter filter =
        GetGenAiSystemDiscoveryRulesFilter.newBuilder().setEnabled(true).build();
    GenAiSystemDiscoveryRule overrideRule =
        DEFAULT_RULE.toBuilder()
            .setGenAiSystemDiscoveryRuleData(
                DEFAULT_RULE.getGenAiSystemDiscoveryRuleData().toBuilder()
                    .setName("override")
                    .build())
            .build();
    when(config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext))
        .thenReturn(Map.of(DEFAULT_RULE_ID, DEFAULT_RULE));
    when(store.getAllConfigData(requestContext, filter)).thenReturn(List.of(overrideRule));

    List<GenAiSystemDiscoveryRule> rules =
        manager.getGenAiSystemDiscoveryRules(requestContext, filter);

    assertEquals(List.of(overrideRule), rules);
  }

  @Test
  void getRules_serverSpanRulesEmptyWhenFeatureDisabled() {
    GetGenAiSystemDiscoveryRulesFilter filter =
        GetGenAiSystemDiscoveryRulesFilter.newBuilder().setEnabled(true).build();
    when(config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext))
        .thenReturn(Map.of(DEFAULT_RULE_ID, DEFAULT_RULE));
    when(store.getAllConfigData(requestContext, filter)).thenReturn(List.of());

    List<GenAiSystemDiscoveryRule> rules =
        manager.getGenAiSystemDiscoveryRules(requestContext, filter);

    assertEquals(List.of(DEFAULT_RULE), rules);
  }

  @Test
  void createRule_generatesIdAndUpsertsRule() {
    CreateGenAiSystemDiscoveryRuleRequest request =
        CreateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRuleData(CUSTOM_RULE.getGenAiSystemDiscoveryRuleData())
            .build();
    when(uuidGenerator.generateId(any(String.class))).thenReturn(NEW_RULE_ID);
    GenAiSystemDiscoveryRule expectedRule =
        GenAiSystemDiscoveryRule.newBuilder()
            .setRuleId(NEW_RULE_ID)
            .setGenAiSystemDiscoveryRuleData(CUSTOM_RULE.getGenAiSystemDiscoveryRuleData())
            .build();
    when(store.upsertObject(requestContext, expectedRule)).thenReturn(upsertedObject);
    when(upsertedObject.getData()).thenReturn(expectedRule);

    GenAiSystemDiscoveryRule result =
        manager.createGenAiSystemDiscoveryRule(requestContext, request);

    assertEquals(expectedRule, result);
  }

  @Test
  void updateRule_existingInStore_upsertsWithUpdatedData() {
    GenAiSystemDiscoveryRule updatedRule =
        CUSTOM_RULE.toBuilder()
            .setGenAiSystemDiscoveryRuleData(
                GenAiSystemDiscoveryRuleData.newBuilder().setName("updated").build())
            .build();
    UpdateGenAiSystemDiscoveryRuleRequest request =
        UpdateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRule(updatedRule)
            .build();
    when(store.getData(requestContext, CUSTOM_RULE_ID)).thenReturn(Optional.of(CUSTOM_RULE));
    when(store.upsertObject(requestContext, updatedRule)).thenReturn(upsertedObject);
    when(upsertedObject.getData()).thenReturn(updatedRule);

    GenAiSystemDiscoveryRule result =
        manager.updateGenAiSystemDiscoveryRule(requestContext, request);

    assertEquals(updatedRule, result);
  }

  @Test
  void updateRule_notInStoreButInDefaults_upsertsWithUpdatedData() {
    GenAiSystemDiscoveryRule updatedRule =
        DEFAULT_RULE.toBuilder()
            .setGenAiSystemDiscoveryRuleData(
                GenAiSystemDiscoveryRuleData.newBuilder().setName("updated-default").build())
            .build();
    UpdateGenAiSystemDiscoveryRuleRequest request =
        UpdateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRule(updatedRule)
            .build();
    when(store.getData(requestContext, DEFAULT_RULE_ID)).thenReturn(Optional.empty());
    when(config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext))
        .thenReturn(Map.of(DEFAULT_RULE_ID, DEFAULT_RULE));
    when(store.upsertObject(requestContext, updatedRule)).thenReturn(upsertedObject);
    when(upsertedObject.getData()).thenReturn(updatedRule);

    GenAiSystemDiscoveryRule result =
        manager.updateGenAiSystemDiscoveryRule(requestContext, request);

    assertEquals(updatedRule, result);
  }

  @Test
  void updateRule_notInStoreButInServerSpanDefaults_upsertsWithUpdatedData() {
    GenAiSystemDiscoveryRule updatedRule =
        DEFAULT_SERVER_SPAN_RULE.toBuilder()
            .setGenAiSystemDiscoveryRuleData(
                GenAiSystemDiscoveryRuleData.newBuilder().setName("updated-server-span").build())
            .build();
    UpdateGenAiSystemDiscoveryRuleRequest request =
        UpdateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRule(updatedRule)
            .build();
    when(store.getData(requestContext, DEFAULT_SERVER_SPAN_RULE_ID)).thenReturn(Optional.empty());
    when(config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext))
        .thenReturn(Map.of(DEFAULT_SERVER_SPAN_RULE_ID, DEFAULT_SERVER_SPAN_RULE));
    when(store.upsertObject(requestContext, updatedRule)).thenReturn(upsertedObject);
    when(upsertedObject.getData()).thenReturn(updatedRule);

    GenAiSystemDiscoveryRule result =
        manager.updateGenAiSystemDiscoveryRule(requestContext, request);

    assertEquals(updatedRule, result);
  }

  @Test
  void updateRule_unknownRuleId_throwsNotFound() {
    UpdateGenAiSystemDiscoveryRuleRequest request =
        UpdateGenAiSystemDiscoveryRuleRequest.newBuilder()
            .setGenAiSystemDiscoveryRule(
                GenAiSystemDiscoveryRule.newBuilder().setRuleId("unknown").build())
            .build();
    when(store.getData(requestContext, "unknown")).thenReturn(Optional.empty());
    when(config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext)).thenReturn(Map.of());

    assertThrows(
        StatusRuntimeException.class,
        () -> manager.updateGenAiSystemDiscoveryRule(requestContext, request));
    verify(store, never()).upsertObject(eq(requestContext), any(GenAiSystemDiscoveryRule.class));
  }

  @Test
  void deleteRule_defaultRuleId_throwsInvalidArgument() {
    when(config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext))
        .thenReturn(Map.of(DEFAULT_RULE_ID, DEFAULT_RULE));

    assertThrows(
        StatusRuntimeException.class,
        () -> manager.deleteGenAiSystemDiscoveryRule(requestContext, DEFAULT_RULE_ID));
    verify(store, never()).deleteObject(requestContext, DEFAULT_RULE_ID);
  }

  @Test
  void deleteRule_serverSpanDefaultRuleId_throwsInvalidArgument() {
    when(config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext))
        .thenReturn(Map.of(DEFAULT_SERVER_SPAN_RULE_ID, DEFAULT_SERVER_SPAN_RULE));

    assertThrows(
        StatusRuntimeException.class,
        () -> manager.deleteGenAiSystemDiscoveryRule(requestContext, DEFAULT_SERVER_SPAN_RULE_ID));
    verify(store, never()).deleteObject(requestContext, DEFAULT_SERVER_SPAN_RULE_ID);
  }

  @Test
  void deleteRule_existingCustomRule_deletes() {
    when(config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext)).thenReturn(Map.of());
    when(store.deleteObject(requestContext, CUSTOM_RULE_ID)).thenReturn(Optional.of(deletedObject));

    manager.deleteGenAiSystemDiscoveryRule(requestContext, CUSTOM_RULE_ID);

    verify(store).deleteObject(requestContext, CUSTOM_RULE_ID);
  }

  @Test
  void deleteRule_notFoundInStore_throwsNotFound() {
    when(config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext)).thenReturn(Map.of());
    when(store.deleteObject(requestContext, CUSTOM_RULE_ID)).thenReturn(Optional.empty());
    when(requestContext.buildTrailers()).thenReturn(new Metadata());

    assertThrows(
        StatusRuntimeException.class,
        () -> manager.deleteGenAiSystemDiscoveryRule(requestContext, CUSTOM_RULE_ID));
  }
}
