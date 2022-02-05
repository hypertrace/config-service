package ai.traceable.anomaly.config.service.exclusion.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils;
import ai.traceable.anomaly.config.service.exclusion.utils.FilterUtils;
import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyActor;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyActorExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleConfig;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleData;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionType;
import ai.traceable.anomaly.config.service.v1.exclusion.GetAnomalyExclusionRulesRequest;
import ai.traceable.anomaly.config.service.v1.exclusion.GetRulesFilter;
import java.util.List;
import java.util.Set;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GetAnomalyExclusionRuleHandlerTest {
  private ConfigServiceHandler configServiceHandler;
  private MockGenericConfigService mockConfigService;
  private ConfigServiceBlockingStub configServiceBlockingStub;
  private GetAnomalyExclusionRuleHandler getAnomalyExclusionRuleHandler;
  private AnomalyConfigScopeUtils scopeMatcher;
  private FilterUtils filterUtils;
  private RequestContext requestContext;

  @BeforeEach
  void setUp() {
    mockConfigService =
        new MockGenericConfigService().mockDelete().mockUpsert().mockGetAll().mockGet();
    mockConfigService.start();
    requestContext = RequestContext.forTenantId("tenant_id");
    configServiceBlockingStub = ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    configServiceHandler =
        new ConfigServiceHandler(
            new AnomalyExclusionRuleConfigStore(
                configServiceBlockingStub, configChangeEventGenerator));
    scopeMatcher = mock(AnomalyConfigScopeUtils.class);
    filterUtils = mock(FilterUtils.class);
    when(scopeMatcher.isParentScope(any(AnomalyConfigScope.class), any(AnomalyConfigScope.class)))
        .thenReturn(true);
    when(filterUtils.filterRuleIds(any(), anySet())).thenReturn(true);
    when(filterUtils.filterAnomalyActorIds(any(), anySet())).thenReturn(true);
    when(filterUtils.filterEventFamilies(any(), anyList())).thenReturn(true);
    when(filterUtils.filterEventIds(any(), anyList())).thenReturn(true);
    getAnomalyExclusionRuleHandler =
        new GetAnomalyExclusionRuleHandler(configServiceHandler, scopeMatcher, filterUtils);
  }

  @AfterEach
  void teardown() {
    mockConfigService.shutdown();
  }

  @Test
  public void testGetRules() {
    AnomalyExclusionRuleConfig configRule1 =
        AnomalyExclusionRuleConfig.newBuilder()
            .setId("rule1_id")
            .setRuleData(
                AnomalyExclusionRuleData.newBuilder()
                    .setName("rule1")
                    .setEventExclusionInfo(
                        EventExclusionInfo.newBuilder()
                            .setEventExclusionType(
                                EventExclusionType.EVENT_EXCLUSION_TYPE_ALL_EVENTS)
                            .build())
                    .setAnomalyActorExclusionInfo(AnomalyActorExclusionInfo.newBuilder().build())
                    .setAnomalyConfigScope(
                        AnomalyConfigScope.newBuilder()
                            .setServiceScope(
                                AnomalyServiceScope.newBuilder().setId("service_1").build())
                            .build())
                    .build())
            .build();

    AnomalyConfigScope rule2ConfigScope =
        AnomalyConfigScope.newBuilder()
            .setApiScope(
                AnomalyApiScope.newBuilder()
                    .setId("api_1")
                    .setServiceScope(AnomalyServiceScope.newBuilder().setId("service_1").build())
                    .build())
            .build();
    AnomalyExclusionRuleConfig configRule2 =
        AnomalyExclusionRuleConfig.newBuilder()
            .setId("rule2_id")
            .setRuleData(
                AnomalyExclusionRuleData.newBuilder()
                    .setName("rule2")
                    .setEventExclusionInfo(
                        EventExclusionInfo.newBuilder()
                            .setEventExclusionType(
                                EventExclusionType.EVENT_EXCLUSION_TYPE_EVENT_TYPE)
                            .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_SESSION)
                            .setEventTypeId("bola")
                            .build())
                    .setAnomalyActorExclusionInfo(
                        AnomalyActorExclusionInfo.newBuilder()
                            .setAnomalyActor(AnomalyActor.newBuilder().setId("actor_id").build())
                            .build())
                    .setAnomalyConfigScope(rule2ConfigScope)
                    .build())
            .build();

    configServiceHandler.upsertExclusionConfigByRuleId(configRule1, requestContext);
    configServiceHandler.upsertExclusionConfigByRuleId(configRule2, requestContext);
    // Filter on Rule-id
    when(filterUtils.filterRuleIds(configRule1, Set.of("rule1_id"))).thenReturn(true);
    when(filterUtils.filterRuleIds(configRule2, Set.of("rule1_id"))).thenReturn(false);
    List<AnomalyExclusionRuleConfig> configs =
        getAnomalyExclusionRuleHandler
            .getRules(
                GetAnomalyExclusionRulesRequest.newBuilder()
                    .setFilter(GetRulesFilter.newBuilder().addAllRuleIds(List.of("rule1_id")))
                    .build(),
                requestContext)
            .getConfigsList();
    assertEquals(1, configs.size());
    assertEquals(configRule1, configs.get(0));

    // Filter on ActorIds
    when(filterUtils.filterAnomalyActorIds(configRule2, Set.of("some_id"))).thenReturn(false);
    configs =
        getAnomalyExclusionRuleHandler
            .getRules(
                GetAnomalyExclusionRulesRequest.newBuilder()
                    .setFilter(
                        GetRulesFilter.newBuilder().addAllAnomalyActorIds(List.of("some_id")))
                    .build(),
                requestContext)
            .getConfigsList();
    assertEquals(1, configs.size());
    assertEquals(configRule1, configs.get(0));

    // Filter on ActorIds
    configs =
        getAnomalyExclusionRuleHandler
            .getRules(
                GetAnomalyExclusionRulesRequest.newBuilder()
                    .setFilter(GetRulesFilter.newBuilder().addAllEventTypeIds(List.of("hola")))
                    .build(),
                requestContext)
            .getConfigsList();
    assertEquals(2, configs.size());
    when(filterUtils.filterEventIds(configRule2, List.of("hola"))).thenReturn(false);
    configs =
        getAnomalyExclusionRuleHandler
            .getRules(
                GetAnomalyExclusionRulesRequest.newBuilder()
                    .setFilter(GetRulesFilter.newBuilder().addAllEventTypeIds(List.of("hola")))
                    .build(),
                requestContext)
            .getConfigsList();
    assertEquals(1, configs.size());
    assertEquals(configRule1, configs.get(0));

    // Filter event families
    configs =
        getAnomalyExclusionRuleHandler
            .getRules(
                GetAnomalyExclusionRulesRequest.newBuilder()
                    .setFilter(
                        GetRulesFilter.newBuilder()
                            .addEventFamilies(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_SESSION))
                    .build(),
                requestContext)
            .getConfigsList();
    assertEquals(2, configs.size());
    when(filterUtils.filterEventFamilies(
            configRule2, List.of(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_SESSION)))
        .thenReturn(false);
    configs =
        getAnomalyExclusionRuleHandler
            .getRules(
                GetAnomalyExclusionRulesRequest.newBuilder()
                    .setFilter(GetRulesFilter.newBuilder().addAllEventTypeIds(List.of("hola")))
                    .build(),
                requestContext)
            .getConfigsList();
    assertEquals(1, configs.size());
    assertEquals(configRule1, configs.get(0));

    // filter anomaly scope
    AnomalyConfigScope requiredScope =
        AnomalyConfigScope.newBuilder()
            .setApiScope(AnomalyApiScope.newBuilder().setId("api_id").build())
            .build();
    configs =
        getAnomalyExclusionRuleHandler
            .getRules(
                GetAnomalyExclusionRulesRequest.newBuilder()
                    .setFilter(
                        GetRulesFilter.newBuilder().setAnomalyConfigScope(requiredScope).build())
                    .build(),
                requestContext)
            .getConfigsList();
    assertEquals(2, configs.size());
    when(scopeMatcher.isParentScope(rule2ConfigScope, requiredScope)).thenReturn(false);
    configs =
        getAnomalyExclusionRuleHandler
            .getRules(
                GetAnomalyExclusionRulesRequest.newBuilder()
                    .setFilter(
                        GetRulesFilter.newBuilder().setAnomalyConfigScope(requiredScope).build())
                    .build(),
                requestContext)
            .getConfigsList();
    assertEquals(1, configs.size());
    assertEquals(configRule1, configs.get(0));
  }
}
