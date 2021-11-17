package ai.traceable.anomaly.config.service.exclusion.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeMatcher;
import ai.traceable.anomaly.config.service.exclusion.utils.FilterUtils;
import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyActor;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyActorExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleConfig;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleData;
import ai.traceable.anomaly.config.service.v1.exclusion.DeleteAnomalyExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionType;
import ai.traceable.anomaly.config.service.v1.exclusion.GetAnomalyExclusionRulesRequest;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DeleteAnomalyExclusionRuleHandlerTest {

  private ConfigServiceHandler configServiceHandler;
  private MockGenericConfigService mockConfigService;
  private ConfigServiceBlockingStub configServiceBlockingStub;
  private DeleteAnomalyExclusionRuleHandler deleteAnomalyExclusionRuleHandler;
  private RequestContext requestContext;
  private AnomalyConfigScopeMatcher scopeMatcher;
  private FilterUtils filterUtils;
  private GetAnomalyExclusionRuleHandler getAnomalyExclusionRuleHandler;

  @BeforeEach
  void setUp() {
    mockConfigService = new MockGenericConfigService().mockDelete().mockUpsert().mockGetAll();
    mockConfigService.start();
    configServiceBlockingStub = ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    configServiceHandler =
        spy(
            new ConfigServiceHandler(
                new AnomalyExclusionRuleConfigStore(
                    configServiceBlockingStub, configChangeEventGenerator)));
    deleteAnomalyExclusionRuleHandler = new DeleteAnomalyExclusionRuleHandler(configServiceHandler);
    requestContext = RequestContext.forTenantId("tenant_id");
    scopeMatcher = mock(AnomalyConfigScopeMatcher.class);
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
  void testDeleteRule() {
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

    DeleteAnomalyExclusionRuleRequest deleteRequest =
        DeleteAnomalyExclusionRuleRequest.newBuilder().setRuleId("rule1_id").build();
    deleteAnomalyExclusionRuleHandler.deleteRule(deleteRequest, requestContext);
    List<AnomalyExclusionRuleConfig> remainingConfigs =
        getAnomalyExclusionRuleHandler
            .getRules(GetAnomalyExclusionRulesRequest.newBuilder().build(), requestContext)
            .getConfigsList();
    verify(configServiceHandler).deleteExclusionConfigByRuleId("rule1_id", requestContext);
    assertEquals(1, remainingConfigs.size());
    assertEquals(configRule2, remainingConfigs.get(0));
  }
}
