package ai.traceable.detection.exclusion.config.service.v1.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceConfig;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleStatus;
import ai.traceable.detection.exclusion.config.service.v1.EnvironmentScope;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import ai.traceable.detection.exclusion.config.service.v1.RuleSource;
import com.google.protobuf.Value;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetAllConfigsResponse;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DetectionExclusionRulesStoreTest {

  private DetectionExclusionRulesStore detectionExclusionRulesStore;
  private static final DetectionExclusionRule DEFAULT_DETECTION_EXCLUSION_RULE =
      DetectionExclusionRule.newBuilder()
          .setId("id")
          .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("defaultRule"))
          .build();
  private MockConfigService mockConfigService;
  private Server mockServer;

  @BeforeEach
  void setUp() throws IOException {
    GrpcChannelRegistry grpcChannelRegistry = mock(GrpcChannelRegistry.class);
    String uniqueName = InProcessServerBuilder.generateName();
    ManagedChannel channel = InProcessChannelBuilder.forName(uniqueName).directExecutor().build();
    when(grpcChannelRegistry.forPlaintextAddress(anyString(), anyInt())).thenReturn(channel);

    mockConfigService = new MockConfigService();
    mockServer =
        InProcessServerBuilder.forName(uniqueName)
            .directExecutor()
            .addService(mockConfigService)
            .build()
            .start();
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(channel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    DetectionExclusionConfigServiceConfig config =
        mock(DetectionExclusionConfigServiceConfig.class);
    when(config.getDefaultDetectionExclusionRules())
        .thenReturn(List.of(DEFAULT_DETECTION_EXCLUSION_RULE));
    detectionExclusionRulesStore =
        new DetectionExclusionRulesStore(
            configServiceBlockingStub, mock(ConfigChangeEventGenerator.class), config);
  }

  @AfterEach
  void tearDown() {
    mockServer.shutdown();
  }

  @Test
  void testGetConfigData() {
    RequestContext requestContext = RequestContext.forTenantId("tenantId");

    mockConfigService.setDetectionExclusionRules(List.of());
    // no rule in mongo
    assertEquals(
        List.of(DEFAULT_DETECTION_EXCLUSION_RULE),
        detectionExclusionRulesStore.getAllConfigData(requestContext));

    DetectionExclusionRule rule = DetectionExclusionRule.newBuilder().setId("id1").build();
    mockConfigService.setDetectionExclusionRules(List.of(rule));
    // returns both mongo and default rules
    assertTrue(
        detectionExclusionRulesStore
            .getAllConfigData(requestContext)
            .containsAll(List.of(DEFAULT_DETECTION_EXCLUSION_RULE, rule)));

    rule = DetectionExclusionRule.newBuilder().setId("id").build();
    mockConfigService.setDetectionExclusionRules(List.of(rule));
    // mongo rule overrides default rule
    assertEquals(List.of(rule), detectionExclusionRulesStore.getAllConfigData(requestContext));
  }

  @Test
  void testDataValueConversion() {
    DetectionExclusionRule rule = DetectionExclusionRule.getDefaultInstance();
    Value value = detectionExclusionRulesStore.buildValueFromData(rule);
    assertEquals(rule, detectionExclusionRulesStore.buildDataFromValue(value).get());
  }

  @Test
  void testFilterConfigData() {
    DetectionExclusionRule detectionExclusionRule =
        DetectionExclusionRule.newBuilder()
            .setId("ruleId")
            .setRuleScope(
                DetectionExclusionRuleScope.newBuilder()
                    .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("envId1")))
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .setRuleStatus(
                        DetectionExclusionRuleStatus.newBuilder()
                            .setDisabled(false)
                            .setHidden(false)
                            .setRuleCreationSource(RuleSource.RULE_SOURCE_CUSTOMER)
                            .build())
                    .build())
            .build();

    Optional<DetectionExclusionRule> result =
        detectionExclusionRulesStore.filterConfigData(
            detectionExclusionRule,
            getRulesFilter("envId2", false, false, "ruleId", RuleSource.RULE_SOURCE_CUSTOMER));
    assertTrue(result.isEmpty());

    result =
        detectionExclusionRulesStore.filterConfigData(
            detectionExclusionRule,
            getRulesFilter("envId1", true, false, "ruleId", RuleSource.RULE_SOURCE_CUSTOMER));
    assertTrue(result.isEmpty());

    result =
        detectionExclusionRulesStore.filterConfigData(
            detectionExclusionRule,
            getRulesFilter("envId1", false, true, "ruleId", RuleSource.RULE_SOURCE_CUSTOMER));
    assertTrue(result.isEmpty());

    result =
        detectionExclusionRulesStore.filterConfigData(
            detectionExclusionRule,
            getRulesFilter("envId1", false, false, "ruleId1", RuleSource.RULE_SOURCE_CUSTOMER));
    assertTrue(result.isEmpty());

    result =
        detectionExclusionRulesStore.filterConfigData(
            detectionExclusionRule,
            getRulesFilter("envId1", false, false, "ruleId", RuleSource.RULE_SOURCE_DEFAULT));
    assertTrue(result.isEmpty());

    result =
        detectionExclusionRulesStore.filterConfigData(
            detectionExclusionRule,
            getRulesFilter("envId1", false, false, "ruleId", RuleSource.RULE_SOURCE_CUSTOMER));
    assertEquals(detectionExclusionRule, result.get());
  }

  private GetRulesFilter getRulesFilter(
      String envId, boolean disabled, boolean hidden, String ruleId, RuleSource creationSource) {
    return GetRulesFilter.newBuilder()
        .addRuleIds(ruleId)
        .setRuleScope(
            DetectionExclusionRuleScope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds(envId)))
        .setDisabled(disabled)
        .setHidden(hidden)
        .addRuleCreationSources(creationSource)
        .build();
  }

  private class MockConfigService extends ConfigServiceGrpc.ConfigServiceImplBase {
    private List<DetectionExclusionRule> detectionExclusionRules;

    public void setDetectionExclusionRules(List<DetectionExclusionRule> detectionExclusionRules) {
      this.detectionExclusionRules = detectionExclusionRules;
    }

    @Override
    public void getAllConfigs(
        GetAllConfigsRequest request, StreamObserver<GetAllConfigsResponse> responseObserver) {
      List<ContextSpecificConfig> contextSpecificConfigs =
          detectionExclusionRules.stream()
              .map(
                  rule ->
                      ContextSpecificConfig.newBuilder()
                          .setConfig(detectionExclusionRulesStore.buildValueFromData(rule))
                          .build())
              .collect(Collectors.toUnmodifiableList());
      GetAllConfigsResponse response =
          GetAllConfigsResponse.newBuilder()
              .addAllContextSpecificConfigs(contextSpecificConfigs)
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }
}
