package ai.traceable.ratelimiting.config.service;

import static ai.traceable.ratelimiting.config.service.v1.RateLimitedEntityType.RATE_LIMITED_ENTITY_TYPE_API;
import static ai.traceable.ratelimiting.service.RateLimitingConfigConstants.RATE_LIMITING_NAMESPACE;
import static ai.traceable.ratelimiting.service.RateLimitingConfigConstants.RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME;
import static ai.traceable.ratelimiting.service.RateLimitingConfigConstants.RULE_RATE_LIMITED_ENTITY_ASSOCIATION_RESOURCE_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ai.traceable.ratelimiting.config.service.v1.CreateRateLimitingRuleConfig;
import ai.traceable.ratelimiting.config.service.v1.CreateRuleConfigRequest;
import ai.traceable.ratelimiting.config.service.v1.CreateRuleConfigResponse;
import ai.traceable.ratelimiting.config.service.v1.CreateRuleRateLimitedEntityAssociationRequest;
import ai.traceable.ratelimiting.config.service.v1.CreateRuleRateLimitedEntityAssociationResponse;
import ai.traceable.ratelimiting.config.service.v1.DeleteRuleConfigRequest;
import ai.traceable.ratelimiting.config.service.v1.DeleteRuleRateLimitedEntityAssociationRequest;
import ai.traceable.ratelimiting.config.service.v1.GetAllRateLimitingRulesRequest;
import ai.traceable.ratelimiting.config.service.v1.GetAllRateLimitingRulesResponse;
import ai.traceable.ratelimiting.config.service.v1.GetRateLimitingConfigsForEntityRequest;
import ai.traceable.ratelimiting.config.service.v1.GetRateLimitingConfigsForEntityResponse;
import ai.traceable.ratelimiting.config.service.v1.RateLimitedEntity;
import ai.traceable.ratelimiting.config.service.v1.RateLimitingRuleConfig;
import ai.traceable.ratelimiting.config.service.v1.RateLimitingRuleWithRateLimitedEntities;
import ai.traceable.ratelimiting.config.service.v1.RuleViolationAction;
import ai.traceable.ratelimiting.config.service.v1.UpdateRuleConfigRequest;
import ai.traceable.ratelimiting.config.service.v1.UpdateRuleConfigResponse;
import ai.traceable.ratelimiting.service.RateLimitingConfigServiceImpl;
import ai.traceable.ratelimiting.service.RateLimitingConfigServiceUtils;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.tuple.Triple;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class RateLimitingConfigServiceImplTest {

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private RateLimitingConfigServiceImpl rateLimitingConfigService;
  private final MockConfigServiceImpl mockConfigService = new MockConfigServiceImpl();
  static final String TENANT_ID = "tenant1";

  @BeforeEach
  void setup() throws IOException {
    String serverName = InProcessServerBuilder.generateName();
    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(mockConfigService)
            .build()
            .start());

    ManagedChannel managedChannel =
        grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build());
    Config config = ConfigFactory.parseMap(Map.of("rate.limiting.config.service", Map.of()));
    rateLimitingConfigService = new RateLimitingConfigServiceImpl(managedChannel, config);
  }

  @Test
  void getAllRateLimitingRules() {
    StreamObserver<GetAllRateLimitingRulesResponse> responseObserver = mock(StreamObserver.class);
    mockConfigService.setRateLimitingRuleConfigs(getRuleConfigMap());
    mockConfigService.setRuleEntityAssociations(getRuleRateLimitedEntityAssociationMap());
    GetAllRateLimitingRulesRequest request = GetAllRateLimitingRulesRequest.newBuilder().build();

    Runnable runnable =
        () -> rateLimitingConfigService.getAllRateLimitingRules(request, responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    ArgumentCaptor<GetAllRateLimitingRulesResponse> argumentCaptor =
        ArgumentCaptor.forClass(GetAllRateLimitingRulesResponse.class);
    verify(responseObserver, times(1)).onNext(argumentCaptor.capture());
    verify(responseObserver, times(1)).onCompleted();
    verify(responseObserver, never()).onError(any(Throwable.class));
    Assertions.assertEquals(1, argumentCaptor.getValue().getRulesList().size());
    RateLimitingRuleWithRateLimitedEntities ruleResponse =
        argumentCaptor.getValue().getRulesList().get(0);
    Assertions.assertEquals("entity1", ruleResponse.getEntitiesAssociated(0).getEntityId());
    Assertions.assertEquals("ruleId1", ruleResponse.getRule().getRuleId());
  }

  @Test
  void getRateLimitConfigForEntityWithRuleConfig() {
    StreamObserver<GetRateLimitingConfigsForEntityResponse> responseObserver =
        mock(StreamObserver.class);
    mockConfigService.setRateLimitingRuleConfigs(getRuleConfigMap());
    mockConfigService.setRuleEntityAssociations(getRuleRateLimitedEntityAssociationMap());

    GetRateLimitingConfigsForEntityRequest request =
        GetRateLimitingConfigsForEntityRequest.newBuilder()
            .setEntity(
                RateLimitedEntity.newBuilder()
                    .setEntityType(RATE_LIMITED_ENTITY_TYPE_API)
                    .setEntityId("entity1")
                    .build())
            .build();
    Runnable runnable =
        () -> rateLimitingConfigService.getRateLimitingConfigsForEntity(request, responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    ArgumentCaptor<GetRateLimitingConfigsForEntityResponse> argumentCaptor =
        ArgumentCaptor.forClass(GetRateLimitingConfigsForEntityResponse.class);
    verify(responseObserver, times(1)).onNext(argumentCaptor.capture());
    verify(responseObserver, times(1)).onCompleted();
    verify(responseObserver, never()).onError(any(Throwable.class));
    RateLimitingRuleConfig ruleConfig = argumentCaptor.getValue().getRuleList().get(0);
    Assertions.assertEquals("ruleId1", ruleConfig.getRuleId());
  }

  @Test
  void getRateLimitConfigForEntityWithNoRuleConfig() {
    StreamObserver<GetRateLimitingConfigsForEntityResponse> responseObserver =
        mock(StreamObserver.class);
    mockConfigService.setRateLimitingRuleConfigs(getRuleConfigMap());
    mockConfigService.setRuleEntityAssociations(getRuleRateLimitedEntityAssociationMap());

    GetRateLimitingConfigsForEntityRequest request =
        GetRateLimitingConfigsForEntityRequest.newBuilder()
            .setEntity(
                RateLimitedEntity.newBuilder()
                    .setEntityType(RATE_LIMITED_ENTITY_TYPE_API)
                    .setEntityId("entity2")
                    .build())
            .build();
    Runnable runnable =
        () -> rateLimitingConfigService.getRateLimitingConfigsForEntity(request, responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    ArgumentCaptor<GetRateLimitingConfigsForEntityResponse> argumentCaptor =
        ArgumentCaptor.forClass(GetRateLimitingConfigsForEntityResponse.class);
    verify(responseObserver, times(1)).onNext(argumentCaptor.capture());
    verify(responseObserver, times(1)).onCompleted();
    verify(responseObserver, never()).onError(any(Throwable.class));
    Assertions.assertTrue(argumentCaptor.getValue().getRuleList().isEmpty());
  }

  @Test
  void createRuleRateLimitedEntityAssociation() {
    StreamObserver<CreateRuleRateLimitedEntityAssociationResponse> responseObserver =
        mock(StreamObserver.class);
    RateLimitedEntity rateLimitedEntity =
        RateLimitedEntity.newBuilder()
            .setEntityId("entity1")
            .setEntityType(RATE_LIMITED_ENTITY_TYPE_API)
            .build();
    CreateRuleRateLimitedEntityAssociationRequest request =
        CreateRuleRateLimitedEntityAssociationRequest.newBuilder()
            .setRuleId("ruleId1")
            .setEntity(rateLimitedEntity)
            .build();
    Runnable runnable =
        () ->
            rateLimitingConfigService.createRuleRateLimitedEntityAssociation(
                request, responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    Assertions.assertEquals(1, mockConfigService.getRuleEntityAssociations().size());
    ArgumentCaptor<CreateRuleRateLimitedEntityAssociationResponse> argumentCaptor =
        ArgumentCaptor.forClass(CreateRuleRateLimitedEntityAssociationResponse.class);
    verify(responseObserver, times(1)).onNext(argumentCaptor.capture());
    verify(responseObserver, times(1)).onCompleted();
    verify(responseObserver, never()).onError(any(Throwable.class));
    String savedRuleId =
        mockConfigService
            .getRuleEntityAssociations()
            .entrySet()
            .iterator()
            .next()
            .getValue()
            .getStringValue();
    Triple<String, String, String> resourceInfo =
        mockConfigService.getRuleEntityAssociations().entrySet().iterator().next().getKey();
    Assertions.assertEquals("ruleId1", savedRuleId);
    Assertions.assertEquals("RATE_LIMITED_ENTITY_TYPE_API:entity1", resourceInfo.getRight());
    Assertions.assertEquals(
        RULE_RATE_LIMITED_ENTITY_ASSOCIATION_RESOURCE_NAME, resourceInfo.getLeft());
    Assertions.assertEquals(RATE_LIMITING_NAMESPACE, resourceInfo.getMiddle());
  }

  @Test
  void deleteRuleRateLimitedEntityAssociation() {
    String ruleId = "ruleId1";
    mockConfigService.setRuleEntityAssociations(getRuleRateLimitedEntityAssociationMap());
    RateLimitedEntity rateLimitedEntityToDelete =
        RateLimitedEntity.newBuilder()
            .setEntityId("entity1")
            .setEntityType(RATE_LIMITED_ENTITY_TYPE_API)
            .build();
    DeleteRuleRateLimitedEntityAssociationRequest request =
        DeleteRuleRateLimitedEntityAssociationRequest.newBuilder()
            .setRuleId(ruleId)
            .setEntity(rateLimitedEntityToDelete)
            .build();
    Runnable runnable =
        () ->
            rateLimitingConfigService.deleteRuleRateLimitedEntityAssociation(
                request, mock(StreamObserver.class));
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    Assertions.assertEquals(0, mockConfigService.getRuleEntityAssociations().size());
  }

  @Test
  void updateRuleConfig() throws InvalidProtocolBufferException {
    StreamObserver<UpdateRuleConfigResponse> responseObserver = mock(StreamObserver.class);
    mockConfigService.setRateLimitingRuleConfigs(getRuleConfigMap());
    RateLimitingRuleConfig rateLimitingRuleConfig =
        RateLimitingRuleConfig.newBuilder()
            .setRuleId("ruleId1")
            .setRuleName("changedName")
            .setDescription("this is rule 1")
            .setMaxCallCountAllowed(10)
            .setMaxCallCountDurationMillis(10000)
            .setRuleViolationAction(RuleViolationAction.RULE_VIOLATION_ACTION_SUSPEND)
            .setSuspendDurationMillis(1000)
            .setDisabled(true)
            .build();
    UpdateRuleConfigRequest request =
        UpdateRuleConfigRequest.newBuilder().setRule(rateLimitingRuleConfig).build();

    Runnable runnable = () -> rateLimitingConfigService.updateRuleConfig(request, responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    Assertions.assertEquals(1, mockConfigService.getRateLimitingRuleConfigs().size());
    Entry<Triple<String, String, String>, Value> updatedEntry =
        mockConfigService.getRateLimitingRuleConfigs().entrySet().iterator().next();
    Assertions.assertEquals(
        rateLimitingRuleConfig,
        RateLimitingConfigServiceUtils.toRateLimitingRuleConfig(updatedEntry.getValue()));

    ArgumentCaptor<UpdateRuleConfigResponse> argumentCaptor =
        ArgumentCaptor.forClass(UpdateRuleConfigResponse.class);
    verify(responseObserver, times(1)).onNext(argumentCaptor.capture());
    verify(responseObserver, never()).onError(any(Throwable.class));
    RateLimitingRuleConfig createdRuleConfig = argumentCaptor.getValue().getRule();
    Assertions.assertEquals("changedName", createdRuleConfig.getRuleName());
    Assertions.assertEquals("ruleId1", createdRuleConfig.getRuleId());
  }

  @Test
  void deleteRuleConfig() {
    mockConfigService.setRateLimitingRuleConfigs(getRuleConfigMap());
    mockConfigService.setRuleEntityAssociations(getRuleRateLimitedEntityAssociationMap());
    DeleteRuleConfigRequest request =
        DeleteRuleConfigRequest.newBuilder().setRuleId("ruleId1").build();
    Runnable runnable =
        () -> rateLimitingConfigService.deleteRuleConfig(request, mock(StreamObserver.class));
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    Assertions.assertEquals(0, mockConfigService.getRateLimitingRuleConfigs().size());
    // since ruleId1 is deleted, association entry corresponding to rule1 should  also be deleted.
    Assertions.assertEquals(0, mockConfigService.getRuleEntityAssociations().size());
  }

  @Test
  void createRuleConfig() {
    StreamObserver<CreateRuleConfigResponse> responseObserver = mock(StreamObserver.class);
    CreateRateLimitingRuleConfig createRateLimitingRuleConfig =
        CreateRateLimitingRuleConfig.newBuilder()
            .setRuleName("rule1")
            .setDescription("this is rule 1")
            .setMaxCallCountAllowed(10)
            .setMaxCallCountDurationMillis(10000)
            .setRuleViolationAction(RuleViolationAction.RULE_VIOLATION_ACTION_SUSPEND)
            .setSuspendDurationMillis(1000)
            .build();
    CreateRuleConfigRequest request =
        CreateRuleConfigRequest.newBuilder().setRule(createRateLimitingRuleConfig).build();
    Runnable runnable = () -> rateLimitingConfigService.createRuleConfig(request, responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    System.out.println(mockConfigService.getRateLimitingRuleConfigs().size());
    ArgumentCaptor<CreateRuleConfigResponse> argumentCaptor =
        ArgumentCaptor.forClass(CreateRuleConfigResponse.class);
    verify(responseObserver, times(1)).onNext(argumentCaptor.capture());
    verify(responseObserver, never()).onError(any(Throwable.class));
    RateLimitingRuleConfig createdRuleConfig = argumentCaptor.getValue().getRule();
    Assertions.assertEquals(createdRuleConfig.getRuleName(), "rule1");
    Assertions.assertNotNull(createdRuleConfig.getRuleId());
  }

  @Test
  void createRuleConfig_maxCallCountDurationOutOfLimit() {
    StreamObserver<CreateRuleConfigResponse> responseObserver = mock(StreamObserver.class);
    CreateRateLimitingRuleConfig createRateLimitingRuleConfig =
        CreateRateLimitingRuleConfig.newBuilder()
            .setRuleName("rule1")
            .setDescription("this is rule 1")
            .setMaxCallCountAllowed(10L)
            .setMaxCallCountDurationMillis(10000000000000L)
            .setRuleViolationAction(RuleViolationAction.RULE_VIOLATION_ACTION_SUSPEND)
            .setSuspendDurationMillis(1000L)
            .build();
    CreateRuleConfigRequest request =
        CreateRuleConfigRequest.newBuilder().setRule(createRateLimitingRuleConfig).build();
    Runnable runnable = () -> rateLimitingConfigService.createRuleConfig(request, responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    System.out.println(mockConfigService.getRateLimitingRuleConfigs().size());
    ArgumentCaptor<CreateRuleConfigResponse> argumentCaptor =
        ArgumentCaptor.forClass(CreateRuleConfigResponse.class);
    verify(responseObserver, never()).onNext(argumentCaptor.capture());
    verify(responseObserver, times(1)).onError(any(Throwable.class));
    Assertions.assertEquals(0, mockConfigService.getRateLimitingRuleConfigs().size());
  }

  private Map<Triple<String, String, String>, Value> getRuleRateLimitedEntityAssociationMap() {
    Triple<String, String, String> resourceInfo =
        Triple.of(
            RULE_RATE_LIMITED_ENTITY_ASSOCIATION_RESOURCE_NAME,
            RATE_LIMITING_NAMESPACE,
            "RATE_LIMITED_ENTITY_TYPE_API:entity1");
    Map<Triple<String, String, String>, Value> valueMap = new HashMap<>();
    valueMap.put(resourceInfo, Value.newBuilder().setStringValue("ruleId1").build());
    return valueMap;
  }

  private Map<Triple<String, String, String>, Value> getRuleConfigMap() {
    Triple<String, String, String> resourceInfo =
        Triple.of(RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME, RATE_LIMITING_NAMESPACE, "ruleId1");
    Value configValue = getRateLimitingConfigValue();
    Map<Triple<String, String, String>, Value> valueMap = new HashMap<>();
    valueMap.put(resourceInfo, configValue);
    return valueMap;
  }

  private Value getRateLimitingConfigValue() {
    String ruleId = "ruleId1";
    String ruleName = "rule1";
    String description = "some description";
    long maxCallsCount = 10;
    long maxCallCountDuration = 10000;
    RuleViolationAction action = RuleViolationAction.RULE_VIOLATION_ACTION_SUSPEND;
    long suspendDurationMillis = 1000;

    Struct struct =
        Struct.newBuilder()
            .putFields("ruleId", Value.newBuilder().setStringValue(ruleId).build())
            .putFields("ruleName", Value.newBuilder().setStringValue(ruleName).build())
            .putFields("description", Value.newBuilder().setStringValue(description).build())
            .putFields(
                "maxCallCountAllowed", Value.newBuilder().setNumberValue(maxCallsCount).build())
            .putFields(
                "maxCallCountDurationMillis",
                Value.newBuilder().setNumberValue(maxCallCountDuration).build())
            .putFields("disabled", Value.newBuilder().setBoolValue(false).build())
            .putFields(
                "ruleViolationAction", Value.newBuilder().setStringValue(action.name()).build())
            .putFields(
                "suspendDurationMillis",
                Value.newBuilder().setNumberValue(suspendDurationMillis).build())
            .build();
    return Value.newBuilder().setStructValue(struct).build();
  }
}
