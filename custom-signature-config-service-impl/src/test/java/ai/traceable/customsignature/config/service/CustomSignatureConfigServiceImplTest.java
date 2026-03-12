package ai.traceable.customsignature.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.customsignature.config.service.migration.CustomSignatureRuleMigrationManager;
import ai.traceable.customsignature.config.service.modsec.ModsecRulesManager;
import ai.traceable.customsignature.config.service.rules.RulesManager;
import ai.traceable.customsignature.config.service.rules.RulesValidator;
import ai.traceable.customsignature.config.service.rules.converter.CustomSignatureEdgeDecisionConverter;
import ai.traceable.customsignature.config.service.v1.BulkDeleteCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.BulkDeleteCustomSignatureRulesResponse;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleResponse;
import ai.traceable.customsignature.config.service.v1.CustomSignatureInlineRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRuleRecord;
import ai.traceable.customsignature.config.service.v1.DeleteCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.DeleteCustomSignatureRuleResponse;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEdgeDecisionRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEdgeDecisionRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEvaluationConfigContextRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEvaluationConfigContextResponse;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleEvaluationPoint;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleResponse;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class CustomSignatureConfigServiceImplTest {
  private static final String TENANT_ID = "default tenant";

  private RulesValidator rulesValidator;
  private RulesManager rulesManager;
  private ModsecRulesManager modsecRulesManager;
  private CustomSignatureEdgeDecisionConverter edgeDecisionConverter;
  private FeatureCachingClient featureCachingClient;
  private CustomSignatureConfigServiceImpl configService;

  @BeforeEach
  void setup() {
    rulesValidator = mock(RulesValidator.class);
    rulesManager = mock(RulesManager.class);
    modsecRulesManager = mock(ModsecRulesManager.class);
    edgeDecisionConverter = mock(CustomSignatureEdgeDecisionConverter.class);
    featureCachingClient = mock(FeatureCachingClient.class);
    CustomSignatureRuleMigrationManager mockRuleMigrationManager =
        mock(CustomSignatureRuleMigrationManager.class);
    configService =
        new CustomSignatureConfigServiceImpl(
            rulesValidator,
            rulesManager,
            modsecRulesManager,
            edgeDecisionConverter,
            mockRuleMigrationManager,
            featureCachingClient);
    when(mockRuleMigrationManager.migrateCreateCustomSignatureRuleRequest(any()))
        .thenAnswer(invocation -> invocation.getArgument(0));
    when(mockRuleMigrationManager.migrateUpdateCustomSignatureRuleRequest(any()))
        .thenAnswer(invocation -> invocation.getArgument(0));
  }

  @Test
  void testGetCustomSignatureEvaluationConfigContextFeatureFlagDisabled_ReturnsDefaultInstance() {
    when(featureCachingClient.isProtectionEngineCustomSignatureEnabledForTenant(any()))
        .thenReturn(false);
    StreamObserver<GetCustomSignatureEvaluationConfigContextResponse> responseObserver =
        mock(StreamObserver.class);
    GetCustomSignatureEvaluationConfigContextRequest request =
        GetCustomSignatureEvaluationConfigContextRequest.getDefaultInstance();

    Runnable runnable =
        () -> configService.getCustomSignatureEvaluationConfigContext(request, responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

    verify(responseObserver, times(1))
        .onNext(
            GetCustomSignatureEvaluationConfigContextResponse.newBuilder()
                .setCustomSignatureEvaluationConfigContext(
                    ai.traceable.protection.engine.config.customsignature.v1
                        .CustomSignatureConfigContext.getDefaultInstance()
                        .toByteString())
                .build());
    verify(responseObserver, times(1)).onCompleted();
    verify(rulesManager, times(0)).getCustomSignatureEvaluationConfigContext(any(), any());
  }

  @Test
  void testGetRules() {
    CustomSignatureRule platformRule =
        CustomSignatureRule.newBuilder()
            .setId("platform-rule-id")
            .setEffect(
                RuleEffect.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM))
            .build();
    CustomSignatureRule agentRule =
        CustomSignatureRule.newBuilder()
            .setId("agent-rule-id")
            .setEffect(
                RuleEffect.newBuilder()
                    .addRuleEvaluationPoints(
                        RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT))
            .build();
    CustomSignatureRule edgeRule =
        CustomSignatureRule.newBuilder()
            .setId("edge-rule-id")
            .setEffect(
                RuleEffect.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE))
            .build();
    CustomSignatureRule multipleEvaluationPointsRule =
        CustomSignatureRule.newBuilder()
            .setId("multi-eval-rule-id")
            .setEffect(
                RuleEffect.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE))
            .build();
    List<CustomSignatureRule> allRules =
        List.of(platformRule, agentRule, edgeRule, multipleEvaluationPointsRule);
    List<CustomSignatureRule> platformRules = List.of(platformRule, multipleEvaluationPointsRule);
    List<CustomSignatureRule> agentRules = List.of(agentRule);
    List<CustomSignatureRule> edgeRules = List.of(edgeRule, multipleEvaluationPointsRule);
    List<CustomSignatureRule> multiEvalPointsRules =
        List.of(platformRule, edgeRule, multipleEvaluationPointsRule);

    doNothing().when(rulesValidator).validate(any(GetCustomSignatureRulesRequest.class));
    StreamObserver<GetCustomSignatureRulesResponse> responseObserver = mock(StreamObserver.class);

    // Case 1: Test with no filter (should return all rules)
    when(rulesManager.getCustomSignatureRuleRecords(any(), any()))
        .thenReturn(
            allRules.stream()
                .map(rule -> CustomSignatureRuleRecord.newBuilder().setRule(rule).build())
                .collect(Collectors.toList()));
    Runnable noFilterRunnable =
        () ->
            configService.getCustomSignatureRules(
                GetCustomSignatureRulesRequest.getDefaultInstance(), responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, noFilterRunnable);
    ArgumentCaptor<GetCustomSignatureRulesResponse> responseCaptor =
        ArgumentCaptor.forClass(GetCustomSignatureRulesResponse.class);
    verify(responseObserver, times(1)).onNext(responseCaptor.capture());
    verify(responseObserver, times(1)).onCompleted();
    assertEquals(allRules, responseCaptor.getValue().getRulesList());
    assertEquals(
        allRules,
        responseCaptor.getValue().getRuleRecordsList().stream()
            .map(CustomSignatureRuleRecord::getRule)
            .collect(Collectors.toList()));

    // Case 2: Test with PLATFORM filter
    reset(responseObserver);
    GetRulesFilter platformFilter =
        GetRulesFilter.newBuilder()
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
            .build();
    when(rulesManager.getCustomSignatureRuleRecords(any(), eq(platformFilter)))
        .thenReturn(
            platformRules.stream()
                .map(rule -> CustomSignatureRuleRecord.newBuilder().setRule(rule).build())
                .collect(Collectors.toList()));

    Runnable platformFilterRunnable =
        () ->
            configService.getCustomSignatureRules(
                GetCustomSignatureRulesRequest.newBuilder()
                    .setFilter(
                        GetRulesFilter.newBuilder()
                            .addRuleEvaluationPoints(
                                RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM))
                    .build(),
                responseObserver);

    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, platformFilterRunnable);
    verify(responseObserver, times(1)).onNext(responseCaptor.capture());
    verify(responseObserver, times(1)).onCompleted();
    assertEquals(platformRules, responseCaptor.getValue().getRulesList());
    assertEquals(
        platformRules,
        responseCaptor.getValue().getRuleRecordsList().stream()
            .map(CustomSignatureRuleRecord::getRule)
            .collect(Collectors.toList()));

    // Case 3: Test with AGENT filter
    reset(responseObserver);
    GetRulesFilter agentFilter =
        GetRulesFilter.newBuilder()
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT)
            .build();
    when(rulesManager.getCustomSignatureRuleRecords(any(), eq(agentFilter)))
        .thenReturn(
            agentRules.stream()
                .map(rule -> CustomSignatureRuleRecord.newBuilder().setRule(rule).build())
                .collect(Collectors.toList()));

    Runnable agentFilterRunnable =
        () ->
            configService.getCustomSignatureRules(
                GetCustomSignatureRulesRequest.newBuilder().setFilter(agentFilter).build(),
                responseObserver);

    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, agentFilterRunnable);
    verify(responseObserver, times(1)).onNext(responseCaptor.capture());
    verify(responseObserver, times(1)).onCompleted();
    assertEquals(agentRules, responseCaptor.getValue().getRulesList());
    assertEquals(
        agentRules,
        responseCaptor.getValue().getRuleRecordsList().stream()
            .map(CustomSignatureRuleRecord::getRule)
            .collect(Collectors.toList()));

    // Case 4: Test with EDGE filter
    reset(responseObserver);
    GetRulesFilter edgeFilter =
        GetRulesFilter.newBuilder()
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();
    when(rulesManager.getCustomSignatureRuleRecords(any(), eq(edgeFilter)))
        .thenReturn(
            edgeRules.stream()
                .map(rule -> CustomSignatureRuleRecord.newBuilder().setRule(rule).build())
                .collect(Collectors.toList()));

    Runnable edgeFilterRunnable =
        () ->
            configService.getCustomSignatureRules(
                GetCustomSignatureRulesRequest.newBuilder().setFilter(edgeFilter).build(),
                responseObserver);

    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, edgeFilterRunnable);
    verify(responseObserver, times(1)).onNext(responseCaptor.capture());
    verify(responseObserver, times(1)).onCompleted();
    assertEquals(edgeRules, responseCaptor.getValue().getRulesList());
    assertEquals(
        edgeRules,
        responseCaptor.getValue().getRuleRecordsList().stream()
            .map(CustomSignatureRuleRecord::getRule)
            .collect(Collectors.toList()));

    // Case 5: Test with multiple evaluation points filter
    reset(responseObserver);
    GetRulesFilter multiEvalPointsFilter =
        GetRulesFilter.newBuilder()
            .addAllRuleEvaluationPoints(
                List.of(
                    RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM,
                    RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE))
            .build();
    when(rulesManager.getCustomSignatureRuleRecords(any(), eq(multiEvalPointsFilter)))
        .thenReturn(
            multiEvalPointsRules.stream()
                .map(rule -> CustomSignatureRuleRecord.newBuilder().setRule(rule).build())
                .collect(Collectors.toList()));

    Runnable multiPointFilterRunnable =
        () ->
            configService.getCustomSignatureRules(
                GetCustomSignatureRulesRequest.newBuilder()
                    .setFilter(multiEvalPointsFilter)
                    .build(),
                responseObserver);

    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, multiPointFilterRunnable);
    verify(responseObserver, times(1)).onNext(responseCaptor.capture());
    verify(responseObserver, times(1)).onCompleted();
    assertEquals(multiEvalPointsRules, responseCaptor.getValue().getRulesList());
    assertEquals(
        multiEvalPointsRules,
        responseCaptor.getValue().getRuleRecordsList().stream()
            .map(CustomSignatureRuleRecord::getRule)
            .collect(Collectors.toList()));

    // Case 6: Test validation failure - exception comes from the validator
    reset(responseObserver);
    reset(rulesManager);
    reset(rulesValidator);
    doThrow(new RuntimeException("Validation failed"))
        .when(rulesValidator)
        .validate(any(GetCustomSignatureRulesRequest.class));

    configService.getCustomSignatureRules(
        GetCustomSignatureRulesRequest.getDefaultInstance(), responseObserver);

    verify(responseObserver, times(1))
        .onError(
            argThat(
                err ->
                    err instanceof RuntimeException
                        && err.getMessage().equals("Validation failed")));

    // Case 7: Test exception handling from the rules manager
    reset(responseObserver);
    reset(rulesValidator);
    doNothing().when(rulesValidator).validate(any(GetCustomSignatureRulesRequest.class));
    when(rulesManager.getCustomSignatureRuleRecords(any(), any()))
        .thenThrow(new RuntimeException("Test exception"));

    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, noFilterRunnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(
                err ->
                    err instanceof RuntimeException && err.getMessage().equals("Test exception")));
  }

  @Test
  void testCreateRule() {
    CustomSignatureRule rule = CustomSignatureRule.newBuilder().setId("id1").build();
    StreamObserver<CreateCustomSignatureRuleResponse> responseObserver = mock(StreamObserver.class);
    CreateCustomSignatureRuleRequest request =
        CreateCustomSignatureRuleRequest.newBuilder().setName("Test Rule").build();

    // Test case 1: Validation fails
    doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
        .when(rulesValidator)
        .validate(any(CreateCustomSignatureRuleRequest.class));
    Runnable runnable = () -> configService.createCustomSignatureRule(request, responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    // Test case 2: Validation passes but rule creation fails
    reset(responseObserver);
    doNothing().when(rulesValidator).validate(any(CreateCustomSignatureRuleRequest.class));
    when(rulesManager.createCustomSignatureRule(any(), any())).thenReturn(Optional.empty());
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INTERNAL));

    // Test case 3: Validation passes and rule creation succeeds
    reset(responseObserver);
    doNothing().when(rulesValidator).validate(any(CreateCustomSignatureRuleRequest.class));
    when(rulesManager.createCustomSignatureRule(any(), any())).thenReturn(Optional.of(rule));
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onNext(CreateCustomSignatureRuleResponse.newBuilder().setRule(rule).build());
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testUpdateRule() {
    CustomSignatureRule rule = CustomSignatureRule.newBuilder().setId("id1").build();

    StreamObserver<UpdateCustomSignatureRuleResponse> responseObserver = mock(StreamObserver.class);
    Runnable runnable =
        () ->
            configService.updateCustomSignatureRule(
                UpdateCustomSignatureRuleRequest.getDefaultInstance(), responseObserver);

    doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
        .when(rulesValidator)
        .validate((UpdateCustomSignatureRuleRequest) any());
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    reset(responseObserver);
    doNothing().when(rulesValidator).validate((UpdateCustomSignatureRuleRequest) any());
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INTERNAL));

    reset(responseObserver);
    when(rulesManager.updateCustomSignatureRule(any(), any())).thenReturn(Optional.of(rule));
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onNext(UpdateCustomSignatureRuleResponse.newBuilder().setRule(rule).build());
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testDeleteRule() throws InvalidProtocolBufferException {
    DeleteCustomSignatureRuleRequest deleteCustomSignatureRuleRequest =
        DeleteCustomSignatureRuleRequest.newBuilder().setId("id").build();
    StreamObserver<DeleteCustomSignatureRuleResponse> responseObserver = mock(StreamObserver.class);
    Runnable runnable =
        () ->
            configService.deleteCustomSignatureRule(
                deleteCustomSignatureRuleRequest, responseObserver);

    doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
        .when(rulesValidator)
        .validate((DeleteCustomSignatureRuleRequest) any());
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    reset(responseObserver);
    doNothing().when(rulesValidator).validate((DeleteCustomSignatureRuleRequest) any());
    when(rulesManager.deleteCustomSignatureRule(any(), eq("id")))
        .thenReturn(Optional.of(CustomSignatureRule.newBuilder().setId("id").build()));
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onNext(DeleteCustomSignatureRuleResponse.getDefaultInstance());
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testBulkDeleteRules() throws InvalidProtocolBufferException {
    BulkDeleteCustomSignatureRulesRequest bulkDeleteRequest =
        BulkDeleteCustomSignatureRulesRequest.newBuilder()
            .addIds("id1")
            .addIds("id2")
            .addIds("id3")
            .build();
    StreamObserver<BulkDeleteCustomSignatureRulesResponse> responseObserver =
        mock(StreamObserver.class);
    Runnable runnable =
        () -> configService.bulkDeleteCustomSignatureRules(bulkDeleteRequest, responseObserver);

    // Test case 1: Validation fails
    doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
        .when(rulesValidator)
        .validate((BulkDeleteCustomSignatureRulesRequest) any());
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    // Test case 2: Validation passes and bulk delete succeeds
    reset(responseObserver);
    doNothing().when(rulesValidator).validate((BulkDeleteCustomSignatureRulesRequest) any());
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(rulesManager, times(1))
        .bulkDeleteCustomSignatureRules(any(), eq(List.of("id1", "id2", "id3")));
    verify(responseObserver, times(1))
        .onNext(BulkDeleteCustomSignatureRulesResponse.getDefaultInstance());
    verify(responseObserver, times(1)).onCompleted();

    // Test case 3: Exception during bulk delete
    reset(responseObserver);
    reset(rulesManager);
    doNothing().when(rulesValidator).validate((BulkDeleteCustomSignatureRulesRequest) any());
    doThrow(new RuntimeException("Delete failed"))
        .when(rulesManager)
        .bulkDeleteCustomSignatureRules(any(), any());
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1)).onError(any());
  }

  @Test
  void testGetCustomSignatureModsecRules() {
    CustomSignatureRule platformRule =
        CustomSignatureRule.newBuilder()
            .setId("platform-rule-id")
            .setEffect(
                RuleEffect.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                    .build())
            .build();
    CustomSignatureRule agentRule =
        CustomSignatureRule.newBuilder()
            .setId("agent-rule-id")
            .setEffect(
                RuleEffect.newBuilder()
                    .addRuleEvaluationPoints(
                        RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT)
                    .build())
            .build();
    CustomSignatureRule edgeRule =
        CustomSignatureRule.newBuilder()
            .setId("edge-rule-id")
            .setEffect(
                RuleEffect.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                    .build())
            .build();
    List<CustomSignatureRule> allRules = List.of(platformRule, agentRule, edgeRule);

    doNothing().when(rulesValidator).validate(any(GetCustomSignatureModsecRulesRequest.class));
    when(rulesManager.getCustomSignatureRules(any(), any())).thenReturn(allRules);
    GetCustomSignatureModsecRulesResponse modsecResponse =
        GetCustomSignatureModsecRulesResponse.newBuilder()
            .addInlineRules(CustomSignatureInlineRule.newBuilder().setRule(edgeRule).build())
            .build();
    when(modsecRulesManager.getModsecRules(
            any(), eq(allRules), any(), eq(false), any(), eq(List.of())))
        .thenReturn(modsecResponse);
    StreamObserver<GetCustomSignatureModsecRulesResponse> responseObserver =
        mock(StreamObserver.class);

    // Case 1: Test successful response
    GetCustomSignatureModsecRulesRequest request =
        GetCustomSignatureModsecRulesRequest.newBuilder()
            .setIncludeAllPartialModsecRules(false)
            .build();
    Runnable successRunnable =
        () -> configService.getCustomSignatureModsecRules(request, responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, successRunnable);
    verify(responseObserver, times(1)).onNext(modsecResponse);
    verify(responseObserver, times(1)).onCompleted();

    // Case 2: Test with filter
    reset(responseObserver);
    GetRulesFilter filter =
        GetRulesFilter.newBuilder()
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();
    List<CustomSignatureRule> edgeRules = List.of(edgeRule);
    when(rulesManager.getCustomSignatureRules(any(), eq(filter))).thenReturn(edgeRules);
    GetCustomSignatureModsecRulesResponse filteredResponse =
        GetCustomSignatureModsecRulesResponse.newBuilder()
            .addInlineRules(CustomSignatureInlineRule.newBuilder().setRule(edgeRule).build())
            .build();
    when(modsecRulesManager.getModsecRules(
            any(), eq(edgeRules), any(), eq(false), any(), eq(List.of())))
        .thenReturn(filteredResponse);
    GetCustomSignatureModsecRulesRequest filteredRequest =
        GetCustomSignatureModsecRulesRequest.newBuilder()
            .setFilter(filter)
            .setIncludeAllPartialModsecRules(false)
            .build();
    Runnable filteredRunnable =
        () -> configService.getCustomSignatureModsecRules(filteredRequest, responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, filteredRunnable);
    verify(responseObserver, times(1)).onNext(filteredResponse);
    verify(responseObserver, times(1)).onCompleted();

    // Case 3: Test validation failure
    reset(responseObserver);
    reset(rulesManager);
    reset(rulesValidator);
    reset(modsecRulesManager);
    doThrow(new RuntimeException("Validation failed"))
        .when(rulesValidator)
        .validate(any(GetCustomSignatureModsecRulesRequest.class));
    configService.getCustomSignatureModsecRules(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(
                err ->
                    err instanceof RuntimeException
                        && err.getMessage().equals("Validation failed")));

    // Case 4: Test exception in modsecRulesManager
    reset(responseObserver);
    reset(rulesValidator);
    reset(modsecRulesManager);
    doNothing().when(rulesValidator).validate(any(GetCustomSignatureModsecRulesRequest.class));
    when(rulesManager.getCustomSignatureRules(any(), any())).thenReturn(allRules);
    when(modsecRulesManager.getModsecRules(any(), any(), any(), anyBoolean(), any(), eq(List.of())))
        .thenThrow(new RuntimeException("ModSec manager error"));
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, successRunnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(
                err ->
                    err instanceof RuntimeException
                        && err.getMessage().equals("ModSec manager error")));
  }

  @Test
  void testGetCustomSignatureEdgeDecisionRules() {
    CustomSignatureRule platformRule =
        CustomSignatureRule.newBuilder()
            .setId("platform-rule-id")
            .setEffect(
                RuleEffect.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                    .build())
            .build();
    CustomSignatureRule edgeRule =
        CustomSignatureRule.newBuilder()
            .setId("edge-rule-id")
            .setEffect(
                RuleEffect.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                    .build())
            .build();
    List<CustomSignatureRule> allRules = List.of(platformRule, edgeRule);
    List<CustomSignatureRule> edgeRules = List.of(edgeRule);

    doNothing()
        .when(rulesValidator)
        .validate(any(GetCustomSignatureEdgeDecisionRulesRequest.class));
    when(rulesManager.getCustomSignatureRules(any(), any())).thenReturn(allRules);

    EdgeDecisionEngineConfig edgeDecisionEngineConfig =
        EdgeDecisionEngineConfig.newBuilder().setId("test-edge-config").build();
    when(edgeDecisionConverter.convert(any())).thenReturn(edgeDecisionEngineConfig);
    StreamObserver<GetCustomSignatureEdgeDecisionRulesResponse> responseObserver =
        mock(StreamObserver.class);

    // Case 1: Test successful response with edge decision enabled
    GetCustomSignatureEdgeDecisionRulesRequest request =
        GetCustomSignatureEdgeDecisionRulesRequest.getDefaultInstance();
    Runnable successRunnable =
        () -> configService.getCustomSignatureEdgeDecisionRules(request, responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, successRunnable);
    verify(responseObserver, times(1))
        .onNext(
            GetCustomSignatureEdgeDecisionRulesResponse.newBuilder()
                .setEdgeDecisionEngineConfig(edgeDecisionEngineConfig)
                .build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 2: Test with filter
    reset(responseObserver);
    GetRulesFilter filter =
        GetRulesFilter.newBuilder()
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();
    when(rulesManager.getCustomSignatureRules(any(), eq(filter))).thenReturn(edgeRules);
    EdgeDecisionEngineConfig filteredEdgeConfig =
        EdgeDecisionEngineConfig.newBuilder().setId("filtered-edge-config").build();
    when(edgeDecisionConverter.convert(any())).thenReturn(filteredEdgeConfig);
    GetCustomSignatureEdgeDecisionRulesRequest filteredRequest =
        GetCustomSignatureEdgeDecisionRulesRequest.newBuilder().setRulesFilter(filter).build();
    Runnable filteredRunnable =
        () -> configService.getCustomSignatureEdgeDecisionRules(filteredRequest, responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, filteredRunnable);
    verify(responseObserver, times(1))
        .onNext(
            GetCustomSignatureEdgeDecisionRulesResponse.newBuilder()
                .setEdgeDecisionEngineConfig(filteredEdgeConfig)
                .build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 3: Test validation failure
    reset(responseObserver);
    reset(rulesManager);
    reset(rulesValidator);
    reset(edgeDecisionConverter);
    doThrow(new RuntimeException("Validation failed"))
        .when(rulesValidator)
        .validate(any(GetCustomSignatureEdgeDecisionRulesRequest.class));
    configService.getCustomSignatureEdgeDecisionRules(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(
                err ->
                    err instanceof RuntimeException
                        && err.getMessage().equals("Validation failed")));
  }
}
