package ai.traceable.customsignature.config.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
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
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleResponse;
import ai.traceable.customsignature.config.service.v1.CustomSignatureInlineRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.DeleteCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.DeleteCustomSignatureRuleResponse;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEdgeDecisionRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEdgeDecisionRulesResponse;
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
import java.util.Objects;
import java.util.Optional;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CustomSignatureConfigServiceImplTest {
  private static final String TENANT_ID = "default tenant";

  private RulesValidator rulesValidator;
  private RulesManager rulesManager;
  private ModsecRulesManager modsecRulesManager;
  private CustomSignatureConfigServiceConfig customSignatureConfigServiceConfig;
  private FeatureCachingClient featureCachingClient;
  private CustomSignatureEdgeDecisionConverter edgeDecisionConverter;
  private CustomSignatureConfigServiceImpl configService;

  @BeforeEach
  void setup() {
    rulesValidator = mock(RulesValidator.class);
    rulesManager = mock(RulesManager.class);
    modsecRulesManager = mock(ModsecRulesManager.class);
    edgeDecisionConverter = mock(CustomSignatureEdgeDecisionConverter.class);
    featureCachingClient = mock(FeatureCachingClient.class);
    customSignatureConfigServiceConfig = mock(CustomSignatureConfigServiceConfig.class);
    CustomSignatureRuleMigrationManager mockRuleMigrationManager =
        mock(CustomSignatureRuleMigrationManager.class);
    when(featureCachingClient.isEdgeDecisionEnabledForTenant(any())).thenReturn(true);
    when(customSignatureConfigServiceConfig.isEdsConversionEnabled()).thenReturn(true);
    configService =
        new CustomSignatureConfigServiceImpl(
            rulesValidator,
            rulesManager,
            modsecRulesManager,
            edgeDecisionConverter,
            featureCachingClient,
            customSignatureConfigServiceConfig,
            mockRuleMigrationManager);
    when(mockRuleMigrationManager.migrateRules(any()))
        .thenAnswer(invocation -> invocation.getArgument(0));
    when(mockRuleMigrationManager.migrateCreateCustomSignatureRuleRequest(any()))
        .thenAnswer(invocation -> invocation.getArgument(0));
    when(mockRuleMigrationManager.migrateUpdateCustomSignatureRuleRequest(any()))
        .thenAnswer(invocation -> invocation.getArgument(0));
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

    when(rulesValidator.validate(any(GetCustomSignatureRulesRequest.class))).thenReturn(Status.OK);
    StreamObserver<GetCustomSignatureRulesResponse> responseObserver = mock(StreamObserver.class);

    // Case 1: Test with no filter (should return all rules)
    when(rulesManager.getCustomSignatureRules(any(), any())).thenReturn(allRules);
    Runnable noFilterRunnable =
        () ->
            configService.getCustomSignatureRules(
                GetCustomSignatureRulesRequest.getDefaultInstance(), responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, noFilterRunnable);
    verify(responseObserver, times(1))
        .onNext(GetCustomSignatureRulesResponse.newBuilder().addAllRules(allRules).build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 2: Test with PLATFORM filter
    reset(responseObserver);
    GetRulesFilter platformFilter =
        GetRulesFilter.newBuilder()
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
            .build();
    when(rulesManager.getCustomSignatureRules(any(), eq(platformFilter))).thenReturn(platformRules);

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
    verify(responseObserver, times(1))
        .onNext(GetCustomSignatureRulesResponse.newBuilder().addAllRules(platformRules).build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 3: Test with AGENT filter
    reset(responseObserver);
    GetRulesFilter agentFilter =
        GetRulesFilter.newBuilder()
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT)
            .build();
    when(rulesManager.getCustomSignatureRules(any(), eq(agentFilter))).thenReturn(agentRules);

    Runnable agentFilterRunnable =
        () ->
            configService.getCustomSignatureRules(
                GetCustomSignatureRulesRequest.newBuilder().setFilter(agentFilter).build(),
                responseObserver);

    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, agentFilterRunnable);
    verify(responseObserver, times(1))
        .onNext(GetCustomSignatureRulesResponse.newBuilder().addAllRules(agentRules).build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 4: Test with EDGE filter
    reset(responseObserver);
    GetRulesFilter edgeFilter =
        GetRulesFilter.newBuilder()
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();
    when(rulesManager.getCustomSignatureRules(any(), eq(edgeFilter))).thenReturn(edgeRules);

    Runnable edgeFilterRunnable =
        () ->
            configService.getCustomSignatureRules(
                GetCustomSignatureRulesRequest.newBuilder().setFilter(edgeFilter).build(),
                responseObserver);

    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, edgeFilterRunnable);
    verify(responseObserver, times(1))
        .onNext(GetCustomSignatureRulesResponse.newBuilder().addAllRules(edgeRules).build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 5: Test with multiple evaluation points filter
    reset(responseObserver);
    GetRulesFilter multiEvalPointsFilter =
        GetRulesFilter.newBuilder()
            .addAllRuleEvaluationPoints(
                List.of(
                    RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM,
                    RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE))
            .build();
    when(rulesManager.getCustomSignatureRules(any(), eq(multiEvalPointsFilter)))
        .thenReturn(multiEvalPointsRules);

    Runnable multiPointFilterRunnable =
        () ->
            configService.getCustomSignatureRules(
                GetCustomSignatureRulesRequest.newBuilder()
                    .setFilter(multiEvalPointsFilter)
                    .build(),
                responseObserver);

    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, multiPointFilterRunnable);
    verify(responseObserver, times(1))
        .onNext(
            GetCustomSignatureRulesResponse.newBuilder().addAllRules(multiEvalPointsRules).build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 6: Test validation failure - exception comes from the validator
    reset(responseObserver);
    reset(rulesManager);
    reset(rulesValidator);
    when(rulesValidator.validate(any(GetCustomSignatureRulesRequest.class)))
        .thenThrow(new RuntimeException("Validation failed"));

    configService.getCustomSignatureRules(
        GetCustomSignatureRulesRequest.getDefaultInstance(), responseObserver);

    verify(responseObserver, times(1))
        .onError(
            argThat(
                err ->
                    Status.fromThrowable(err).getCode() == Status.Code.INTERNAL
                        && Objects.equals(
                            Status.fromThrowable(err).getDescription(),
                            "Unable to fetch custom signature rules")));

    // Case 7: Test exception handling from the rules manager
    reset(responseObserver);
    reset(rulesValidator);
    when(rulesValidator.validate(any(GetCustomSignatureRulesRequest.class))).thenReturn(Status.OK);
    when(rulesManager.getCustomSignatureRules(any(), any()))
        .thenThrow(new RuntimeException("Test exception"));

    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, noFilterRunnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(
                err ->
                    Status.fromThrowable(err).getCode() == Status.Code.INTERNAL
                        && Objects.equals(
                            Status.fromThrowable(err).getDescription(),
                            "Unable to fetch custom signature rules")));
  }

  @Test
  void testCreateRule() {
    CustomSignatureRule rule = CustomSignatureRule.newBuilder().setId("id1").build();
    StreamObserver<CreateCustomSignatureRuleResponse> responseObserver = mock(StreamObserver.class);
    CreateCustomSignatureRuleRequest request =
        CreateCustomSignatureRuleRequest.newBuilder().setName("Test Rule").build();

    // Test case 1: Validation fails
    when(rulesValidator.validate(any(CreateCustomSignatureRuleRequest.class)))
        .thenReturn(Status.INVALID_ARGUMENT);
    Runnable runnable = () -> configService.createCustomSignatureRule(request, responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    // Test case 2: Validation passes but rule creation fails
    reset(responseObserver);
    when(rulesValidator.validate(any(CreateCustomSignatureRuleRequest.class)))
        .thenReturn(Status.OK);
    when(rulesManager.createCustomSignatureRule(any(), any())).thenReturn(Optional.empty());
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INTERNAL));

    // Test case 3: Validation passes and rule creation succeeds
    reset(responseObserver);
    when(rulesValidator.validate(any(CreateCustomSignatureRuleRequest.class)))
        .thenReturn(Status.OK);
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

    when(rulesValidator.validate((UpdateCustomSignatureRuleRequest) any()))
        .thenReturn(Status.INVALID_ARGUMENT);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    reset(responseObserver);
    when(rulesValidator.validate((UpdateCustomSignatureRuleRequest) any())).thenReturn(Status.OK);
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

    when(rulesValidator.validate((DeleteCustomSignatureRuleRequest) any()))
        .thenReturn(Status.INVALID_ARGUMENT);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    reset(responseObserver);
    when(rulesValidator.validate((DeleteCustomSignatureRuleRequest) any())).thenReturn(Status.OK);
    when(rulesManager.deleteCustomSignatureRule(any(), eq("id")))
        .thenReturn(Optional.of(CustomSignatureRule.newBuilder().setId("id").build()));
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onNext(DeleteCustomSignatureRuleResponse.getDefaultInstance());
    verify(responseObserver, times(1)).onCompleted();
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

    when(rulesValidator.validate(any(GetCustomSignatureModsecRulesRequest.class)))
        .thenReturn(Status.OK);
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
    when(rulesValidator.validate(any(GetCustomSignatureModsecRulesRequest.class)))
        .thenThrow(new RuntimeException("Validation failed"));
    configService.getCustomSignatureModsecRules(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(
                err ->
                    Status.fromThrowable(err).getCode() == Status.Code.INTERNAL
                        && Objects.equals(
                            Status.fromThrowable(err).getDescription(),
                            "Unable to fetch modsec custom signature rules")));

    // Case 4: Test exception in modsecRulesManager
    reset(responseObserver);
    reset(rulesValidator);
    reset(modsecRulesManager);
    when(rulesValidator.validate(any(GetCustomSignatureModsecRulesRequest.class)))
        .thenReturn(Status.OK);
    when(rulesManager.getCustomSignatureRules(any(), any())).thenReturn(allRules);
    when(modsecRulesManager.getModsecRules(any(), any(), any(), anyBoolean(), any(), eq(List.of())))
        .thenThrow(new RuntimeException("ModSec manager error"));
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, successRunnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(
                err ->
                    Status.fromThrowable(err).getCode() == Status.Code.INTERNAL
                        && Objects.equals(
                            Status.fromThrowable(err).getDescription(),
                            "Unable to fetch modsec custom signature rules")));
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

    when(rulesValidator.validate(any(GetCustomSignatureEdgeDecisionRulesRequest.class)))
        .thenReturn(Status.OK);
    when(rulesManager.getCustomSignatureRules(any(), any())).thenReturn(allRules);
    when(featureCachingClient.isEdgeDecisionEnabledForTenant(any())).thenReturn(true);
    when(customSignatureConfigServiceConfig.isEdsConversionEnabled()).thenReturn(true);

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

    // Case 3: Test with edge decision disabled
    reset(responseObserver);
    when(featureCachingClient.isEdgeDecisionEnabledForTenant(any())).thenReturn(false);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, successRunnable);
    verify(responseObserver, times(1))
        .onNext(
            GetCustomSignatureEdgeDecisionRulesResponse.newBuilder()
                .setEdgeDecisionEngineConfig(EdgeDecisionEngineConfig.getDefaultInstance())
                .build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 4: Test with EDS conversion disabled
    reset(responseObserver);
    when(featureCachingClient.isEdgeDecisionEnabledForTenant(any())).thenReturn(true);
    when(customSignatureConfigServiceConfig.isEdsConversionEnabled()).thenReturn(false);

    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, successRunnable);
    verify(responseObserver, times(1))
        .onNext(
            GetCustomSignatureEdgeDecisionRulesResponse.newBuilder()
                .setEdgeDecisionEngineConfig(EdgeDecisionEngineConfig.getDefaultInstance())
                .build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 5: Test validation failure
    reset(responseObserver);
    reset(rulesManager);
    reset(rulesValidator);
    reset(edgeDecisionConverter);
    when(rulesValidator.validate(any(GetCustomSignatureEdgeDecisionRulesRequest.class)))
        .thenThrow(new RuntimeException("Validation failed"));
    configService.getCustomSignatureEdgeDecisionRules(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(
                err ->
                    Status.fromThrowable(err).getCode() == Status.Code.INTERNAL
                        && Objects.equals(
                            Status.fromThrowable(err).getDescription(),
                            "Unable to fetch custom signature edge decision rules")));

    // Case 6: Test exception in edgeDecisionConverter
    reset(responseObserver);
    reset(rulesValidator);
    reset(edgeDecisionConverter);
    when(rulesValidator.validate(any(GetCustomSignatureEdgeDecisionRulesRequest.class)))
        .thenReturn(Status.OK);
    when(rulesManager.getCustomSignatureRules(any(), any())).thenReturn(allRules);
    when(featureCachingClient.isEdgeDecisionEnabledForTenant(any())).thenReturn(true);
    when(customSignatureConfigServiceConfig.isEdsConversionEnabled()).thenReturn(true);
    when(edgeDecisionConverter.convert(any()))
        .thenThrow(new RuntimeException("Edge decision converter error"));
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, successRunnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(
                err ->
                    Status.fromThrowable(err).getCode() == Status.Code.INTERNAL
                        && Objects.equals(
                            Status.fromThrowable(err).getDescription(),
                            "Unable to fetch custom signature edge decision rules")));
  }
}
