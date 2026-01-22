package ai.traceable.ratelimiting.config.service.v2;

import static org.mockito.ArgumentMatchers.any;
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
import ai.traceable.ratelimiting.service.v2.RateLimitingConfigServiceImpl;
import ai.traceable.ratelimiting.service.v2.rules.RulesManager;
import ai.traceable.ratelimiting.service.v2.rules.RulesValidator;
import ai.traceable.ratelimiting.service.v2.rules.converter.RateLimitingEdgeDecisionConverter;
import ai.traceable.ratelimiting.service.v2.rules.migration.RateLimitingMigrationManager;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RateLimitingConfigServiceImplTest {
  private static final String TENANT_ID = "default tenant";

  private RulesValidator rulesValidator;
  private RulesManager rulesManager;
  private RateLimitingConfigServiceImpl configService;
  private RateLimitingMigrationManager rateLimitingMigrationManager;

  @BeforeEach
  void setUp() {
    rulesValidator = mock(RulesValidator.class);
    rulesManager = mock(RulesManager.class);
    FeatureCachingClient featureCachingClient = mock(FeatureCachingClient.class);
    RateLimitingEdgeDecisionConverter translator = mock(RateLimitingEdgeDecisionConverter.class);
    rateLimitingMigrationManager = mock(RateLimitingMigrationManager.class);

    when(featureCachingClient.isEdgeDecisionEnabledForTenant(any())).thenReturn(true);

    configService =
        new RateLimitingConfigServiceImpl(
            rulesValidator,
            rulesManager,
            translator,
            featureCachingClient,
            rateLimitingMigrationManager);
  }

  @Test
  void testCreateRateLimitingRule() {
    StreamObserver<CreateRateLimitingRuleResponse> responseObserver = mock(StreamObserver.class);
    when(rateLimitingMigrationManager.migrateCreateRateLimitingRuleRequest(any()))
        .thenAnswer(invocation -> invocation.getArgument(0));
    Runnable runnable =
        () ->
            configService.createRateLimitingRule(
                CreateRateLimitingRuleRequest.getDefaultInstance(), responseObserver);

    doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
        .when(rulesValidator)
        .validateOrThrow(any(), (CreateRateLimitingRuleRequest) any(), any());
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    RateLimitingRule rule = buildRateLimitingRule();
    reset(responseObserver);
    doNothing()
        .when(rulesValidator)
        .validateOrThrow(any(), (CreateRateLimitingRuleRequest) any(), any());
    when(rulesManager.createRateLimitingRule(any(), any())).thenReturn(rule);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onNext(CreateRateLimitingRuleResponse.newBuilder().setRule(rule).build());
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testGetRateLimitingRules() {
    RateLimitingRule platformRule =
        buildRateLimitingRuleWithRuleEvaluationPoints(
            "platform-rule-id",
            "platform rule name",
            Category.CATEGORY_RATE_LIMITING,
            List.of(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM));
    RateLimitingRule agentRule =
        buildRateLimitingRuleWithRuleEvaluationPoints(
            "agent-rule-id",
            "agent rule name",
            Category.CATEGORY_RATE_LIMITING,
            List.of(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT));
    RateLimitingRule edgeRule =
        buildRateLimitingRuleWithRuleEvaluationPoints(
            "edge-rule-id",
            "edge rule name",
            Category.CATEGORY_DATA_EXFILTRATION,
            List.of(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE));
    RateLimitingRule multipleEvaluationPointsRule =
        buildRateLimitingRuleWithRuleEvaluationPoints(
            "multi-evaluation-points-rule-id",
            "multi evaluation points rule name",
            Category.CATEGORY_RATE_LIMITING,
            List.of(
                RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM,
                RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE));
    List<RateLimitingRule> allRules =
        List.of(platformRule, agentRule, edgeRule, multipleEvaluationPointsRule);
    List<RateLimitingRule> platformRules = List.of(platformRule, multipleEvaluationPointsRule);
    List<RateLimitingRule> agentRules = List.of(agentRule);
    List<RateLimitingRule> edgeRules = List.of(edgeRule, multipleEvaluationPointsRule);
    StreamObserver<GetRateLimitingRulesResponse> responseObserver = mock(StreamObserver.class);
    doNothing()
        .when(rateLimitingMigrationManager)
        .migrateForRuleEvaluationPointsIfApplicable(any());

    // Case 1: Test with no filter
    Runnable noFilterRunnable =
        () ->
            configService.getRateLimitingRules(
                GetRateLimitingRulesRequest.getDefaultInstance(), responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, noFilterRunnable);
    verify(responseObserver, times(1)).onNext(GetRateLimitingRulesResponse.getDefaultInstance());
    verify(responseObserver, times(1)).onCompleted();

    // Case 2: Test with all rules returned
    reset(responseObserver);
    when(rulesManager.getRateLimitingRuleRecords(any(), any())).thenReturn(toRecords(allRules));

    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, noFilterRunnable);
    verify(responseObserver, times(1))
        .onNext(
            GetRateLimitingRulesResponse.newBuilder()
                .addAllRules(allRules)
                .addAllRuleRecords(toRecords(allRules))
                .build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 3: Test with PLATFORM evaluation point filter
    reset(responseObserver);
    GetRateLimitingRulesFilter platformFilter =
        GetRateLimitingRulesFilter.newBuilder()
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
            .build();
    when(rulesManager.getRateLimitingRuleRecords(any(), eq(platformFilter)))
        .thenReturn(toRecords(platformRules));
    Runnable platformFilterRunnable =
        () ->
            configService.getRateLimitingRules(
                GetRateLimitingRulesRequest.newBuilder().setRulesFilter(platformFilter).build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, platformFilterRunnable);
    verify(responseObserver, times(1))
        .onNext(
            GetRateLimitingRulesResponse.newBuilder()
                .addAllRules(platformRules)
                .addAllRuleRecords(toRecords(platformRules))
                .build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 4: Test with AGENT evaluation point filter
    reset(responseObserver);
    GetRateLimitingRulesFilter agentFilter =
        GetRateLimitingRulesFilter.newBuilder()
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT)
            .build();
    when(rulesManager.getRateLimitingRuleRecords(any(), eq(agentFilter)))
        .thenReturn(toRecords(agentRules));
    Runnable agentFilterRunnable =
        () ->
            configService.getRateLimitingRules(
                GetRateLimitingRulesRequest.newBuilder().setRulesFilter(agentFilter).build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, agentFilterRunnable);
    verify(responseObserver, times(1))
        .onNext(
            GetRateLimitingRulesResponse.newBuilder()
                .addAllRules(agentRules)
                .addAllRuleRecords(toRecords(agentRules))
                .build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 5: Test with EDGE evaluation point filter
    reset(responseObserver);
    GetRateLimitingRulesFilter edgeFilter =
        GetRateLimitingRulesFilter.newBuilder()
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();
    when(rulesManager.getRateLimitingRuleRecords(any(), eq(edgeFilter)))
        .thenReturn(toRecords(edgeRules));
    Runnable edgeFilterRunnable =
        () ->
            configService.getRateLimitingRules(
                GetRateLimitingRulesRequest.newBuilder().setRulesFilter(edgeFilter).build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, edgeFilterRunnable);
    verify(responseObserver, times(1))
        .onNext(
            GetRateLimitingRulesResponse.newBuilder()
                .addAllRules(edgeRules)
                .addAllRuleRecords(toRecords(edgeRules))
                .build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 6: Test with multiple evaluation points filter (PLATFORM and EDGE)
    reset(responseObserver);
    GetRateLimitingRulesFilter multiPointFilter =
        GetRateLimitingRulesFilter.newBuilder()
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();
    List<RateLimitingRule> platformEdgeRules = List.of(platformRule, edgeRule);
    when(rulesManager.getRateLimitingRuleRecords(any(), eq(multiPointFilter)))
        .thenReturn(toRecords(platformEdgeRules));
    Runnable multiPointFilterRunnable =
        () ->
            configService.getRateLimitingRules(
                GetRateLimitingRulesRequest.newBuilder().setRulesFilter(multiPointFilter).build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, multiPointFilterRunnable);
    verify(responseObserver, times(1))
        .onNext(
            GetRateLimitingRulesResponse.newBuilder()
                .addAllRules(platformEdgeRules)
                .addAllRuleRecords(toRecords(platformEdgeRules))
                .build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 7: Test with combined filters (category + evaluation point)
    reset(responseObserver);
    GetRateLimitingRulesFilter combinedFilter =
        GetRateLimitingRulesFilter.newBuilder()
            .addCategories(Category.CATEGORY_RATE_LIMITING)
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
            .build();
    List<RateLimitingRule> filteredRules = List.of(platformRule, multipleEvaluationPointsRule);
    when(rulesManager.getRateLimitingRuleRecords(any(), eq(combinedFilter)))
        .thenReturn(toRecords(filteredRules));
    Runnable combinedFilterRunnable =
        () ->
            configService.getRateLimitingRules(
                GetRateLimitingRulesRequest.newBuilder().setRulesFilter(combinedFilter).build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, combinedFilterRunnable);
    verify(responseObserver, times(1))
        .onNext(
            GetRateLimitingRulesResponse.newBuilder()
                .addAllRules(filteredRules)
                .addAllRuleRecords(toRecords(filteredRules))
                .build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 8: Test validation failure
    reset(responseObserver);
    reset(rulesManager);
    reset(rulesValidator);
    doThrow(new RuntimeException("Validation failed"))
        .when(rulesValidator)
        .validateOrThrow(any(), any(GetRateLimitingRulesRequest.class));
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, noFilterRunnable);
    verify(responseObserver, times(1)).onError(any(RuntimeException.class));
  }

  @Test
  void testGetRateLimitingRulesWithEdgeDecisionFilter() {
    RateLimitingRule platformRule =
        RateLimitingRule.newBuilder()
            .setId("platform-rule-id")
            .setData(
                RateLimitingRuleData.newBuilder()
                    .setName("Platform Rule")
                    .setCategory(Category.CATEGORY_RATE_LIMITING)
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                    .addThresholdActionConfigs(
                        ThresholdActionConfig.newBuilder()
                            .addActions(
                                Action.newBuilder()
                                    .setBlock(Action.Block.newBuilder().setDurationIso("1h")))
                            .addResourceAccessThresholdConfigs(
                                ResourceAccessThresholdConfig.newBuilder()
                                    .setRollingWindowThresholdConfig(
                                        ResourceAccessThresholdConfig.RollingWindowThresholdConfig
                                            .newBuilder()
                                            .setCountAllowed(5)
                                            .setDurationIso("10m")))))
            .build();
    RateLimitingRule agentRule =
        RateLimitingRule.newBuilder()
            .setId("agent-rule-id")
            .setData(
                RateLimitingRuleData.newBuilder()
                    .setName("Agent Rule")
                    .setCategory(Category.CATEGORY_RATE_LIMITING)
                    .addRuleEvaluationPoints(
                        RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT)
                    .addThresholdActionConfigs(
                        ThresholdActionConfig.newBuilder()
                            .addActions(
                                Action.newBuilder()
                                    .setBlock(Action.Block.newBuilder().setDurationIso("1h")))
                            .addResourceAccessThresholdConfigs(
                                ResourceAccessThresholdConfig.newBuilder()
                                    .setRollingWindowThresholdConfig(
                                        ResourceAccessThresholdConfig.RollingWindowThresholdConfig
                                            .newBuilder()
                                            .setCountAllowed(5)
                                            .setDurationIso("10m")))))
            .build();
    RateLimitingRule edgeRule =
        RateLimitingRule.newBuilder()
            .setId("edge-rule-id")
            .setData(
                RateLimitingRuleData.newBuilder()
                    .setName("Edge Rule")
                    .setCategory(Category.CATEGORY_DATA_EXFILTRATION)
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                    .addThresholdActionConfigs(
                        ThresholdActionConfig.newBuilder()
                            .addActions(
                                Action.newBuilder()
                                    .setBlock(Action.Block.newBuilder().setDurationIso("1h")))
                            .addResourceAccessThresholdConfigs(
                                ResourceAccessThresholdConfig.newBuilder()
                                    .setRollingWindowThresholdConfig(
                                        ResourceAccessThresholdConfig.RollingWindowThresholdConfig
                                            .newBuilder()
                                            .setCountAllowed(5)
                                            .setDurationIso("10m")))))
            .build();
    RateLimitingRule multiEvalPointRule =
        RateLimitingRule.newBuilder()
            .setId("multi-eval-rule-id")
            .setData(
                RateLimitingRuleData.newBuilder()
                    .setName("Multi Evaluation Point Rule")
                    .setCategory(Category.CATEGORY_RATE_LIMITING)
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                    .addThresholdActionConfigs(
                        ThresholdActionConfig.newBuilder()
                            .addActions(
                                Action.newBuilder()
                                    .setBlock(Action.Block.newBuilder().setDurationIso("1h")))
                            .addResourceAccessThresholdConfigs(
                                ResourceAccessThresholdConfig.newBuilder()
                                    .setRollingWindowThresholdConfig(
                                        ResourceAccessThresholdConfig.RollingWindowThresholdConfig
                                            .newBuilder()
                                            .setCountAllowed(5)
                                            .setDurationIso("10m")))))
            .build();
    RateLimitingRule noEvalPointRule =
        RateLimitingRule.newBuilder()
            .setId("no-eval-point-rule-id")
            .setData(
                RateLimitingRuleData.newBuilder()
                    .setName("No Evaluation Point Rule")
                    .setCategory(Category.CATEGORY_RATE_LIMITING)
                    .addThresholdActionConfigs(
                        ThresholdActionConfig.newBuilder()
                            .addActions(
                                Action.newBuilder()
                                    .setBlock(Action.Block.newBuilder().setDurationIso("1h")))
                            .addResourceAccessThresholdConfigs(
                                ResourceAccessThresholdConfig.newBuilder()
                                    .setRollingWindowThresholdConfig(
                                        ResourceAccessThresholdConfig.RollingWindowThresholdConfig
                                            .newBuilder()
                                            .setCountAllowed(5)
                                            .setDurationIso("10m")))))
            .build();
    List<RateLimitingRule> allRules =
        List.of(platformRule, agentRule, edgeRule, multiEvalPointRule, noEvalPointRule);
    StreamObserver<GetRateLimitingRulesResponse> responseObserver = mock(StreamObserver.class);
    doNothing()
        .when(rateLimitingMigrationManager)
        .migrateForRuleEvaluationPointsIfApplicable(any());

    // Case 1: Test with EDGE evaluation point filter
    GetRateLimitingRulesFilter edgeFilter =
        GetRateLimitingRulesFilter.newBuilder()
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();
    List<RateLimitingRule> edgeRules = List.of(edgeRule, multiEvalPointRule);
    when(rulesManager.getRateLimitingRuleRecords(any(), eq(edgeFilter)))
        .thenReturn(toRecords(edgeRules));
    Runnable edgeFilterRunnable =
        () ->
            configService.getRateLimitingRules(
                GetRateLimitingRulesRequest.newBuilder().setRulesFilter(edgeFilter).build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, edgeFilterRunnable);
    verify(responseObserver, times(1))
        .onNext(
            GetRateLimitingRulesResponse.newBuilder()
                .addAllRules(edgeRules)
                .addAllRuleRecords(toRecords(edgeRules))
                .build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 2: Test with empty filter (should return all rules)
    reset(responseObserver);
    GetRateLimitingRulesFilter emptyFilter = GetRateLimitingRulesFilter.newBuilder().build();
    when(rulesManager.getRateLimitingRuleRecords(any(), eq(emptyFilter)))
        .thenReturn(toRecords(allRules));
    Runnable emptyFilterRunnable =
        () ->
            configService.getRateLimitingRules(
                GetRateLimitingRulesRequest.newBuilder().setRulesFilter(emptyFilter).build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, emptyFilterRunnable);
    verify(responseObserver, times(1))
        .onNext(
            GetRateLimitingRulesResponse.newBuilder()
                .addAllRules(allRules)
                .addAllRuleRecords(toRecords(allRules))
                .build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 3: Test with PLATFORM evaluation point filter
    reset(responseObserver);
    GetRateLimitingRulesFilter platformFilter =
        GetRateLimitingRulesFilter.newBuilder()
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
            .build();
    List<RateLimitingRule> platformRules = List.of(platformRule, multiEvalPointRule);
    when(rulesManager.getRateLimitingRuleRecords(any(), eq(platformFilter)))
        .thenReturn(toRecords(platformRules));
    Runnable platformFilterRunnable =
        () ->
            configService.getRateLimitingRules(
                GetRateLimitingRulesRequest.newBuilder().setRulesFilter(platformFilter).build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, platformFilterRunnable);
    verify(responseObserver, times(1))
        .onNext(
            GetRateLimitingRulesResponse.newBuilder()
                .addAllRules(platformRules)
                .addAllRuleRecords(toRecords(platformRules))
                .build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 4: Test with AGENT evaluation point filter
    reset(responseObserver);
    GetRateLimitingRulesFilter agentFilter =
        GetRateLimitingRulesFilter.newBuilder()
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT)
            .build();
    List<RateLimitingRule> agentRules = List.of(agentRule);
    when(rulesManager.getRateLimitingRuleRecords(any(), eq(agentFilter)))
        .thenReturn(toRecords(agentRules));
    Runnable agentFilterRunnable =
        () ->
            configService.getRateLimitingRules(
                GetRateLimitingRulesRequest.newBuilder().setRulesFilter(agentFilter).build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, agentFilterRunnable);
    verify(responseObserver, times(1))
        .onNext(
            GetRateLimitingRulesResponse.newBuilder()
                .addAllRules(agentRules)
                .addAllRuleRecords(toRecords(agentRules))
                .build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 5: Test with multiple evaluation points filter (PLATFORM, EDGE)
    reset(responseObserver);
    GetRateLimitingRulesFilter multiPointFilter =
        GetRateLimitingRulesFilter.newBuilder()
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();
    List<RateLimitingRule> multiPointRules = List.of(platformRule, edgeRule, multiEvalPointRule);
    when(rulesManager.getRateLimitingRuleRecords(any(), eq(multiPointFilter)))
        .thenReturn(toRecords(multiPointRules));
    Runnable multiPointRunnable =
        () ->
            configService.getRateLimitingRules(
                GetRateLimitingRulesRequest.newBuilder().setRulesFilter(multiPointFilter).build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, multiPointRunnable);
    verify(responseObserver, times(1))
        .onNext(
            GetRateLimitingRulesResponse.newBuilder()
                .addAllRules(multiPointRules)
                .addAllRuleRecords(toRecords(multiPointRules))
                .build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 6: Test with combined filter (evaluation point + category)
    reset(responseObserver);
    GetRateLimitingRulesFilter combinedFilter =
        GetRateLimitingRulesFilter.newBuilder()
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .addCategories(Category.CATEGORY_RATE_LIMITING)
            .build();
    List<RateLimitingRule> combinedFilterRules = List.of(multiEvalPointRule);
    when(rulesManager.getRateLimitingRuleRecords(any(), eq(combinedFilter)))
        .thenReturn(toRecords(combinedFilterRules));
    Runnable combinedFilterRunnable =
        () ->
            configService.getRateLimitingRules(
                GetRateLimitingRulesRequest.newBuilder().setRulesFilter(combinedFilter).build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, combinedFilterRunnable);
    verify(responseObserver, times(1))
        .onNext(
            GetRateLimitingRulesResponse.newBuilder()
                .addAllRules(combinedFilterRules)
                .addAllRuleRecords(toRecords(combinedFilterRules))
                .build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 7: Test with edge decision filter set to True
    reset(responseObserver);
    GetRateLimitingRulesFilter edgeDecisionFilter =
        GetRateLimitingRulesFilter.newBuilder()
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setFilterEdgeDecisionRules(true)
            .build();
    List<RateLimitingRule> edgeDecisionRules = List.of(edgeRule, multiEvalPointRule);
    when(rulesManager.getRateLimitingRuleRecords(any(), eq(edgeDecisionFilter)))
        .thenReturn(toRecords(edgeDecisionRules));
    doNothing()
        .when(rateLimitingMigrationManager)
        .migrateForRuleEvaluationPointsIfApplicable(any());
    Runnable edgeDecisionFilterRunnable =
        () ->
            configService.getRateLimitingRules(
                GetRateLimitingRulesRequest.newBuilder().setRulesFilter(edgeDecisionFilter).build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, edgeDecisionFilterRunnable);
    verify(responseObserver, times(1))
        .onNext(
            GetRateLimitingRulesResponse.newBuilder()
                .addAllRules(edgeDecisionRules)
                .addAllRuleRecords(toRecords(edgeDecisionRules))
                .build());
    verify(responseObserver, times(1)).onCompleted();

    // Case 8: Test error handling
    reset(responseObserver);
    reset(rulesManager);
    reset(rulesValidator);
    doThrow(new RuntimeException("Test exception"))
        .when(rulesManager)
        .getRateLimitingRuleRecords(any(), any());
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, edgeFilterRunnable);
    verify(responseObserver, times(1)).onError(any(RuntimeException.class));
  }

  @Test
  void testGetRateLimitingRuleModsecRules() {
    RateLimitingRule platformRule =
        buildRateLimitingRuleWithRuleEvaluationPoints(
            "platform-rule-id",
            "Platform Rule",
            Category.CATEGORY_RATE_LIMITING,
            List.of(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM));
    ModsecRuleIdInfo modsecRuleIdInfo =
        ModsecRuleIdInfo.newBuilder()
            .setType(ModsecRuleIdInfo.IdType.ID_TYPE_DATA_TYPE_CUSTOM_LOCATION)
            .addMatchConditions(
                ModsecRuleIdInfo.MatchCondition.newBuilder().setMatchId("modsec-rule-1").build())
            .build();
    ModsecBlobData modsecBlobData =
        ModsecBlobData.newBuilder()
            .setModsecBlob("SecRule REQUEST_URI \"@rx /api/.*\" \"id:1000,phase:1,deny\"")
            .addServiceNames("service1")
            .addRuleIds(platformRule.getId())
            .build();
    GetRateLimitingRuleModsecRulesResponse platformResponse =
        GetRateLimitingRuleModsecRulesResponse.newBuilder()
            .setModsecDirectivesBlob("SecRuleEngine On")
            .addModsecBlobsData(modsecBlobData)
            .addRules(
                RateLimitingModsecRule.newBuilder()
                    .setId(platformRule.getId())
                    .setData(platformRule.getData())
                    .addAssociatedModsecRuleIds(modsecRuleIdInfo)
                    .build())
            .build();
    StreamObserver<GetRateLimitingRuleModsecRulesResponse> responseObserver =
        mock(StreamObserver.class);

    // Case 1: Test with no filter (default behavior)
    Runnable noFilterRunnable =
        () ->
            configService.getRateLimitingRuleModsecRules(
                GetRateLimitingRuleModsecRulesRequest.getDefaultInstance(), responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, noFilterRunnable);
    verify(responseObserver, times(1)).onNext(null);
    verify(responseObserver, times(1)).onCompleted();

    // Case 2: Test with platform evaluation point filter
    reset(responseObserver);
    GetRateLimitingRulesFilter platformFilter =
        GetRateLimitingRulesFilter.newBuilder()
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
            .build();
    GetRateLimitingModsecRulesFilter platformModsecFilter =
        GetRateLimitingModsecRulesFilter.newBuilder().setRulesFilter(platformFilter).build();
    when(rulesManager.getRateLimitingModsecRules(any(), eq(platformModsecFilter)))
        .thenReturn(platformResponse);
    Runnable platformFilterRunnable =
        () ->
            configService.getRateLimitingRuleModsecRules(
                GetRateLimitingRuleModsecRulesRequest.newBuilder()
                    .setRulesFilter(platformModsecFilter)
                    .build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, platformFilterRunnable);
    verify(responseObserver, times(1)).onNext(platformResponse);
    verify(responseObserver, times(1)).onCompleted();

    // Case 3: Test with service names filter
    reset(responseObserver);
    GetRateLimitingModsecRulesFilter serviceFilter =
        GetRateLimitingModsecRulesFilter.newBuilder().addServiceNames("service1").build();
    when(rulesManager.getRateLimitingModsecRules(any(), eq(serviceFilter)))
        .thenReturn(platformResponse);
    Runnable serviceFilterRunnable =
        () ->
            configService.getRateLimitingRuleModsecRules(
                GetRateLimitingRuleModsecRulesRequest.newBuilder()
                    .setRulesFilter(serviceFilter)
                    .build(),
                responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, serviceFilterRunnable);
    verify(responseObserver, times(1)).onNext(platformResponse);
    verify(responseObserver, times(1)).onCompleted();

    // Case 4: Test error handling
    reset(responseObserver);
    reset(rulesValidator);
    doThrow(new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Invalid filter")))
        .when(rulesValidator)
        .validateOrThrow(any(), any(GetRateLimitingRuleModsecRulesRequest.class));
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, noFilterRunnable);
    verify(responseObserver, times(1)).onError(any(StatusRuntimeException.class));
  }

  @Test
  void testUpdateRateLimitingRule() {
    when(rateLimitingMigrationManager.migrateUpdateRateLimitingRuleRequest(any()))
        .thenAnswer(invocation -> invocation.getArgument(0));
    StreamObserver<UpdateRateLimitingRuleResponse> responseObserver = mock(StreamObserver.class);
    Runnable runnable =
        () ->
            configService.updateRateLimitingRule(
                UpdateRateLimitingRuleRequest.getDefaultInstance(), responseObserver);

    doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
        .when(rulesValidator)
        .validateOrThrow(any(), (UpdateRateLimitingRuleRequest) any(), any());
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    RateLimitingRule rule = buildRateLimitingRule();
    reset(responseObserver);
    doNothing()
        .when(rulesValidator)
        .validateOrThrow(any(), (UpdateRateLimitingRuleRequest) any(), any());
    when(rulesManager.updateRateLimitingRule(any(), any(), any())).thenReturn(rule);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onNext(UpdateRateLimitingRuleResponse.newBuilder().setRule(rule).build());
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testDeleteRateLimitingRule() {
    StreamObserver<DeleteRateLimitingRuleResponse> responseObserver = mock(StreamObserver.class);
    Runnable runnable =
        () ->
            configService.deleteRateLimitingRule(
                DeleteRateLimitingRuleRequest.getDefaultInstance(), responseObserver);
    doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
        .when(rulesValidator)
        .validateOrThrow(any(), (DeleteRateLimitingRuleRequest) any());
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    RateLimitingRule rule = buildRateLimitingRule();
    reset(responseObserver);
    doNothing().when(rulesValidator).validateOrThrow(any(), (DeleteRateLimitingRuleRequest) any());
    when(rulesManager.deleteRateLimitingRule(any(), any())).thenReturn(Optional.of(rule));
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(rulesManager, times(1)).deleteRateLimitingRule(any(), any());
    verify(responseObserver, times(1)).onNext(DeleteRateLimitingRuleResponse.getDefaultInstance());
    verify(responseObserver, times(1)).onCompleted();
  }

  private RateLimitingRule buildRateLimitingRule() {
    return RateLimitingRule.newBuilder().setId("id").setData(buildRateLimitingRuleData()).build();
  }

  private RateLimitingRuleData buildRateLimitingRuleData() {
    return RateLimitingRuleData.newBuilder()
        .setName("rule")
        .setCategory(Category.CATEGORY_RATE_LIMITING)
        .build();
  }

  private RateLimitingRule buildRateLimitingRuleWithRuleEvaluationPoints(
      String id, String name, Category category, List<RuleEvaluationPoint> ruleEvaluationPoints) {
    return RateLimitingRule.newBuilder()
        .setId(id)
        .setData(
            RateLimitingRuleData.newBuilder()
                .setCategory(category)
                .setName(name)
                .addAllRuleEvaluationPoints(ruleEvaluationPoints))
        .build();
  }

  private List<RateLimitingRuleRecord> toRecords(List<RateLimitingRule> rules) {
    return rules.stream()
        .map(rule -> RateLimitingRuleRecord.newBuilder().setRule(rule).build())
        .collect(Collectors.toList());
  }

  @Test
  void testBulkDeleteRateLimitingRules() {
    StreamObserver<BulkDeleteRateLimitingRulesResponse> responseObserver =
        mock(StreamObserver.class);
    BulkDeleteRateLimitingRulesRequest bulkDeleteRequest =
        BulkDeleteRateLimitingRulesRequest.newBuilder().addIds("id1").addIds("id2").build();

    Runnable runnable =
        () -> configService.bulkDeleteRateLimitingRules(bulkDeleteRequest, responseObserver);

    doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
        .when(rulesValidator)
        .validateOrThrow(any(), (BulkDeleteRateLimitingRulesRequest) any());
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    reset(responseObserver);
    doNothing()
        .when(rulesValidator)
        .validateOrThrow(any(), (BulkDeleteRateLimitingRulesRequest) any());
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(rulesManager, times(1)).bulkDeleteRateLimitingRules(any(), eq(List.of("id1", "id2")));
    verify(responseObserver, times(1))
        .onNext(BulkDeleteRateLimitingRulesResponse.getDefaultInstance());
    verify(responseObserver, times(1)).onCompleted();
  }
}
