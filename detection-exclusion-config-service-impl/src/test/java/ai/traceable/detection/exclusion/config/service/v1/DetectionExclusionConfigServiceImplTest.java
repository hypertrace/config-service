package ai.traceable.detection.exclusion.config.service.v1;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.detection.exclusion.config.service.v1.rules.RulesManager;
import ai.traceable.detection.exclusion.config.service.v1.rules.RulesValidator;
import ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision.DetectionExclusionRuleEdgeDecisionConverter;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DetectionExclusionConfigServiceImplTest {

  private RulesManager rulesManager;
  private RulesValidator rulesValidator;
  private FeatureCachingClient featureCachingClient;
  private DetectionExclusionRuleEdgeDecisionConverter edgeDecisionConverter;
  private DetectionExclusionConfigServiceImpl detectionExclusionConfigService;
  private final RequestContext requestContext = RequestContext.forTenantId("tenantId");

  @BeforeEach
  void setUp() {
    rulesManager = mock(RulesManager.class);
    rulesValidator = mock(RulesValidator.class);
    featureCachingClient = mock(FeatureCachingClient.class);
    edgeDecisionConverter = mock(DetectionExclusionRuleEdgeDecisionConverter.class);
    detectionExclusionConfigService =
        new DetectionExclusionConfigServiceImpl(
            rulesManager, rulesValidator, featureCachingClient, edgeDecisionConverter);
  }

  @Test
  void testGetDetectionExclusionRules() {
    DetectionExclusionRule platformRule =
        getRule("platform-rule-id", List.of(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM));
    DetectionExclusionRule inlineRule =
        getRule(
            "inline-rule-id",
            List.of(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT));
    DetectionExclusionRule edgeRule =
        getRule("edge-rule-id", List.of(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE));
    DetectionExclusionRule multipleRuleEvaluationPointsRule =
        getRule(
            "multiple-rule-evaluation-points-rule-id",
            List.of(
                RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM,
                RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE));
    List<DetectionExclusionRule> allRules =
        List.of(platformRule, inlineRule, edgeRule, multipleRuleEvaluationPointsRule);
    when(rulesManager.getDetectionExclusionRules(eq(requestContext), any()))
        .thenAnswer(
            invocation -> {
              GetRulesFilter filter = invocation.getArgument(1);
              if (filter.getRuleEvaluationPointsList().isEmpty()) {
                return allRules;
              }
              return allRules.stream()
                  .filter(
                      rule ->
                          rule.getRuleInfo().getRuleEvaluationPointsList().stream()
                              .anyMatch(filter.getRuleEvaluationPointsList()::contains))
                  .collect(Collectors.toList());
            });
    doNothing()
        .when(rulesValidator)
        .validateOrThrow(eq(requestContext), any(GetDetectionExclusionRulesRequest.class));

    // Test 1: Filter for PLATFORM rules
    StreamObserver<GetDetectionExclusionRulesResponse> platformObserver =
        mock(StreamObserver.class);
    GetDetectionExclusionRulesRequest platformRequest =
        GetDetectionExclusionRulesRequest.newBuilder()
            .setFilter(
                GetRulesFilter.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                    .build())
            .build();
    requestContext.run(
        () ->
            detectionExclusionConfigService.getDetectionExclusionRules(
                platformRequest, platformObserver));
    verify(platformObserver, times(1))
        .onNext(
            GetDetectionExclusionRulesResponse.newBuilder()
                .addRules(platformRule)
                .addRules(multipleRuleEvaluationPointsRule)
                .build());
    verify(platformObserver, times(1)).onCompleted();

    // Test 2: Filter for INLINE_TRACING_AGENT rules
    StreamObserver<GetDetectionExclusionRulesResponse> inlineObserver = mock(StreamObserver.class);
    GetDetectionExclusionRulesRequest inlineRequest =
        GetDetectionExclusionRulesRequest.newBuilder()
            .setFilter(
                GetRulesFilter.newBuilder()
                    .addRuleEvaluationPoints(
                        RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT)
                    .build())
            .build();
    requestContext.run(
        () ->
            detectionExclusionConfigService.getDetectionExclusionRules(
                inlineRequest, inlineObserver));
    verify(inlineObserver, times(1))
        .onNext(GetDetectionExclusionRulesResponse.newBuilder().addRules(inlineRule).build());
    verify(inlineObserver, times(1)).onCompleted();

    // Test 3: Filter for EDGE rules
    StreamObserver<GetDetectionExclusionRulesResponse> edgeObserver = mock(StreamObserver.class);
    GetDetectionExclusionRulesRequest edgeRequest =
        GetDetectionExclusionRulesRequest.newBuilder()
            .setFilter(
                GetRulesFilter.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                    .build())
            .build();
    requestContext.run(
        () ->
            detectionExclusionConfigService.getDetectionExclusionRules(edgeRequest, edgeObserver));
    verify(edgeObserver, times(1))
        .onNext(
            GetDetectionExclusionRulesResponse.newBuilder()
                .addRules(edgeRule)
                .addRules(multipleRuleEvaluationPointsRule)
                .build());
    verify(edgeObserver, times(1)).onCompleted();

    // Test 4: Filter for multiple evaluation points
    StreamObserver<GetDetectionExclusionRulesResponse> multiObserver = mock(StreamObserver.class);
    GetDetectionExclusionRulesRequest multiRequest =
        GetDetectionExclusionRulesRequest.newBuilder()
            .setFilter(
                GetRulesFilter.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                    .build())
            .build();
    requestContext.run(
        () ->
            detectionExclusionConfigService.getDetectionExclusionRules(
                multiRequest, multiObserver));
    verify(multiObserver, times(1))
        .onNext(
            GetDetectionExclusionRulesResponse.newBuilder()
                .addRules(platformRule)
                .addRules(edgeRule)
                .addRules(multipleRuleEvaluationPointsRule)
                .build());
    verify(multiObserver, times(1)).onCompleted();

    // Test 5: No filter (should return all rules)
    StreamObserver<GetDetectionExclusionRulesResponse> allObserver = mock(StreamObserver.class);
    GetDetectionExclusionRulesRequest allRequest =
        GetDetectionExclusionRulesRequest.newBuilder()
            .setFilter(GetRulesFilter.getDefaultInstance())
            .build();
    requestContext.run(
        () -> detectionExclusionConfigService.getDetectionExclusionRules(allRequest, allObserver));
    verify(allObserver, times(1))
        .onNext(
            GetDetectionExclusionRulesResponse.newBuilder()
                .addRules(platformRule)
                .addRules(inlineRule)
                .addRules(edgeRule)
                .addRules(multipleRuleEvaluationPointsRule)
                .build());
    verify(allObserver, times(1)).onCompleted();
  }

  @Test
  void testCreateDetectionExclusionRule() {
    DetectionExclusionRule detectionExclusionRule = DetectionExclusionRule.getDefaultInstance();
    CreateDetectionExclusionRuleRequest request =
        CreateDetectionExclusionRuleRequest.getDefaultInstance();
    StreamObserver<CreateDetectionExclusionRuleResponse> streamObserver =
        mock(StreamObserver.class);

    when(rulesManager.createDetectionExclusionRule(eq(requestContext), any(), any()))
        .thenReturn(detectionExclusionRule);

    // validation throws error
    Exception exception = Status.INVALID_ARGUMENT.asRuntimeException();
    doThrow(exception).when(rulesValidator).validateOrThrow(eq(requestContext), eq(request), any());

    requestContext.run(
        () ->
            detectionExclusionConfigService.createDetectionExclusionRule(request, streamObserver));
    verify(streamObserver, times(1)).onError(exception);

    // validation succeeds
    doNothing().when(rulesValidator).validateOrThrow(eq(requestContext), eq(request), any());

    requestContext.run(
        () ->
            detectionExclusionConfigService.createDetectionExclusionRule(request, streamObserver));
    verify(streamObserver, times(1))
        .onNext(
            CreateDetectionExclusionRuleResponse.newBuilder()
                .setRule(detectionExclusionRule)
                .build());
    verify(streamObserver, times(1)).onCompleted();
  }

  @Test
  void testUpdateDetectionExclusionRule() {
    DetectionExclusionRule detectionExclusionRule = DetectionExclusionRule.getDefaultInstance();
    UpdateDetectionExclusionRuleRequest request =
        UpdateDetectionExclusionRuleRequest.getDefaultInstance();
    StreamObserver<UpdateDetectionExclusionRuleResponse> streamObserver =
        mock(StreamObserver.class);

    when(rulesManager.updateDetectionExclusionRule(eq(requestContext), any()))
        .thenReturn(detectionExclusionRule);

    // validation throws error
    Exception exception = Status.INVALID_ARGUMENT.asRuntimeException();
    doThrow(exception).when(rulesValidator).validateOrThrow(eq(requestContext), eq(request), any());

    requestContext.run(
        () ->
            detectionExclusionConfigService.updateDetectionExclusionRule(request, streamObserver));
    verify(streamObserver, times(1)).onError(exception);

    // validation succeeds
    doNothing().when(rulesValidator).validateOrThrow(eq(requestContext), eq(request), any());

    requestContext.run(
        () ->
            detectionExclusionConfigService.updateDetectionExclusionRule(request, streamObserver));
    verify(streamObserver, times(1))
        .onNext(
            UpdateDetectionExclusionRuleResponse.newBuilder()
                .setRule(detectionExclusionRule)
                .build());
    verify(streamObserver, times(1)).onCompleted();
  }

  @Test
  void testDeleteDetectionExclusionRule() {
    DeleteDetectionExclusionRuleRequest request =
        DeleteDetectionExclusionRuleRequest.getDefaultInstance();
    StreamObserver<DeleteDetectionExclusionRuleResponse> streamObserver =
        mock(StreamObserver.class);

    // validation throws error
    Exception exception = Status.INVALID_ARGUMENT.asRuntimeException();
    doThrow(exception).when(rulesValidator).validateOrThrow(requestContext, request);

    requestContext.run(
        () ->
            detectionExclusionConfigService.deleteDetectionExclusionRule(request, streamObserver));
    verify(streamObserver, times(1)).onError(exception);

    // validation succeeds
    doNothing().when(rulesValidator).validateOrThrow(requestContext, request);

    requestContext.run(
        () ->
            detectionExclusionConfigService.deleteDetectionExclusionRule(request, streamObserver));
    verify(rulesManager, times(1)).deleteDetectionExclusionRule(eq(requestContext), any());
    verify(streamObserver, times(1))
        .onNext(DeleteDetectionExclusionRuleResponse.getDefaultInstance());
    verify(streamObserver, times(1)).onCompleted();
  }

  @Test
  void testGetDetectionExclusionModsecRules() {
    GetExclusionModsecRulesResponse platformResponse =
        createModsecRulesResponse(
            "platform-rules", List.of(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM));
    GetExclusionModsecRulesResponse inlineResponse =
        createModsecRulesResponse(
            "inline-rules",
            List.of(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT));
    GetExclusionModsecRulesResponse edgeResponse =
        createModsecRulesResponse(
            "edge-rules", List.of(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE));
    GetExclusionModsecRulesResponse multiPointResponse =
        createModsecRulesResponse(
            "multi-point-rules",
            List.of(
                RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM,
                RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE));
    when(rulesManager.getDetectionExclusionModsecRules(
            eq(requestContext), any(GetExclusionModsecRulesRequest.class)))
        .thenAnswer(
            invocation -> {
              GetExclusionModsecRulesRequest request = invocation.getArgument(1);
              if (request.getRulesFilter().getRuleEvaluationPointsList().isEmpty()) {
                return combineModsecResponses(
                    platformResponse, inlineResponse, edgeResponse, multiPointResponse);
              }
              List<RuleEvaluationPoint> evaluationPoints =
                  request.getRulesFilter().getRuleEvaluationPointsList();
              if (evaluationPoints.size() == 1) {
                RuleEvaluationPoint point = evaluationPoints.get(0);
                if (point == RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM) {
                  return combineModsecResponses(platformResponse, multiPointResponse);
                } else if (point
                    == RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT) {
                  return inlineResponse;
                } else if (point == RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE) {
                  return combineModsecResponses(edgeResponse, multiPointResponse);
                }
              } else if (evaluationPoints.contains(
                      RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                  && evaluationPoints.contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)) {
                return combineModsecResponses(platformResponse, edgeResponse, multiPointResponse);
              }
              return GetExclusionModsecRulesResponse.getDefaultInstance();
            });
    doNothing()
        .when(rulesValidator)
        .validateOrThrow(eq(requestContext), any(GetExclusionModsecRulesRequest.class));

    // Test 1: Filter for PLATFORM rules
    StreamObserver<GetExclusionModsecRulesResponse> platformObserver = mock(StreamObserver.class);
    GetExclusionModsecRulesRequest platformRequest =
        GetExclusionModsecRulesRequest.newBuilder()
            .setRulesFilter(
                GetRulesFilter.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM))
            .build();
    requestContext.run(
        () ->
            detectionExclusionConfigService.getExclusionModsecRules(
                platformRequest, platformObserver));
    verify(platformObserver, times(1))
        .onNext(combineModsecResponses(platformResponse, multiPointResponse));
    verify(platformObserver, times(1)).onCompleted();

    // Test 2: Filter for INLINE_TRACING_AGENT rules
    StreamObserver<GetExclusionModsecRulesResponse> inlineObserver = mock(StreamObserver.class);
    GetExclusionModsecRulesRequest inlineRequest =
        GetExclusionModsecRulesRequest.newBuilder()
            .setRulesFilter(
                GetRulesFilter.newBuilder()
                    .addRuleEvaluationPoints(
                        RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT))
            .build();
    requestContext.run(
        () ->
            detectionExclusionConfigService.getExclusionModsecRules(inlineRequest, inlineObserver));
    verify(inlineObserver, times(1)).onNext(inlineResponse);
    verify(inlineObserver, times(1)).onCompleted();

    // Test 3: Filter for EDGE rules
    StreamObserver<GetExclusionModsecRulesResponse> edgeObserver = mock(StreamObserver.class);
    GetExclusionModsecRulesRequest edgeRequest =
        GetExclusionModsecRulesRequest.newBuilder()
            .setRulesFilter(
                GetRulesFilter.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE))
            .build();
    requestContext.run(
        () -> detectionExclusionConfigService.getExclusionModsecRules(edgeRequest, edgeObserver));
    verify(edgeObserver, times(1)).onNext(combineModsecResponses(edgeResponse, multiPointResponse));
    verify(edgeObserver, times(1)).onCompleted();

    // Test 4: Filter for multiple evaluation points
    StreamObserver<GetExclusionModsecRulesResponse> multiObserver = mock(StreamObserver.class);
    GetExclusionModsecRulesRequest multiRequest =
        GetExclusionModsecRulesRequest.newBuilder()
            .setRulesFilter(
                GetRulesFilter.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE))
            .build();
    requestContext.run(
        () -> detectionExclusionConfigService.getExclusionModsecRules(multiRequest, multiObserver));
    verify(multiObserver, times(1))
        .onNext(combineModsecResponses(platformResponse, edgeResponse, multiPointResponse));
    verify(multiObserver, times(1)).onCompleted();

    // Test 5: No filter (should return all rules)
    StreamObserver<GetExclusionModsecRulesResponse> allObserver = mock(StreamObserver.class);
    GetExclusionModsecRulesRequest allRequest =
        GetExclusionModsecRulesRequest.newBuilder()
            .setRulesFilter(GetRulesFilter.getDefaultInstance())
            .build();
    requestContext.run(
        () -> detectionExclusionConfigService.getExclusionModsecRules(allRequest, allObserver));
    verify(allObserver, times(1))
        .onNext(
            combineModsecResponses(
                platformResponse, inlineResponse, edgeResponse, multiPointResponse));
    verify(allObserver, times(1)).onCompleted();

    // Test 6: Validation failure
    StreamObserver<GetExclusionModsecRulesResponse> failureObserver = mock(StreamObserver.class);
    GetExclusionModsecRulesRequest failureRequest =
        GetExclusionModsecRulesRequest.newBuilder().build();
    Exception exception = Status.INVALID_ARGUMENT.asRuntimeException();
    doThrow(exception).when(rulesValidator).validateOrThrow(eq(requestContext), eq(failureRequest));
    requestContext.run(
        () ->
            detectionExclusionConfigService.getExclusionModsecRules(
                failureRequest, failureObserver));
    verify(failureObserver, times(1)).onError(exception);
    verify(failureObserver, times(0)).onCompleted();
  }

  @Test
  void testGetDetectionExclusionEdgeDecisionRules() {
    DetectionExclusionRule platformRule =
        getRule("platform-rule-id", List.of(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM));
    DetectionExclusionRule inlineRule =
        getRule(
            "inline-rule-id",
            List.of(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT));
    DetectionExclusionRule edgeRule =
        getRule("edge-rule-id", List.of(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE));
    DetectionExclusionRule multiPointRule =
        getRule(
            "multi-point-rule-id",
            List.of(
                RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM,
                RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE));
    List<DetectionExclusionRule> allRules =
        List.of(platformRule, inlineRule, edgeRule, multiPointRule);
    when(featureCachingClient.isEdgeDecisionEnabledForTenant(eq(requestContext))).thenReturn(true);
    when(edgeDecisionConverter.convert(eq(requestContext), any()))
        .thenAnswer(
            invocation -> {
              List<DetectionExclusionRule> rules = invocation.getArgument(1);
              return EdgeDecisionEngineConfig.newBuilder()
                  .setId("edge-config-with-" + rules.size() + "-rules")
                  .build();
            });
    when(rulesManager.getDetectionExclusionRules(eq(requestContext), any()))
        .thenAnswer(
            invocation -> {
              GetRulesFilter filter = invocation.getArgument(1);
              if (filter.getRuleEvaluationPointsList().isEmpty()) {
                return allRules;
              }
              return allRules.stream()
                  .filter(
                      rule ->
                          rule.getRuleInfo().getRuleEvaluationPointsList().stream()
                              .anyMatch(filter.getRuleEvaluationPointsList()::contains))
                  .collect(Collectors.toList());
            });
    doNothing()
        .when(rulesValidator)
        .validateOrThrow(
            eq(requestContext), any(GetDetectionExclusionEdgeDecisionRulesRequest.class));

    // Test 1: Filter for PLATFORM rules
    StreamObserver<GetDetectionExclusionEdgeDecisionRulesResponse> platformObserver =
        mock(StreamObserver.class);
    GetDetectionExclusionEdgeDecisionRulesRequest platformRequest =
        GetDetectionExclusionEdgeDecisionRulesRequest.newBuilder()
            .setFilter(
                GetRulesFilter.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                    .build())
            .build();
    requestContext.run(
        () ->
            detectionExclusionConfigService.getDetectionExclusionEdgeDecisionRules(
                platformRequest, platformObserver));
    verify(edgeDecisionConverter, times(1))
        .convert(
            eq(requestContext),
            argThat(
                rules ->
                    rules.size() == 2
                        && rules.contains(platformRule)
                        && rules.contains(multiPointRule)));
    verify(platformObserver, times(1))
        .onNext(
            GetDetectionExclusionEdgeDecisionRulesResponse.newBuilder()
                .setEdgeDecisionEngineConfig(
                    EdgeDecisionEngineConfig.newBuilder().setId("edge-config-with-2-rules").build())
                .build());
    verify(platformObserver, times(1)).onCompleted();

    // Test 2: Filter for INLINE_TRACING_AGENT rules
    StreamObserver<GetDetectionExclusionEdgeDecisionRulesResponse> inlineObserver =
        mock(StreamObserver.class);
    GetDetectionExclusionEdgeDecisionRulesRequest inlineRequest =
        GetDetectionExclusionEdgeDecisionRulesRequest.newBuilder()
            .setFilter(
                GetRulesFilter.newBuilder()
                    .addRuleEvaluationPoints(
                        RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT)
                    .build())
            .build();
    requestContext.run(
        () ->
            detectionExclusionConfigService.getDetectionExclusionEdgeDecisionRules(
                inlineRequest, inlineObserver));
    verify(edgeDecisionConverter, times(1))
        .convert(
            eq(requestContext), argThat(rules -> rules.size() == 1 && rules.contains(inlineRule)));
    verify(inlineObserver, times(1))
        .onNext(
            GetDetectionExclusionEdgeDecisionRulesResponse.newBuilder()
                .setEdgeDecisionEngineConfig(
                    EdgeDecisionEngineConfig.newBuilder().setId("edge-config-with-1-rules").build())
                .build());
    verify(inlineObserver, times(1)).onCompleted();

    // Test 3: Filter for EDGE rules
    StreamObserver<GetDetectionExclusionEdgeDecisionRulesResponse> edgeObserver =
        mock(StreamObserver.class);
    GetDetectionExclusionEdgeDecisionRulesRequest edgeRequest =
        GetDetectionExclusionEdgeDecisionRulesRequest.newBuilder()
            .setFilter(
                GetRulesFilter.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                    .build())
            .build();
    requestContext.run(
        () ->
            detectionExclusionConfigService.getDetectionExclusionEdgeDecisionRules(
                edgeRequest, edgeObserver));
    verify(edgeDecisionConverter, times(1))
        .convert(
            eq(requestContext),
            argThat(
                rules ->
                    rules.size() == 2
                        && rules.contains(edgeRule)
                        && rules.contains(multiPointRule)));
    verify(edgeObserver, times(1))
        .onNext(
            GetDetectionExclusionEdgeDecisionRulesResponse.newBuilder()
                .setEdgeDecisionEngineConfig(
                    EdgeDecisionEngineConfig.newBuilder().setId("edge-config-with-2-rules").build())
                .build());
    verify(edgeObserver, times(1)).onCompleted();

    // Test 4: Filter for multiple evaluation points
    StreamObserver<GetDetectionExclusionEdgeDecisionRulesResponse> multiObserver =
        mock(StreamObserver.class);
    GetDetectionExclusionEdgeDecisionRulesRequest multiRequest =
        GetDetectionExclusionEdgeDecisionRulesRequest.newBuilder()
            .setFilter(
                GetRulesFilter.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                    .build())
            .build();
    requestContext.run(
        () ->
            detectionExclusionConfigService.getDetectionExclusionEdgeDecisionRules(
                multiRequest, multiObserver));
    verify(edgeDecisionConverter, times(1))
        .convert(
            eq(requestContext),
            argThat(
                rules ->
                    rules.size() == 3
                        && rules.contains(platformRule)
                        && rules.contains(edgeRule)
                        && rules.contains(multiPointRule)));
    verify(multiObserver, times(1))
        .onNext(
            GetDetectionExclusionEdgeDecisionRulesResponse.newBuilder()
                .setEdgeDecisionEngineConfig(
                    EdgeDecisionEngineConfig.newBuilder().setId("edge-config-with-3-rules").build())
                .build());
    verify(multiObserver, times(1)).onCompleted();

    // Test 5: No filter (should return all rules)
    StreamObserver<GetDetectionExclusionEdgeDecisionRulesResponse> allObserver =
        mock(StreamObserver.class);
    GetDetectionExclusionEdgeDecisionRulesRequest allRequest =
        GetDetectionExclusionEdgeDecisionRulesRequest.newBuilder()
            .setFilter(GetRulesFilter.getDefaultInstance())
            .build();
    requestContext.run(
        () ->
            detectionExclusionConfigService.getDetectionExclusionEdgeDecisionRules(
                allRequest, allObserver));
    verify(edgeDecisionConverter, times(1))
        .convert(
            eq(requestContext),
            argThat(
                rules ->
                    rules.size() == 4
                        && rules.contains(platformRule)
                        && rules.contains(inlineRule)
                        && rules.contains(edgeRule)
                        && rules.contains(multiPointRule)));
    verify(allObserver, times(1))
        .onNext(
            GetDetectionExclusionEdgeDecisionRulesResponse.newBuilder()
                .setEdgeDecisionEngineConfig(
                    EdgeDecisionEngineConfig.newBuilder().setId("edge-config-with-4-rules").build())
                .build());
    verify(allObserver, times(1)).onCompleted();

    // Test 6: Feature flag disabled
    when(featureCachingClient.isEdgeDecisionEnabledForTenant(eq(requestContext))).thenReturn(false);
    StreamObserver<GetDetectionExclusionEdgeDecisionRulesResponse> disabledObserver =
        mock(StreamObserver.class);
    GetDetectionExclusionEdgeDecisionRulesRequest disabledRequest =
        GetDetectionExclusionEdgeDecisionRulesRequest.newBuilder()
            .setFilter(GetRulesFilter.getDefaultInstance())
            .build();
    requestContext.run(
        () ->
            detectionExclusionConfigService.getDetectionExclusionEdgeDecisionRules(
                disabledRequest, disabledObserver));
    verify(disabledObserver, times(1))
        .onNext(GetDetectionExclusionEdgeDecisionRulesResponse.getDefaultInstance());
    verify(disabledObserver, times(1)).onCompleted();

    // Test 7: Validation failure
    when(featureCachingClient.isEdgeDecisionEnabledForTenant(eq(requestContext))).thenReturn(true);
    StreamObserver<GetDetectionExclusionEdgeDecisionRulesResponse> failureObserver =
        mock(StreamObserver.class);
    GetDetectionExclusionEdgeDecisionRulesRequest failureRequest =
        GetDetectionExclusionEdgeDecisionRulesRequest.newBuilder().build();
    Exception exception = Status.INVALID_ARGUMENT.asRuntimeException();
    doThrow(exception).when(rulesValidator).validateOrThrow(eq(requestContext), eq(failureRequest));
    requestContext.run(
        () ->
            detectionExclusionConfigService.getDetectionExclusionEdgeDecisionRules(
                failureRequest, failureObserver));
    verify(failureObserver, times(1)).onError(exception);
    verify(failureObserver, times(0)).onCompleted();
  }

  @Test
  void testBulkDeleteDetectionExclusionRules() {
    BulkDeleteDetectionExclusionRulesRequest request =
        BulkDeleteDetectionExclusionRulesRequest.getDefaultInstance();
    StreamObserver<BulkDeleteDetectionExclusionRulesResponse> streamObserver =
        mock(StreamObserver.class);

    // validation succeeds
    doNothing().when(rulesValidator).validateOrThrow(requestContext, request);

    requestContext.run(
        () ->
            detectionExclusionConfigService.bulkDeleteDetectionExclusionRules(
                request, streamObserver));
    verify(rulesManager, times(1)).bulkDeleteDetectionExclusionRules(eq(requestContext), any());
    verify(streamObserver, times(1))
        .onNext(BulkDeleteDetectionExclusionRulesResponse.getDefaultInstance());
    verify(streamObserver, times(1)).onCompleted();
  }

  @Test
  void testBulkUpsertDetectionExclusionRules() {
    BulkUpsertDetectionExclusionRulesRequest request =
        BulkUpsertDetectionExclusionRulesRequest.getDefaultInstance();
    StreamObserver<BulkUpsertDetectionExclusionRulesResponse> streamObserver =
        mock(StreamObserver.class);

    // validation succeeds
    doNothing()
        .when(rulesValidator)
        .validateOrThrowBulkUpsertRequest(requestContext, request.getRulesList());

    requestContext.run(
        () ->
            detectionExclusionConfigService.bulkUpsertDetectionExclusionRules(
                request, streamObserver));
    verify(rulesManager, times(1)).bulkUpsertDetectionExclusionRule(eq(requestContext), any());
    verify(streamObserver, times(1))
        .onNext(BulkUpsertDetectionExclusionRulesResponse.getDefaultInstance());
    verify(streamObserver, times(1)).onCompleted();
  }

  private DetectionExclusionRule getRule(
      String ruleId, List<RuleEvaluationPoint> ruleEvaluationPoints) {
    return DetectionExclusionRule.newBuilder()
        .setId(ruleId)
        .setRuleInfo(
            DetectionExclusionRuleInfo.newBuilder()
                .setRuleStatus(
                    DetectionExclusionRuleStatus.newBuilder()
                        .setRuleCreationSource(RuleSource.RULE_SOURCE_TRACEABLE))
                .addAllRuleEvaluationPoints(ruleEvaluationPoints))
        .build();
  }

  private GetExclusionModsecRulesResponse createModsecRulesResponse(
      String modsecDirectivesBlob, List<RuleEvaluationPoint> ruleEvaluationPoints) {
    return GetExclusionModsecRulesResponse.newBuilder()
        .setModsecDirectivesBlob(modsecDirectivesBlob)
        .addModsecRules(
            DetectionExclusionModsecRule.newBuilder()
                .addAssociatedModsecRuleIds(modsecDirectivesBlob)
                .setRule(
                    DetectionExclusionRule.newBuilder()
                        .setId(modsecDirectivesBlob)
                        .setRuleInfo(
                            DetectionExclusionRuleInfo.newBuilder()
                                .addAllRuleEvaluationPoints(ruleEvaluationPoints))
                        .setRuleScope(
                            DetectionExclusionRuleScope.newBuilder()
                                .setEnvironmentScope(
                                    EnvironmentScope.newBuilder().addEnvironmentIds("env-id")))))
        .build();
  }

  private GetExclusionModsecRulesResponse combineModsecResponses(
      GetExclusionModsecRulesResponse... responses) {
    GetExclusionModsecRulesResponse.Builder builder = GetExclusionModsecRulesResponse.newBuilder();
    for (GetExclusionModsecRulesResponse response : responses) {
      for (DetectionExclusionModsecRule modsecRule : response.getModsecRulesList()) {
        builder.addModsecRules(modsecRule);
      }
    }
    return builder.build();
  }
}
