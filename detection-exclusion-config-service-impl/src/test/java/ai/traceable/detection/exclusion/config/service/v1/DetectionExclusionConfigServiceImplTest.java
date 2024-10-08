package ai.traceable.detection.exclusion.config.service.v1;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.detection.exclusion.config.service.v1.rules.RulesManager;
import ai.traceable.detection.exclusion.config.service.v1.rules.RulesValidator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DetectionExclusionConfigServiceImplTest {

  private RulesManager rulesManager;
  private RulesValidator rulesValidator;
  private DetectionExclusionConfigServiceImpl detectionExclusionConfigService;
  private final RequestContext requestContext = RequestContext.forTenantId("tenantId");

  @BeforeEach
  void setUp() {
    rulesManager = mock(RulesManager.class);
    rulesValidator = mock(RulesValidator.class);
    detectionExclusionConfigService =
        new DetectionExclusionConfigServiceImpl(rulesManager, rulesValidator);
  }

  @Test
  void testGetDetectionExclusionRules() {
    DetectionExclusionRule detectionExclusionRule = DetectionExclusionRule.getDefaultInstance();
    GetDetectionExclusionRulesRequest request =
        GetDetectionExclusionRulesRequest.getDefaultInstance();
    StreamObserver<GetDetectionExclusionRulesResponse> streamObserver = mock(StreamObserver.class);

    when(rulesManager.getDetectionExclusionRules(eq(requestContext), any()))
        .thenReturn(List.of(detectionExclusionRule));

    // validation throws error
    Exception exception = Status.INVALID_ARGUMENT.asRuntimeException();
    doThrow(exception).when(rulesValidator).validateOrThrow(requestContext, request);

    requestContext.run(
        () -> detectionExclusionConfigService.getDetectionExclusionRules(request, streamObserver));
    verify(streamObserver, times(1)).onError(exception);

    // validation succeeds
    doNothing().when(rulesValidator).validateOrThrow(requestContext, request);

    requestContext.run(
        () -> detectionExclusionConfigService.getDetectionExclusionRules(request, streamObserver));
    verify(streamObserver, times(1))
        .onNext(
            GetDetectionExclusionRulesResponse.newBuilder()
                .addRules(detectionExclusionRule)
                .build());
    verify(streamObserver, times(1)).onCompleted();
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
    GetExclusionModsecRulesResponse exclusionModsecRulesResponse =
        GetExclusionModsecRulesResponse.getDefaultInstance();
    GetExclusionModsecRulesRequest request = GetExclusionModsecRulesRequest.getDefaultInstance();
    StreamObserver<GetExclusionModsecRulesResponse> streamObserver = mock(StreamObserver.class);

    when(rulesManager.getDetectionExclusionModsecRules(eq(requestContext), any()))
        .thenReturn(exclusionModsecRulesResponse);

    // validation throws error
    Exception exception = Status.INVALID_ARGUMENT.asRuntimeException();
    doThrow(exception).when(rulesValidator).validateOrThrow(requestContext, request);

    requestContext.run(
        () -> detectionExclusionConfigService.getExclusionModsecRules(request, streamObserver));
    verify(streamObserver, times(1)).onError(exception);

    // validation succeeds
    doNothing().when(rulesValidator).validateOrThrow(requestContext, request);

    requestContext.run(
        () -> detectionExclusionConfigService.getExclusionModsecRules(request, streamObserver));
    verify(streamObserver, times(1)).onNext(exclusionModsecRulesResponse);
    verify(streamObserver, times(1)).onCompleted();
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
  void testBulkCreateDetectionExclusionRules() {
    BulkCreateDetectionExclusionRulesRequest request =
        BulkCreateDetectionExclusionRulesRequest.getDefaultInstance();
    StreamObserver<BulkCreateDetectionExclusionRulesResponse> streamObserver =
        mock(StreamObserver.class);

    // validation succeeds
    doNothing().when(rulesValidator).validateOrThrow(requestContext, request.getRuleDataList());

    requestContext.run(
        () ->
            detectionExclusionConfigService.bulkCreateDetectionExclusionRules(
                request, streamObserver));
    verify(rulesManager, times(1)).bulkCreateDetectionExclusionRule(eq(requestContext), any());
    verify(streamObserver, times(1))
        .onNext(BulkCreateDetectionExclusionRulesResponse.getDefaultInstance());
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
}
