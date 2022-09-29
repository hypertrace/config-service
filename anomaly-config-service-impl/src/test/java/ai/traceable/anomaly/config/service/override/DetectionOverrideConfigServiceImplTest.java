package ai.traceable.anomaly.config.service.override;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.override.exclusion.ExclusionRulesManager;
import ai.traceable.anomaly.config.service.override.exclusion.ExclusionRulesValidator;
import ai.traceable.anomaly.config.service.v1.override.CreateDetectionExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.override.CreateDetectionExclusionRuleResponse;
import ai.traceable.anomaly.config.service.v1.override.DeleteDetectionExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.override.DeleteDetectionExclusionRuleResponse;
import ai.traceable.anomaly.config.service.v1.override.DetectionExclusionRule;
import ai.traceable.anomaly.config.service.v1.override.GetDetectionExclusionRulesRequest;
import ai.traceable.anomaly.config.service.v1.override.GetDetectionExclusionRulesResponse;
import ai.traceable.anomaly.config.service.v1.override.UpdateDetectionExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.override.UpdateDetectionExclusionRuleResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DetectionOverrideConfigServiceImplTest {
  private final String TENANT_ID = "test-tenant";
  private ExclusionRulesValidator rulesValidator;
  private ExclusionRulesManager rulesManager;
  private DetectionOverrideConfigServiceImpl detectionOverrideConfigService;

  @BeforeEach
  void setup() {
    rulesValidator = mock(ExclusionRulesValidator.class);
    rulesManager = mock(ExclusionRulesManager.class);
    detectionOverrideConfigService =
        new DetectionOverrideConfigServiceImpl(rulesValidator, rulesManager);
  }

  @Test
  public void testGetDetectionExclusionRules() {
    StreamObserver<GetDetectionExclusionRulesResponse> responseObserver =
        mock(StreamObserver.class);
    Runnable runnable =
        () ->
            detectionOverrideConfigService.getDetectionExclusionRules(
                GetDetectionExclusionRulesRequest.newBuilder().build(), responseObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

    verify(responseObserver, times(1))
        .onNext(GetDetectionExclusionRulesResponse.newBuilder().build());
    verify(responseObserver, times(1)).onCompleted();

    reset(responseObserver);

    List<DetectionExclusionRule> rules =
        List.of(
            DetectionExclusionRule.newBuilder().setId("id1").build(),
            DetectionExclusionRule.newBuilder().setId("id2").build());

    when(rulesManager.getDetectionExclusionRules(any(), any())).thenReturn(rules);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

    verify(responseObserver, times(1))
        .onNext(GetDetectionExclusionRulesResponse.newBuilder().addAllRules(rules).build());
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  public void testCreateDetectionExclusionRule() {
    StreamObserver<CreateDetectionExclusionRuleResponse> responseObserver =
        mock(StreamObserver.class);
    Runnable runnable =
        () ->
            detectionOverrideConfigService.createDetectionExclusionRule(
                CreateDetectionExclusionRuleRequest.newBuilder().build(), responseObserver);
    when(rulesValidator.validate((CreateDetectionExclusionRuleRequest) any()))
        .thenReturn(Status.OK);
    DetectionExclusionRule rule = DetectionExclusionRule.newBuilder().setId("id1").build();
    when(rulesManager.createDetectionExclusionRule(any(), any())).thenReturn(rule);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

    verify(responseObserver, times(1))
        .onNext(CreateDetectionExclusionRuleResponse.newBuilder().setRule(rule).build());
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  public void testUpdateDetectionExclusionRule() {
    StreamObserver<UpdateDetectionExclusionRuleResponse> responseObserver =
        mock(StreamObserver.class);
    Runnable runnable =
        () ->
            detectionOverrideConfigService.updateDetectionExclusionRule(
                UpdateDetectionExclusionRuleRequest.newBuilder().build(), responseObserver);
    when(rulesValidator.validate((UpdateDetectionExclusionRuleRequest) any()))
        .thenReturn(Status.OK);
    DetectionExclusionRule rule = DetectionExclusionRule.newBuilder().setId("id1").build();
    when(rulesManager.updateDetectionExclusionRule(any(), any())).thenReturn(rule);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

    verify(responseObserver, times(1))
        .onNext(UpdateDetectionExclusionRuleResponse.newBuilder().setRule(rule).build());
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  public void testDeleteDetectionExclusionRule() {
    DeleteDetectionExclusionRuleRequest request =
        DeleteDetectionExclusionRuleRequest.newBuilder().setRuleId("id1").build();
    StreamObserver<DeleteDetectionExclusionRuleResponse> responseObserver =
        mock(StreamObserver.class);
    Runnable runnable =
        () ->
            detectionOverrideConfigService.deleteDetectionExclusionRule(request, responseObserver);
    when(rulesValidator.validate((DeleteDetectionExclusionRuleRequest) any()))
        .thenReturn(Status.OK);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

    verify(responseObserver, times(1))
        .onNext(DeleteDetectionExclusionRuleResponse.newBuilder().build());
    verify(responseObserver, times(1)).onCompleted();
  }
}
