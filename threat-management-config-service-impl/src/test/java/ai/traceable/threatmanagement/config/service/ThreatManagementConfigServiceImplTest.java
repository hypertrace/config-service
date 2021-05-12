package ai.traceable.threatmanagement.config.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.threatmanagement.config.service.threatscore.ThreatScoreManager;
import ai.traceable.threatmanagement.config.service.v1.GetThreatScoreBoundRequest;
import ai.traceable.threatmanagement.config.service.v1.GetThreatScoreBoundResponse;
import ai.traceable.threatmanagement.config.service.v1.ThreatScoreBound;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatScoreBoundRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatScoreBoundResponse;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ThreatManagementConfigServiceImplTest {
  private static final String TENANT_ID = "tenant-1";

  private static final int DEFAULT_UPPER_BOUND_MEDIUM_THREAT_SCORE = 10;
  private static final int DEFAULT_UPPER_BOUND_HIGH_THREAT_SCORE = 20;

  private static final ThreatScoreBound DEFAULT_THREAT_SCORE_BOUND =
      ThreatScoreBound.newBuilder()
          .setMediumScoreUpperBound(DEFAULT_UPPER_BOUND_MEDIUM_THREAT_SCORE)
          .setHighScoreUpperBound(DEFAULT_UPPER_BOUND_HIGH_THREAT_SCORE)
          .build();
  private static final ThreatScoreBound THREAT_SCORE_BOUND_1 =
      ThreatScoreBound.newBuilder()
          .setMediumScoreUpperBound(50)
          .setHighScoreUpperBound(100)
          .build();
  private static final ThreatScoreBound THREAT_SCORE_BOUND_2 =
      ThreatScoreBound.newBuilder()
          .setMediumScoreUpperBound(200)
          .setHighScoreUpperBound(300)
          .build();

  private ThreatManagementConfigRequestValidator requestValidator;
  private ThreatManagementConfigServiceConfig config;
  private ThreatScoreManager threatScoreManager;

  private ThreatManagementConfigServiceImpl threatManagementConfigService;

  @BeforeEach
  void setup() {
    requestValidator = mock(ThreatManagementConfigRequestValidator.class);
    doNothing()
        .when(requestValidator)
        .validateOrThrow(any(RequestContext.class), any(GetThreatScoreBoundRequest.class));
    doNothing()
        .when(requestValidator)
        .validateOrThrow(any(RequestContext.class), any(UpdateThreatScoreBoundRequest.class));

    this.config = mock(ThreatManagementConfigServiceConfig.class);
    when(this.config.getDefaultThreatUpperBoundMediumScore())
        .thenReturn(DEFAULT_UPPER_BOUND_MEDIUM_THREAT_SCORE);
    when(this.config.getDefaultThreatUpperBoundHighScore())
        .thenReturn(DEFAULT_UPPER_BOUND_HIGH_THREAT_SCORE);

    this.threatScoreManager = mock(ThreatScoreManager.class);
    this.threatManagementConfigService =
        new ThreatManagementConfigServiceImpl(config, requestValidator, threatScoreManager);
  }

  @Nested
  class ThreatScoreBounds {
    @Test
    void getThreatScoreBound() {
      when(threatScoreManager.getThreatScoreBound(any(RequestContext.class)))
          .thenReturn(THREAT_SCORE_BOUND_1);

      StreamObserver<GetThreatScoreBoundResponse> responseObserver = mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.getThreatScoreBound(
                  GetThreatScoreBoundRequest.getDefaultInstance(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(
              GetThreatScoreBoundResponse.newBuilder()
                  .setThreatScoreBound(THREAT_SCORE_BOUND_1)
                  .setDefaultThreatScoreBound(DEFAULT_THREAT_SCORE_BOUND)
                  .build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    void updateThreatScoreBound() {
      when(threatScoreManager.upsertThreatScoreBound(
              any(RequestContext.class), eq(THREAT_SCORE_BOUND_1)))
          .thenReturn(THREAT_SCORE_BOUND_2);

      StreamObserver<UpdateThreatScoreBoundResponse> responseObserver = mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.updateThreatScoreBound(
                  UpdateThreatScoreBoundRequest.newBuilder()
                      .setThreatScoreBound(THREAT_SCORE_BOUND_1)
                      .build(),
                  responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(
              UpdateThreatScoreBoundResponse.newBuilder()
                  .setThreatScoreBound(THREAT_SCORE_BOUND_2)
                  .setDefaultThreatScoreBound(DEFAULT_THREAT_SCORE_BOUND)
                  .build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    void skipUpdateThreatScoreBound() {
      when(threatScoreManager.upsertThreatScoreBound(
              any(RequestContext.class), eq(THREAT_SCORE_BOUND_1)))
          .thenThrow(IllegalArgumentException.class);

      StreamObserver<UpdateThreatScoreBoundResponse> responseObserver = mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.updateThreatScoreBound(
                  UpdateThreatScoreBoundRequest.newBuilder()
                      .setThreatScoreBound(THREAT_SCORE_BOUND_1)
                      .build(),
                  responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.UNKNOWN));
    }
  }
}
