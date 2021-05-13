package ai.traceable.threatmanagement.config.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.threatmanagement.config.service.eventscore.SecurityEventScoreContributionManager;
import ai.traceable.threatmanagement.config.service.threatscore.ThreatScoreManager;
import ai.traceable.threatmanagement.config.service.v1.GetSecurityEventScoreContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.GetSecurityEventScoreContributionResponse;
import ai.traceable.threatmanagement.config.service.v1.GetThreatScoreBoundRequest;
import ai.traceable.threatmanagement.config.service.v1.GetThreatScoreBoundResponse;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventScoreContribution;
import ai.traceable.threatmanagement.config.service.v1.ThreatScoreBound;
import ai.traceable.threatmanagement.config.service.v1.UpdateSecurityEventScoreContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateSecurityEventScoreContributionResponse;
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

  private static final int DEFAULT_SECURITY_EVENT_CONTRIBUTION_ANOMALY_SCORE = 1;
  private static final int DEFAULT_SECURITY_EVENT_CONTRIBUTION_MEDIUM_SCORE = 2;
  private static final int DEFAULT_SECURITY_EVENT_CONTRIBUTION_HIGH_SCORE = 3;

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

  private static final SecurityEventScoreContribution DEFAULT_SECURITY_EVENT_SCORE_CONTRIBUTION =
      SecurityEventScoreContribution.newBuilder()
          .setAnomalyScore(DEFAULT_SECURITY_EVENT_CONTRIBUTION_ANOMALY_SCORE)
          .setMediumScore(DEFAULT_SECURITY_EVENT_CONTRIBUTION_MEDIUM_SCORE)
          .setHighScore(DEFAULT_SECURITY_EVENT_CONTRIBUTION_HIGH_SCORE)
          .build();
  private static final SecurityEventScoreContribution SECURITY_EVENT_SCORE_CONTRIBUTION_1 =
      SecurityEventScoreContribution.newBuilder()
          .setAnomalyScore(10)
          .setMediumScore(20)
          .setHighScore(30)
          .build();
  private static final SecurityEventScoreContribution SECURITY_EVENT_SCORE_CONTRIBUTION_2 =
      SecurityEventScoreContribution.newBuilder()
          .setAnomalyScore(100)
          .setMediumScore(200)
          .setHighScore(300)
          .build();

  private ThreatManagementConfigRequestValidator requestValidator;
  private ThreatScoreManager threatScoreManager;
  private SecurityEventScoreContributionManager securityEventScoreContributionManager;

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
    doNothing()
        .when(requestValidator)
        .validateOrThrow(
            any(RequestContext.class), any(GetSecurityEventScoreContributionRequest.class));
    doNothing()
        .when(requestValidator)
        .validateOrThrow(
            any(RequestContext.class), any(UpdateSecurityEventScoreContributionRequest.class));

    this.threatScoreManager = mock(ThreatScoreManager.class);
    when(this.threatScoreManager.getDefaultThreatScoreBound())
        .thenReturn(DEFAULT_THREAT_SCORE_BOUND);

    this.securityEventScoreContributionManager = mock(SecurityEventScoreContributionManager.class);
    when(this.securityEventScoreContributionManager.getDefaultSecurityEventScoreContribution())
        .thenReturn(DEFAULT_SECURITY_EVENT_SCORE_CONTRIBUTION);

    this.threatManagementConfigService =
        new ThreatManagementConfigServiceImpl(
            requestValidator, threatScoreManager, securityEventScoreContributionManager);
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

  @Nested
  class SecurityEventScoreContributions {
    @Test
    void getSecurityEventContribution() {
      when(securityEventScoreContributionManager.getSecurityEventScoreContribution(
              any(RequestContext.class)))
          .thenReturn(SECURITY_EVENT_SCORE_CONTRIBUTION_1);

      StreamObserver<GetSecurityEventScoreContributionResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.getSecurityEventScoreContribution(
                  GetSecurityEventScoreContributionRequest.getDefaultInstance(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(
              GetSecurityEventScoreContributionResponse.newBuilder()
                  .setSecurityEventScoreContribution(SECURITY_EVENT_SCORE_CONTRIBUTION_1)
                  .setDefaultSecurityEventScoreContribution(
                      DEFAULT_SECURITY_EVENT_SCORE_CONTRIBUTION)
                  .build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    void updateSecurityEventScoreContribution() {
      when(securityEventScoreContributionManager.upsertSecurityEventScoreContribution(
              any(RequestContext.class), eq(SECURITY_EVENT_SCORE_CONTRIBUTION_1)))
          .thenReturn(SECURITY_EVENT_SCORE_CONTRIBUTION_2);

      StreamObserver<UpdateSecurityEventScoreContributionResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.updateSecurityEventScoreContribution(
                  UpdateSecurityEventScoreContributionRequest.newBuilder()
                      .setSecurityEventScoreContribution(SECURITY_EVENT_SCORE_CONTRIBUTION_1)
                      .build(),
                  responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(
              UpdateSecurityEventScoreContributionResponse.newBuilder()
                  .setSecurityEventScoreContribution(SECURITY_EVENT_SCORE_CONTRIBUTION_2)
                  .setDefaultSecurityEventScoreContribution(
                      DEFAULT_SECURITY_EVENT_SCORE_CONTRIBUTION)
                  .build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    void skipUpdateSecurityEventScoreContribution() {
      when(securityEventScoreContributionManager.upsertSecurityEventScoreContribution(
              any(RequestContext.class), eq(SECURITY_EVENT_SCORE_CONTRIBUTION_1)))
          .thenThrow(IllegalArgumentException.class);

      StreamObserver<UpdateSecurityEventScoreContributionResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.updateSecurityEventScoreContribution(
                  UpdateSecurityEventScoreContributionRequest.newBuilder()
                      .setSecurityEventScoreContribution(SECURITY_EVENT_SCORE_CONTRIBUTION_1)
                      .build(),
                  responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.UNKNOWN));
    }
  }
}
