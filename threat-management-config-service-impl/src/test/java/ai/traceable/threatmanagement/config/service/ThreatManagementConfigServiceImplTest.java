package ai.traceable.threatmanagement.config.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.threatmanagement.config.service.anomalyscore.AnomalyScoreContributionManager;
import ai.traceable.threatmanagement.config.service.eventscore.SecurityEventScoreContributionManager;
import ai.traceable.threatmanagement.config.service.eventtype.SecurityEventTypeContributionManager;
import ai.traceable.threatmanagement.config.service.ipreputation.IpReputationThreatScoreConfigManager;
import ai.traceable.threatmanagement.config.service.statuscode.StatusCodeThreatScoreConfigsManager;
import ai.traceable.threatmanagement.config.service.threatautoblocking.ThreatAutoBlockingManager;
import ai.traceable.threatmanagement.config.service.threatscore.ThreatScoreDecayManager;
import ai.traceable.threatmanagement.config.service.threatscore.ThreatScoreManager;
import ai.traceable.threatmanagement.config.service.v1.AnomalyScoreContribution;
import ai.traceable.threatmanagement.config.service.v1.GetAnomalyScoreContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.GetAnomalyScoreContributionResponse;
import ai.traceable.threatmanagement.config.service.v1.GetIpReputationThreatScoreConfigRequest;
import ai.traceable.threatmanagement.config.service.v1.GetIpReputationThreatScoreConfigResponse;
import ai.traceable.threatmanagement.config.service.v1.GetSecurityEventScoreContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.GetSecurityEventScoreContributionResponse;
import ai.traceable.threatmanagement.config.service.v1.GetSecurityEventTypeContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.GetSecurityEventTypeContributionResponse;
import ai.traceable.threatmanagement.config.service.v1.GetStatusCodeThreatScoreConfigsRequest;
import ai.traceable.threatmanagement.config.service.v1.GetStatusCodeThreatScoreConfigsResponse;
import ai.traceable.threatmanagement.config.service.v1.GetThreatAutoBlockingConfigRequest;
import ai.traceable.threatmanagement.config.service.v1.GetThreatAutoBlockingConfigResponse;
import ai.traceable.threatmanagement.config.service.v1.GetThreatScoreBoundRequest;
import ai.traceable.threatmanagement.config.service.v1.GetThreatScoreBoundResponse;
import ai.traceable.threatmanagement.config.service.v1.GetThreatScoreConfigRequest;
import ai.traceable.threatmanagement.config.service.v1.GetThreatScoreConfigResponse;
import ai.traceable.threatmanagement.config.service.v1.IpReputationThreatScoreConfig;
import ai.traceable.threatmanagement.config.service.v1.Protocol;
import ai.traceable.threatmanagement.config.service.v1.ScopeConfig;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventScoreContribution;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventTypeContribution;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventTypeContribution.SecurityEventTypeContributionKind;
import ai.traceable.threatmanagement.config.service.v1.SeverityDowngradePolicy;
import ai.traceable.threatmanagement.config.service.v1.StatusCodeThreatScoreConfig;
import ai.traceable.threatmanagement.config.service.v1.StatusCodeThreatScoreConfigs;
import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionConfig;
import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionConfig.ExpirationDetails;
import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionType;
import ai.traceable.threatmanagement.config.service.v1.ThreatScoreBound;
import ai.traceable.threatmanagement.config.service.v1.ThreatScoreConfig;
import ai.traceable.threatmanagement.config.service.v1.UpdateAnomalyScoreContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateAnomalyScoreContributionResponse;
import ai.traceable.threatmanagement.config.service.v1.UpdateIpReputationThreatScoreConfigRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateIpReputationThreatScoreConfigResponse;
import ai.traceable.threatmanagement.config.service.v1.UpdateSecurityEventScoreContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateSecurityEventScoreContributionResponse;
import ai.traceable.threatmanagement.config.service.v1.UpdateSecurityEventTypeContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateSecurityEventTypeContributionResponse;
import ai.traceable.threatmanagement.config.service.v1.UpdateStatusCodeThreatScoreConfigsRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateStatusCodeThreatScoreConfigsResponse;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatAutoBlockingConfigRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatAutoBlockingConfigResponse;
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
import org.mockito.Mock;

class ThreatManagementConfigServiceImplTest {
  private static final String TENANT_ID = "tenant-1";

  private static final int DEFAULT_UPPER_BOUND_MEDIUM_THREAT_SCORE = 10;
  private static final int DEFAULT_UPPER_BOUND_HIGH_THREAT_SCORE = 20;

  private static final int DEFAULT_SECURITY_EVENT_CONTRIBUTION_LOW_SCORE = 1;
  private static final int DEFAULT_SECURITY_EVENT_CONTRIBUTION_MEDIUM_SCORE = 2;
  private static final int DEFAULT_SECURITY_EVENT_CONTRIBUTION_HIGH_SCORE = 3;
  private static final int DEFAULT_SECURITY_EVENT_CONTRIBUTION_CRITICAL_SCORE = 10;

  private static final int DEFAULT_ANOMALY_CONTRIBUTION_SCORE = 1;

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
          .setLowScore(DEFAULT_SECURITY_EVENT_CONTRIBUTION_LOW_SCORE)
          .setMediumScore(DEFAULT_SECURITY_EVENT_CONTRIBUTION_MEDIUM_SCORE)
          .setHighScore(DEFAULT_SECURITY_EVENT_CONTRIBUTION_HIGH_SCORE)
          .setCriticalScore(DEFAULT_SECURITY_EVENT_CONTRIBUTION_CRITICAL_SCORE)
          .build();
  private static final SecurityEventScoreContribution SECURITY_EVENT_SCORE_CONTRIBUTION_1 =
      SecurityEventScoreContribution.newBuilder()
          .setLowScore(10)
          .setMediumScore(20)
          .setHighScore(30)
          .setCriticalScore(100)
          .build();
  private static final SecurityEventScoreContribution SECURITY_EVENT_SCORE_CONTRIBUTION_2 =
      SecurityEventScoreContribution.newBuilder()
          .setLowScore(100)
          .setMediumScore(200)
          .setHighScore(300)
          .build();

  private static final AnomalyScoreContribution DEFAULT_ANOMALY_SCORE_CONTRIBUTION =
      AnomalyScoreContribution.newBuilder()
          .setAnomalyScore(DEFAULT_ANOMALY_CONTRIBUTION_SCORE)
          .build();
  private static final AnomalyScoreContribution ANOMALY_SCORE_CONTRIBUTION_1 =
      AnomalyScoreContribution.newBuilder().setAnomalyScore(10).build();
  private static final AnomalyScoreContribution ANOMALY_SCORE_CONTRIBUTION_2 =
      AnomalyScoreContribution.newBuilder().setAnomalyScore(100).build();

  private static final SecurityEventTypeContribution SECURITY_EVENT_TYPE_CONTRIBUTION_1 =
      SecurityEventTypeContribution.newBuilder()
          .setSecurityEventTypeContributionKind(
              SecurityEventTypeContributionKind.SECURITY_EVENT_TYPE_CONTRIBUTION_KIND_ALL)
          .build();
  private static final SecurityEventTypeContribution SECURITY_EVENT_TYPE_CONTRIBUTION_2 =
      SecurityEventTypeContribution.newBuilder()
          .setSecurityEventTypeContributionKind(
              SecurityEventTypeContributionKind
                  .SECURITY_EVENT_TYPE_CONTRIBUTION_KIND_HIGH_RISK_APIS)
          .build();

  private static final ThreatAutoBlockingActionConfig THREAT_AUTO_BLOCKING_ACTION_CONFIG_1 =
      ThreatAutoBlockingActionConfig.newBuilder()
          .setActionType(ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK)
          .build();
  private static final ThreatAutoBlockingActionConfig THREAT_AUTO_BLOCKING_ACTION_CONFIG_2 =
      ThreatAutoBlockingActionConfig.newBuilder()
          .setActionType(ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK)
          .setExpirationDetails(
              ExpirationDetails.newBuilder().setDuration("PT1H2M34S").setTimestampMillis(3754000))
          .build();

  private static final IpReputationThreatScoreConfig DEFAULT_IP_REPUTATION_THREAT_SCORE_CONFIG =
      IpReputationThreatScoreConfig.newBuilder()
          .setCriticalIpReputationThreatScoreIncrement(3)
          .setHighIpReputationThreatScoreIncrement(2)
          .setMediumIpReputationThreatScoreIncrement(1)
          .build();
  private static final IpReputationThreatScoreConfig IP_REPUTATION_THREAT_SCORE_CONFIG_1 =
      IpReputationThreatScoreConfig.newBuilder()
          .setCriticalIpReputationThreatScoreIncrement(4)
          .setHighIpReputationThreatScoreIncrement(3)
          .setMediumIpReputationThreatScoreIncrement(2)
          .setLowIpReputationThreatScoreIncrement(1)
          .build();

  private static final StatusCodeThreatScoreConfigs STATUS_CODE_THREAT_SCORE_CONFIGS =
      StatusCodeThreatScoreConfigs.newBuilder()
          .addConfigs(
              StatusCodeThreatScoreConfig.newBuilder()
                  .setProtocol(Protocol.PROTOCOL_HTTP)
                  .setErrorStatusCodeRegex("400")
                  .setSeverityDowngradePolicy(
                      SeverityDowngradePolicy.SEVERITY_DOWNGRADE_POLICY_IGNORE_SCORE))
          .build();

  @Mock private ThreatManagementConfigRequestValidator requestValidator;
  @Mock private ThreatScoreManager threatScoreManager;
  @Mock private SecurityEventScoreContributionManager securityEventScoreContributionManager;
  @Mock private AnomalyScoreContributionManager anomalyScoreContributionManager;
  @Mock private SecurityEventTypeContributionManager securityEventTypeContributionManager;
  @Mock private ThreatAutoBlockingManager threatAutoBlockingManager;
  @Mock private IpReputationThreatScoreConfigManager ipReputationThreatScoreConfigManager;
  @Mock private StatusCodeThreatScoreConfigsManager statusCodeThreatScoreConfigsManager;
  @Mock private ThreatScoreDecayManager threatScoreDecayManager;

  private ThreatManagementConfigServiceImpl threatManagementConfigService;
  private ThreatManagementConfigServiceConfig mockConfig;

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

    this.anomalyScoreContributionManager = mock(AnomalyScoreContributionManager.class);
    when(this.anomalyScoreContributionManager.getDefaultAnomalyScoreContribution())
        .thenReturn(DEFAULT_ANOMALY_SCORE_CONTRIBUTION);

    this.securityEventTypeContributionManager = mock(SecurityEventTypeContributionManager.class);

    this.threatAutoBlockingManager = mock(ThreatAutoBlockingManager.class);

    this.ipReputationThreatScoreConfigManager = mock(IpReputationThreatScoreConfigManager.class);
    when(this.ipReputationThreatScoreConfigManager.getDefaultIpReputationThreatScoreConfig())
        .thenReturn(DEFAULT_IP_REPUTATION_THREAT_SCORE_CONFIG);

    this.statusCodeThreatScoreConfigsManager = mock(StatusCodeThreatScoreConfigsManager.class);

    this.mockConfig = mock(ThreatManagementConfigServiceConfig.class);
    this.threatManagementConfigService =
        new ThreatManagementConfigServiceImpl(
            requestValidator,
            threatScoreManager,
            securityEventScoreContributionManager,
            anomalyScoreContributionManager,
            securityEventTypeContributionManager,
            threatAutoBlockingManager,
            ipReputationThreatScoreConfigManager,
            statusCodeThreatScoreConfigsManager,
            threatScoreDecayManager);
  }

  @Nested
  class ThreatScoreBounds {
    @Test
    void getThreatScoreBound() {
      when(threatScoreManager.getThreatScoreBound(
              any(RequestContext.class), any(ScopeConfig.class)))
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
    void getSecurityEventScoreContribution() {
      when(securityEventScoreContributionManager.getSecurityEventScoreContribution(
              any(RequestContext.class), any(ScopeConfig.class)))
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

  @Nested
  class AnomalyScoreContributions {
    @Test
    void getAnomalyScoreContribution() {
      when(anomalyScoreContributionManager.getAnomalyScoreContribution(
              any(RequestContext.class), any(ScopeConfig.class)))
          .thenReturn(ANOMALY_SCORE_CONTRIBUTION_1);

      StreamObserver<GetAnomalyScoreContributionResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.getAnomalyScoreContribution(
                  GetAnomalyScoreContributionRequest.getDefaultInstance(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(
              GetAnomalyScoreContributionResponse.newBuilder()
                  .setAnomalyScoreContribution(ANOMALY_SCORE_CONTRIBUTION_1)
                  .setDefaultAnomalyScoreContribution(DEFAULT_ANOMALY_SCORE_CONTRIBUTION)
                  .build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    void updateAnomalyScoreContribution() {
      when(anomalyScoreContributionManager.upsertAnomalyScoreContribution(
              any(RequestContext.class), eq(ANOMALY_SCORE_CONTRIBUTION_1)))
          .thenReturn(ANOMALY_SCORE_CONTRIBUTION_2);

      StreamObserver<UpdateAnomalyScoreContributionResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.updateAnomalyScoreContribution(
                  UpdateAnomalyScoreContributionRequest.newBuilder()
                      .setAnomalyScoreContribution(ANOMALY_SCORE_CONTRIBUTION_1)
                      .build(),
                  responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(
              UpdateAnomalyScoreContributionResponse.newBuilder()
                  .setAnomalyScoreContribution(ANOMALY_SCORE_CONTRIBUTION_2)
                  .setDefaultAnomalyScoreContribution(DEFAULT_ANOMALY_SCORE_CONTRIBUTION)
                  .build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    void skipUpdateAnomalyScoreContribution() {
      when(anomalyScoreContributionManager.upsertAnomalyScoreContribution(
              any(RequestContext.class), eq(ANOMALY_SCORE_CONTRIBUTION_1)))
          .thenThrow(IllegalArgumentException.class);

      StreamObserver<UpdateAnomalyScoreContributionResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.updateAnomalyScoreContribution(
                  UpdateAnomalyScoreContributionRequest.newBuilder()
                      .setAnomalyScoreContribution(ANOMALY_SCORE_CONTRIBUTION_1)
                      .build(),
                  responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.UNKNOWN));
    }
  }

  @Nested
  class SecurityEventTypeContributions {
    @Test
    void getSecurityEventTypeContribution() {
      when(securityEventTypeContributionManager.getSecurityEventTypeContribution(
              any(RequestContext.class), any(ScopeConfig.class)))
          .thenReturn(SECURITY_EVENT_TYPE_CONTRIBUTION_1);
      when(securityEventTypeContributionManager.getDefaultSecurityEventTypeContribution())
          .thenReturn(SECURITY_EVENT_TYPE_CONTRIBUTION_1);

      StreamObserver<GetSecurityEventTypeContributionResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.getSecurityEventTypeContribution(
                  GetSecurityEventTypeContributionRequest.getDefaultInstance(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(
              GetSecurityEventTypeContributionResponse.newBuilder()
                  .setSecurityEventTypeContribution(SECURITY_EVENT_TYPE_CONTRIBUTION_1)
                  .setDefaultEventTypeContribution(SECURITY_EVENT_TYPE_CONTRIBUTION_1)
                  .build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    void updateSecurityEventTypeContribution() {
      when(securityEventTypeContributionManager.upsertSecurityEventTypeContribution(
              any(RequestContext.class), eq(SECURITY_EVENT_TYPE_CONTRIBUTION_1)))
          .thenReturn(SECURITY_EVENT_TYPE_CONTRIBUTION_2);
      when(securityEventTypeContributionManager.getDefaultSecurityEventTypeContribution())
          .thenReturn(SECURITY_EVENT_TYPE_CONTRIBUTION_2);

      StreamObserver<UpdateSecurityEventTypeContributionResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.updateSecurityEventTypeContribution(
                  UpdateSecurityEventTypeContributionRequest.newBuilder()
                      .setSecurityEventTypeContribution(SECURITY_EVENT_TYPE_CONTRIBUTION_1)
                      .build(),
                  responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(
              UpdateSecurityEventTypeContributionResponse.newBuilder()
                  .setSecurityEventTypeContribution(SECURITY_EVENT_TYPE_CONTRIBUTION_2)
                  .setDefaultEventTypeContribution(SECURITY_EVENT_TYPE_CONTRIBUTION_2)
                  .build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    void skipUpdateSecurityEventTypeContribution() {
      when(securityEventTypeContributionManager.upsertSecurityEventTypeContribution(
              any(RequestContext.class), eq(SECURITY_EVENT_TYPE_CONTRIBUTION_1)))
          .thenThrow(IllegalArgumentException.class);

      StreamObserver<UpdateSecurityEventTypeContributionResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.updateSecurityEventTypeContribution(
                  UpdateSecurityEventTypeContributionRequest.newBuilder()
                      .setSecurityEventTypeContribution(SECURITY_EVENT_TYPE_CONTRIBUTION_1)
                      .build(),
                  responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.UNKNOWN));
    }
  }

  @Nested
  class ThreatAutoBlocking {
    @Test
    void getThreatAutoBlockingActionConfig() {
      when(threatAutoBlockingManager.getThreatAutoBlockingAction(
              any(RequestContext.class), any(ScopeConfig.class)))
          .thenReturn(THREAT_AUTO_BLOCKING_ACTION_CONFIG_1);

      StreamObserver<GetThreatAutoBlockingConfigResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.getThreatAutoBlockingConfig(
                  GetThreatAutoBlockingConfigRequest.getDefaultInstance(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(
              GetThreatAutoBlockingConfigResponse.newBuilder()
                  .setAutoBlockingActionConfig(THREAT_AUTO_BLOCKING_ACTION_CONFIG_1)
                  .build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    void updateThreatAutoBlockingActionConfig() {
      UpdateThreatAutoBlockingConfigRequest request =
          UpdateThreatAutoBlockingConfigRequest.newBuilder()
              .setActionType(ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK)
              .setExpirationDetails(
                  UpdateThreatAutoBlockingConfigRequest.ExpirationDetails.newBuilder()
                      .setDuration("PT1H2M34S")
                      .build())
              .build();
      when(threatAutoBlockingManager.upsertThreatAutoBlockingAction(
              any(RequestContext.class), eq(request)))
          .thenReturn(THREAT_AUTO_BLOCKING_ACTION_CONFIG_2);

      StreamObserver<UpdateThreatAutoBlockingConfigResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.updateThreatAutoBlockingConfig(
                  request, responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(
              UpdateThreatAutoBlockingConfigResponse.newBuilder()
                  .setAutoBlockingActionConfig(THREAT_AUTO_BLOCKING_ACTION_CONFIG_2)
                  .build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    void skipUpdateThreatAutoBlockingActionConfig() {
      UpdateThreatAutoBlockingConfigRequest request =
          UpdateThreatAutoBlockingConfigRequest.newBuilder()
              .setActionType(ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK)
              .build();
      when(threatAutoBlockingManager.upsertThreatAutoBlockingAction(
              any(RequestContext.class), eq(request)))
          .thenThrow(IllegalArgumentException.class);

      StreamObserver<UpdateThreatAutoBlockingConfigResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.updateThreatAutoBlockingConfig(
                  request, responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.UNKNOWN));
    }
  }

  @Nested
  class IpReputationThreatScoreConfigTest {
    @Test
    void getIpReputationThreatScoreConfig() {
      when(ipReputationThreatScoreConfigManager.getIpReputationThreatScoreConfig(
              any(RequestContext.class), any(ScopeConfig.class)))
          .thenReturn(IP_REPUTATION_THREAT_SCORE_CONFIG_1);

      StreamObserver<GetIpReputationThreatScoreConfigResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.getIpReputationThreatScoreConfig(
                  GetIpReputationThreatScoreConfigRequest.getDefaultInstance(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(
              GetIpReputationThreatScoreConfigResponse.newBuilder()
                  .setIpReputationThreatScoreConfig(IP_REPUTATION_THREAT_SCORE_CONFIG_1)
                  .setDefaultIpReputationThreatScoreConfig(
                      DEFAULT_IP_REPUTATION_THREAT_SCORE_CONFIG)
                  .build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    void updateIpReputationThreatScoreConfig() {
      when(ipReputationThreatScoreConfigManager.updateIpReputationThreatScoreConfig(
              any(RequestContext.class), eq(IP_REPUTATION_THREAT_SCORE_CONFIG_1)))
          .thenReturn(IP_REPUTATION_THREAT_SCORE_CONFIG_1);

      StreamObserver<UpdateIpReputationThreatScoreConfigResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.updateIpReputationThreatScoreConfig(
                  UpdateIpReputationThreatScoreConfigRequest.newBuilder()
                      .setIpReputationThreatScoreConfig(IP_REPUTATION_THREAT_SCORE_CONFIG_1)
                      .build(),
                  responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(
              UpdateIpReputationThreatScoreConfigResponse.newBuilder()
                  .setIpReputationThreatScoreConfig(IP_REPUTATION_THREAT_SCORE_CONFIG_1)
                  .setDefaultIpReputationThreatScoreConfig(
                      DEFAULT_IP_REPUTATION_THREAT_SCORE_CONFIG)
                  .build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    void skipUpdateIpReputationThreatScoreConfig() {
      when(ipReputationThreatScoreConfigManager.updateIpReputationThreatScoreConfig(
              any(RequestContext.class), eq(IP_REPUTATION_THREAT_SCORE_CONFIG_1)))
          .thenThrow(IllegalArgumentException.class);

      StreamObserver<UpdateIpReputationThreatScoreConfigResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.updateIpReputationThreatScoreConfig(
                  UpdateIpReputationThreatScoreConfigRequest.newBuilder()
                      .setIpReputationThreatScoreConfig(IP_REPUTATION_THREAT_SCORE_CONFIG_1)
                      .build(),
                  responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.UNKNOWN));
    }
  }

  @Nested
  class StatusCodeThreatScoreConfigsTest {
    @Test
    void getStatusCodeThreatScoreConfigs() {
      when(statusCodeThreatScoreConfigsManager.getStatusCodeThreatScoreConfigs(
              any(RequestContext.class), any(ScopeConfig.class)))
          .thenReturn(STATUS_CODE_THREAT_SCORE_CONFIGS);

      StreamObserver<GetStatusCodeThreatScoreConfigsResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.getStatusCodeThreatScoreConfigs(
                  GetStatusCodeThreatScoreConfigsRequest.getDefaultInstance(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(
              GetStatusCodeThreatScoreConfigsResponse.newBuilder()
                  .setStatusCodeThreatScoreConfigs(STATUS_CODE_THREAT_SCORE_CONFIGS)
                  .build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    void updateStatusCodeThreatScoreConfigs() {
      when(statusCodeThreatScoreConfigsManager.updateStatusCodeThreatScoreConfigs(
              any(RequestContext.class), eq(STATUS_CODE_THREAT_SCORE_CONFIGS)))
          .thenReturn(STATUS_CODE_THREAT_SCORE_CONFIGS);

      StreamObserver<UpdateStatusCodeThreatScoreConfigsResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.updateStatusCodeThreatScoreConfigs(
                  UpdateStatusCodeThreatScoreConfigsRequest.newBuilder()
                      .setStatusCodeThreatScoreConfigs(STATUS_CODE_THREAT_SCORE_CONFIGS)
                      .build(),
                  responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(
              UpdateStatusCodeThreatScoreConfigsResponse.newBuilder()
                  .setStatusCodeThreatScoreConfigs(STATUS_CODE_THREAT_SCORE_CONFIGS)
                  .build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    void skipUpdateStatusCodeThreatScoreConfigs() {
      when(statusCodeThreatScoreConfigsManager.updateStatusCodeThreatScoreConfigs(
              any(RequestContext.class), eq(STATUS_CODE_THREAT_SCORE_CONFIGS)))
          .thenThrow(IllegalArgumentException.class);

      StreamObserver<UpdateStatusCodeThreatScoreConfigsResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.updateStatusCodeThreatScoreConfigs(
                  UpdateStatusCodeThreatScoreConfigsRequest.newBuilder()
                      .setStatusCodeThreatScoreConfigs(STATUS_CODE_THREAT_SCORE_CONFIGS)
                      .build(),
                  responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.UNKNOWN));
    }
  }

  @Nested
  class ThreatScoreConfigTest {
    @Test
    void getThreatScoreConfig() {
      when(anomalyScoreContributionManager.getAnomalyScoreContribution(
              any(RequestContext.class), any(ScopeConfig.class)))
          .thenReturn(ANOMALY_SCORE_CONTRIBUTION_1);
      when(securityEventScoreContributionManager.getSecurityEventScoreContribution(
              any(RequestContext.class), any(ScopeConfig.class)))
          .thenReturn(SECURITY_EVENT_SCORE_CONTRIBUTION_1);
      when(securityEventTypeContributionManager.getSecurityEventTypeContribution(
              any(RequestContext.class), any(ScopeConfig.class)))
          .thenReturn(SECURITY_EVENT_TYPE_CONTRIBUTION_1);
      when(ipReputationThreatScoreConfigManager.getIpReputationThreatScoreConfig(
              any(RequestContext.class), any(ScopeConfig.class)))
          .thenReturn(IP_REPUTATION_THREAT_SCORE_CONFIG_1);
      when(threatScoreManager.getThreatScoreBound(
              any(RequestContext.class), any(ScopeConfig.class)))
          .thenReturn(THREAT_SCORE_BOUND_1);
      when(statusCodeThreatScoreConfigsManager.getStatusCodeThreatScoreConfigs(
              any(RequestContext.class), any(ScopeConfig.class)))
          .thenReturn(STATUS_CODE_THREAT_SCORE_CONFIGS);

      StreamObserver<GetThreatScoreConfigResponse> responseObserver = mock(StreamObserver.class);

      Runnable runnable =
          () ->
              threatManagementConfigService.getThreatScoreConfig(
                  GetThreatScoreConfigRequest.getDefaultInstance(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(
              GetThreatScoreConfigResponse.newBuilder()
                  .setThreatScoreConfig(
                      ThreatScoreConfig.newBuilder()
                          .setAnomalyScoreContribution(ANOMALY_SCORE_CONTRIBUTION_1)
                          .setSecurityEventScoreContribution(SECURITY_EVENT_SCORE_CONTRIBUTION_1)
                          .setSecurityEventTypeContribution(SECURITY_EVENT_TYPE_CONTRIBUTION_1)
                          .setIpReputationThreatScoreConfig(IP_REPUTATION_THREAT_SCORE_CONFIG_1)
                          .setStatusCodeThreatScoreConfigs(STATUS_CODE_THREAT_SCORE_CONFIGS)
                          .setThreatScoreBound(THREAT_SCORE_BOUND_1))
                  .build());
      verify(responseObserver, times(1)).onCompleted();
    }
  }
}
