package ai.traceable.threatmanagement.config.service;

import ai.traceable.threatmanagement.config.service.anomalyscore.AnomalyScoreContributionManager;
import ai.traceable.threatmanagement.config.service.eventscore.SecurityEventScoreContributionManager;
import ai.traceable.threatmanagement.config.service.eventtype.SecurityEventTypeContributionManager;
import ai.traceable.threatmanagement.config.service.ipreputation.IpReputationThreatScoreConfigManager;
import ai.traceable.threatmanagement.config.service.statuscode.StatusCodeThreatScoreConfigsManager;
import ai.traceable.threatmanagement.config.service.threatautoblocking.ThreatAutoBlockingManager;
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
import ai.traceable.threatmanagement.config.service.v1.SecurityEventScoreContribution;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventTypeContribution;
import ai.traceable.threatmanagement.config.service.v1.StatusCodeThreatScoreConfigs;
import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionConfig;
import ai.traceable.threatmanagement.config.service.v1.ThreatManagementConfigServiceGrpc.ThreatManagementConfigServiceImplBase;
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
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class ThreatManagementConfigServiceImpl extends ThreatManagementConfigServiceImplBase {
  private final ThreatManagementConfigRequestValidator requestValidator;
  private final ThreatScoreManager threatScoreManager;
  private final SecurityEventScoreContributionManager securityEventScoreContributionManager;
  private final AnomalyScoreContributionManager anomalyScoreContributionManager;
  private final SecurityEventTypeContributionManager securityEventTypeContributionManager;
  private final ThreatAutoBlockingManager threatAutoBlockingManager;
  private final IpReputationThreatScoreConfigManager ipReputationThreatScoreConfigManager;
  private final StatusCodeThreatScoreConfigsManager statusCodeThreatScoreConfigsManager;

  @Inject
  ThreatManagementConfigServiceImpl(
      ThreatManagementConfigRequestValidator requestValidator,
      ThreatScoreManager threatScoreManager,
      SecurityEventScoreContributionManager securityEventScoreContributionManager,
      AnomalyScoreContributionManager anomalyScoreContributionManager,
      SecurityEventTypeContributionManager securityEventTypeContributionManager,
      ThreatAutoBlockingManager threatAutoBlockingManager,
      IpReputationThreatScoreConfigManager ipReputationThreatScoreConfigManager,
      StatusCodeThreatScoreConfigsManager statusCodeThreatScoreConfigsManager) {
    this.requestValidator = requestValidator;
    this.threatScoreManager = threatScoreManager;
    this.securityEventScoreContributionManager = securityEventScoreContributionManager;
    this.anomalyScoreContributionManager = anomalyScoreContributionManager;
    this.securityEventTypeContributionManager = securityEventTypeContributionManager;
    this.threatAutoBlockingManager = threatAutoBlockingManager;
    this.ipReputationThreatScoreConfigManager = ipReputationThreatScoreConfigManager;
    this.statusCodeThreatScoreConfigsManager = statusCodeThreatScoreConfigsManager;
  }

  @Override
  public void getThreatScoreBound(
      GetThreatScoreBoundRequest request,
      StreamObserver<GetThreatScoreBoundResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      ThreatScoreBound threatScoreBound =
          threatScoreManager.getThreatScoreBound(requestContext, request.getScope());
      ThreatScoreBound defaultThreatScoreBound = threatScoreManager.getDefaultThreatScoreBound();

      responseObserver.onNext(
          GetThreatScoreBoundResponse.newBuilder()
              .setThreatScoreBound(threatScoreBound)
              .setDefaultThreatScoreBound(defaultThreatScoreBound)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get threat score bound for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateThreatScoreBound(
      UpdateThreatScoreBoundRequest request,
      StreamObserver<UpdateThreatScoreBoundResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      ThreatScoreBound threatScoreBound =
          threatScoreManager.upsertThreatScoreBound(requestContext, request.getThreatScoreBound());
      ThreatScoreBound defaultThreatScoreBound = threatScoreManager.getDefaultThreatScoreBound();

      responseObserver.onNext(
          UpdateThreatScoreBoundResponse.newBuilder()
              .setThreatScoreBound(threatScoreBound)
              .setDefaultThreatScoreBound(defaultThreatScoreBound)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to update threat score bound for request {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getSecurityEventScoreContribution(
      GetSecurityEventScoreContributionRequest request,
      StreamObserver<GetSecurityEventScoreContributionResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      SecurityEventScoreContribution securityEventScoreContribution =
          securityEventScoreContributionManager.getSecurityEventScoreContribution(
              requestContext, request.getScope());
      SecurityEventScoreContribution defaultSecurityEventScoreContribution =
          securityEventScoreContributionManager.getDefaultSecurityEventScoreContribution();

      responseObserver.onNext(
          GetSecurityEventScoreContributionResponse.newBuilder()
              .setSecurityEventScoreContribution(securityEventScoreContribution)
              .setDefaultSecurityEventScoreContribution(defaultSecurityEventScoreContribution)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get security event contribution for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateSecurityEventScoreContribution(
      UpdateSecurityEventScoreContributionRequest request,
      StreamObserver<UpdateSecurityEventScoreContributionResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      SecurityEventScoreContribution securityEventScoreContribution =
          securityEventScoreContributionManager.upsertSecurityEventScoreContribution(
              requestContext, request.getSecurityEventScoreContribution());
      SecurityEventScoreContribution defaultSecurityEventScoreContribution =
          securityEventScoreContributionManager.getDefaultSecurityEventScoreContribution();

      responseObserver.onNext(
          UpdateSecurityEventScoreContributionResponse.newBuilder()
              .setSecurityEventScoreContribution(securityEventScoreContribution)
              .setDefaultSecurityEventScoreContribution(defaultSecurityEventScoreContribution)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to update security event score contribution for request {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAnomalyScoreContribution(
      GetAnomalyScoreContributionRequest request,
      StreamObserver<GetAnomalyScoreContributionResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      AnomalyScoreContribution anomalyScoreContribution =
          anomalyScoreContributionManager.getAnomalyScoreContribution(
              requestContext, request.getScope());
      AnomalyScoreContribution defaultAnomalyScoreContribution =
          anomalyScoreContributionManager.getDefaultAnomalyScoreContribution();

      responseObserver.onNext(
          GetAnomalyScoreContributionResponse.newBuilder()
              .setAnomalyScoreContribution(anomalyScoreContribution)
              .setDefaultAnomalyScoreContribution(defaultAnomalyScoreContribution)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get anomaly contribution for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateAnomalyScoreContribution(
      UpdateAnomalyScoreContributionRequest request,
      StreamObserver<UpdateAnomalyScoreContributionResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      AnomalyScoreContribution anomalyScoreContribution =
          anomalyScoreContributionManager.upsertAnomalyScoreContribution(
              requestContext, request.getAnomalyScoreContribution());
      AnomalyScoreContribution defaultAnomalyScoreContribution =
          anomalyScoreContributionManager.getDefaultAnomalyScoreContribution();

      responseObserver.onNext(
          UpdateAnomalyScoreContributionResponse.newBuilder()
              .setAnomalyScoreContribution(anomalyScoreContribution)
              .setDefaultAnomalyScoreContribution(defaultAnomalyScoreContribution)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to update anomaly score contribution for request {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getSecurityEventTypeContribution(
      GetSecurityEventTypeContributionRequest request,
      StreamObserver<GetSecurityEventTypeContributionResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      SecurityEventTypeContribution securityEventTypeContribution =
          securityEventTypeContributionManager.getSecurityEventTypeContribution(
              requestContext, request.getScope());

      responseObserver.onNext(
          GetSecurityEventTypeContributionResponse.newBuilder()
              .setSecurityEventTypeContribution(securityEventTypeContribution)
              .setDefaultEventTypeContribution(
                  securityEventTypeContributionManager.getDefaultSecurityEventTypeContribution())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get security type contribution for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateSecurityEventTypeContribution(
      UpdateSecurityEventTypeContributionRequest request,
      StreamObserver<UpdateSecurityEventTypeContributionResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      SecurityEventTypeContribution securityEventTypeContribution =
          securityEventTypeContributionManager.upsertSecurityEventTypeContribution(
              requestContext, request.getSecurityEventTypeContribution());

      responseObserver.onNext(
          UpdateSecurityEventTypeContributionResponse.newBuilder()
              .setSecurityEventTypeContribution(securityEventTypeContribution)
              .setDefaultEventTypeContribution(
                  securityEventTypeContributionManager.getDefaultSecurityEventTypeContribution())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to update security event type contribution for request {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getThreatAutoBlockingConfig(
      GetThreatAutoBlockingConfigRequest request,
      StreamObserver<GetThreatAutoBlockingConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      ThreatAutoBlockingActionConfig threatAutoBlockingActionConfig =
          threatAutoBlockingManager.getThreatAutoBlockingAction(requestContext, request.getScope());

      responseObserver.onNext(
          GetThreatAutoBlockingConfigResponse.newBuilder()
              .setAutoBlockingActionConfig(threatAutoBlockingActionConfig)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get threat auto blocking action config for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateThreatAutoBlockingConfig(
      UpdateThreatAutoBlockingConfigRequest request,
      StreamObserver<UpdateThreatAutoBlockingConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      ThreatAutoBlockingActionConfig threatAutoBlockingActionConfig =
          threatAutoBlockingManager.upsertThreatAutoBlockingAction(requestContext, request);

      responseObserver.onNext(
          UpdateThreatAutoBlockingConfigResponse.newBuilder()
              .setAutoBlockingActionConfig(threatAutoBlockingActionConfig)
              .build());
      responseObserver.onCompleted();

    } catch (Exception e) {
      log.error("Unable to update threat auto blocking action config for request {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getIpReputationThreatScoreConfig(
      GetIpReputationThreatScoreConfigRequest request,
      StreamObserver<GetIpReputationThreatScoreConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateRequestContext(requestContext);

      IpReputationThreatScoreConfig ipReputationThreatScoreConfig =
          ipReputationThreatScoreConfigManager.getIpReputationThreatScoreConfig(
              requestContext, request.getScope());

      responseObserver.onNext(
          GetIpReputationThreatScoreConfigResponse.newBuilder()
              .setIpReputationThreatScoreConfig(ipReputationThreatScoreConfig)
              .setDefaultIpReputationThreatScoreConfig(
                  ipReputationThreatScoreConfigManager.getDefaultIpReputationThreatScoreConfig())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get ip reputation threat score config for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateIpReputationThreatScoreConfig(
      UpdateIpReputationThreatScoreConfigRequest request,
      StreamObserver<UpdateIpReputationThreatScoreConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      IpReputationThreatScoreConfig ipReputationThreatScoreConfig =
          ipReputationThreatScoreConfigManager.updateIpReputationThreatScoreConfig(
              requestContext, request.getIpReputationThreatScoreConfig());

      responseObserver.onNext(
          UpdateIpReputationThreatScoreConfigResponse.newBuilder()
              .setIpReputationThreatScoreConfig(ipReputationThreatScoreConfig)
              .setDefaultIpReputationThreatScoreConfig(
                  ipReputationThreatScoreConfigManager.getDefaultIpReputationThreatScoreConfig())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to update ip reputation threat score config for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getStatusCodeThreatScoreConfigs(
      GetStatusCodeThreatScoreConfigsRequest request,
      StreamObserver<GetStatusCodeThreatScoreConfigsResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateRequestContext(requestContext);

      StatusCodeThreatScoreConfigs statusCodeThreatScoreConfigs =
          statusCodeThreatScoreConfigsManager.getStatusCodeThreatScoreConfigs(
              requestContext, request.getScope());

      responseObserver.onNext(
          GetStatusCodeThreatScoreConfigsResponse.newBuilder()
              .setStatusCodeThreatScoreConfigs(statusCodeThreatScoreConfigs)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get status code threat score configs for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateStatusCodeThreatScoreConfigs(
      UpdateStatusCodeThreatScoreConfigsRequest request,
      StreamObserver<UpdateStatusCodeThreatScoreConfigsResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      StatusCodeThreatScoreConfigs statusCodeThreatScoreConfigs =
          statusCodeThreatScoreConfigsManager.updateStatusCodeThreatScoreConfigs(
              requestContext, request.getStatusCodeThreatScoreConfigs());

      responseObserver.onNext(
          UpdateStatusCodeThreatScoreConfigsResponse.newBuilder()
              .setStatusCodeThreatScoreConfigs(statusCodeThreatScoreConfigs)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to update status code threat score configs for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getThreatScoreConfig(
      GetThreatScoreConfigRequest request,
      StreamObserver<GetThreatScoreConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateRequestContext(requestContext);

      responseObserver.onNext(
          GetThreatScoreConfigResponse.newBuilder()
              .setThreatScoreConfig(
                  ThreatScoreConfig.newBuilder()
                      .setAnomalyScoreContribution(
                          anomalyScoreContributionManager.getAnomalyScoreContribution(
                              requestContext, request.getScope()))
                      .setSecurityEventScoreContribution(
                          securityEventScoreContributionManager.getSecurityEventScoreContribution(
                              requestContext, request.getScope()))
                      .setSecurityEventTypeContribution(
                          securityEventTypeContributionManager.getSecurityEventTypeContribution(
                              requestContext, request.getScope()))
                      .setIpReputationThreatScoreConfig(
                          ipReputationThreatScoreConfigManager.getIpReputationThreatScoreConfig(
                              requestContext, request.getScope()))
                      .setStatusCodeThreatScoreConfigs(
                          statusCodeThreatScoreConfigsManager.getStatusCodeThreatScoreConfigs(
                              requestContext, request.getScope()))
                      .setThreatScoreBound(
                          threatScoreManager.getThreatScoreBound(
                              requestContext, request.getScope()))
                      .build())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get threat score config for request: {}", request, e);
      responseObserver.onError(e);
    }
  }
}
