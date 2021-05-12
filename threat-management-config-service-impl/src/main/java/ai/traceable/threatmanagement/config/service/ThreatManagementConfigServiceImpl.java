package ai.traceable.threatmanagement.config.service;

import ai.traceable.threatmanagement.config.service.threatscore.ThreatScoreManager;
import ai.traceable.threatmanagement.config.service.v1.GetThreatScoreBoundRequest;
import ai.traceable.threatmanagement.config.service.v1.GetThreatScoreBoundResponse;
import ai.traceable.threatmanagement.config.service.v1.ThreatManagementConfigServiceGrpc.ThreatManagementConfigServiceImplBase;
import ai.traceable.threatmanagement.config.service.v1.ThreatScoreBound;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatScoreBoundRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatScoreBoundResponse;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class ThreatManagementConfigServiceImpl extends ThreatManagementConfigServiceImplBase {
  private final ThreatManagementConfigServiceConfig config;
  private final ThreatManagementConfigRequestValidator requestValidator;
  private final ThreatScoreManager threatScoreManager;

  @Inject
  ThreatManagementConfigServiceImpl(
      ThreatManagementConfigServiceConfig config,
      ThreatManagementConfigRequestValidator requestValidator,
      ThreatScoreManager threatScoreManager) {
    this.config = config;
    this.requestValidator = requestValidator;
    this.threatScoreManager = threatScoreManager;
  }

  @Override
  public void getThreatScoreBound(
      GetThreatScoreBoundRequest request,
      StreamObserver<GetThreatScoreBoundResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      ThreatScoreBound threatScoreBound = threatScoreManager.getThreatScoreBound(requestContext);
      responseObserver.onNext(
          GetThreatScoreBoundResponse.newBuilder()
              .setThreatScoreBound(threatScoreBound)
              .setDefaultThreatScoreBound(buildDefaultThreatScoreBound())
              .build());
      responseObserver.onCompleted();
    } catch (RuntimeException e) {
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
      responseObserver.onNext(
          UpdateThreatScoreBoundResponse.newBuilder()
              .setThreatScoreBound(threatScoreBound)
              .setDefaultThreatScoreBound(buildDefaultThreatScoreBound())
              .build());
      responseObserver.onCompleted();
    } catch (RuntimeException e) {
      log.error("Unable to update threat score bound for request {}", request, e);
      responseObserver.onError(e);
    }
  }

  private ThreatScoreBound buildDefaultThreatScoreBound() {
    return ThreatScoreBound.newBuilder()
        .setMediumScoreUpperBound(config.getDefaultThreatUpperBoundMediumScore())
        .setHighScoreUpperBound(config.getDefaultThreatUpperBoundHighScore())
        .build();
  }
}
