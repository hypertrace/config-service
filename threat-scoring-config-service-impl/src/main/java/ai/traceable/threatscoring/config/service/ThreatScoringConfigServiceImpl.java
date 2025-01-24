package ai.traceable.threatscoring.config.service;

import ai.traceable.threatscoring.config.service.v1.DeleteEventConfidenceScoringConfigOverridesRequest;
import ai.traceable.threatscoring.config.service.v1.DeleteEventConfidenceScoringConfigOverridesResponse;
import ai.traceable.threatscoring.config.service.v1.DeleteThreatActivityConfidenceScoringConfigOverridesRequest;
import ai.traceable.threatscoring.config.service.v1.DeleteThreatActivityConfidenceScoringConfigOverridesResponse;
import ai.traceable.threatscoring.config.service.v1.GetScopedThreatScoringConfigsRequest;
import ai.traceable.threatscoring.config.service.v1.GetScopedThreatScoringConfigsResponse;
import ai.traceable.threatscoring.config.service.v1.OverrideEventConfidenceScoringConfigRequest;
import ai.traceable.threatscoring.config.service.v1.OverrideEventConfidenceScoringConfigResponse;
import ai.traceable.threatscoring.config.service.v1.OverrideThreatActivityConfidenceScoringConfigRequest;
import ai.traceable.threatscoring.config.service.v1.OverrideThreatActivityConfidenceScoringConfigResponse;
import ai.traceable.threatscoring.config.service.v1.ScopedThreatScoringConfigs;
import ai.traceable.threatscoring.config.service.v1.ThreatScoringConfigServiceGrpc;
import ai.traceable.threatscoring.config.service.v1.ThreatScoringConfigs;
import ai.traceable.threatscoring.config.service.validation.ThreatScoringConfigRequestValidator;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
public class ThreatScoringConfigServiceImpl
    extends ThreatScoringConfigServiceGrpc.ThreatScoringConfigServiceImplBase {
  private EventConfidenceScoringConfigManager eventConfidenceScoringConfigManager;
  private ThreatActivityConfidenceScoringConfigManager threatActivityConfidenceScoringConfigManager;
  private ThreatScoringConfigRequestValidator validator;

  @Override
  public void getScopedThreatScoringConfigs(
      GetScopedThreatScoringConfigsRequest request,
      StreamObserver<GetScopedThreatScoringConfigsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validateGetOrDeleteRequest(requestContext);
      responseObserver.onNext(
          GetScopedThreatScoringConfigsResponse.newBuilder()
              .setScopedThreatScoringConfigs(fetchThreatScoringConfigs(request, requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);

      log.warn(
          "Error retrieving threat scoring configs for customer with request context {}",
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void overrideEventConfidenceScoringConfig(
      OverrideEventConfidenceScoringConfigRequest request,
      StreamObserver<OverrideEventConfidenceScoringConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validateOverrideEventConfidenceScoringConfigRequest(requestContext, request);
      responseObserver.onNext(
          OverrideEventConfidenceScoringConfigResponse.newBuilder()
              .setDefaultEventConfidenceScoringConfig(
                  eventConfidenceScoringConfigManager.getDefaultEventConfidenceScoringConfig())
              .setResolvedEventConfidenceScoringConfig(
                  eventConfidenceScoringConfigManager.overrideConfig(request, requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);

      log.warn(
          "Error overriding event confidence scoring configs for customer with request context {}",
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void deleteEventConfidenceScoringConfigOverrides(
      DeleteEventConfidenceScoringConfigOverridesRequest request,
      StreamObserver<DeleteEventConfidenceScoringConfigOverridesResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validateGetOrDeleteRequest(requestContext);
      eventConfidenceScoringConfigManager.deleteOverrides(request, requestContext);
      responseObserver.onNext(
          DeleteEventConfidenceScoringConfigOverridesResponse.newBuilder()
              .setDefaultEventConfidenceScoringConfig(
                  eventConfidenceScoringConfigManager.getDefaultEventConfidenceScoringConfig())
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);

      log.warn(
          "Error deleting event confidence scoring configs for customer with request context {}",
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void overrideThreatActivityConfidenceScoringConfig(
      OverrideThreatActivityConfidenceScoringConfigRequest request,
      StreamObserver<OverrideThreatActivityConfidenceScoringConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validateOverrideThreatActivityConfidenceScoringConfigRequest(
          requestContext, request);
      responseObserver.onNext(
          OverrideThreatActivityConfidenceScoringConfigResponse.newBuilder()
              .setDefaultThreatActivityConfidenceScoringConfig(
                  threatActivityConfidenceScoringConfigManager
                      .getDefaultThreatActivityConfidenceScoringConfig())
              .setResolvedThreatActivityConfidenceScoringConfig(
                  threatActivityConfidenceScoringConfigManager.overrideConfig(
                      request, requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);

      log.warn(
          "Error overriding threat activity confidence scoring configs for customer with request context {}",
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void deleteThreatActivityConfidenceScoringConfigOverrides(
      DeleteThreatActivityConfidenceScoringConfigOverridesRequest request,
      StreamObserver<DeleteThreatActivityConfidenceScoringConfigOverridesResponse>
          responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validateGetOrDeleteRequest(requestContext);
      threatActivityConfidenceScoringConfigManager.deleteOverrides(request, requestContext);
      responseObserver.onNext(
          DeleteThreatActivityConfidenceScoringConfigOverridesResponse.newBuilder()
              .setDefaultThreatActivityConfidenceScoringConfig(
                  threatActivityConfidenceScoringConfigManager
                      .getDefaultThreatActivityConfidenceScoringConfig())
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);

      log.warn(
          "Error deleting threat activity confidence scoring configs for customer with request context {}",
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  private ScopedThreatScoringConfigs fetchThreatScoringConfigs(
      GetScopedThreatScoringConfigsRequest request, RequestContext requestContext) {
    ThreatScoringConfigs.Builder builder = ThreatScoringConfigs.newBuilder();

    switch (request.getFilter().getConfigType()) {
      case THREAT_SCORING_CONFIG_TYPE_EVENT_CONFIDENCE_SCORING:
        builder.setEventConfidenceScoringConfig(
            eventConfidenceScoringConfigManager.getResolvedConfig(request, requestContext));
        break;
      case THREAT_SCORING_CONFIG_TYPE_THREAT_ACTIVITY_CONFIDENCE_SCORING:
        builder.setThreatActivityConfidenceScoringConfig(
            threatActivityConfidenceScoringConfigManager.getResolvedConfig(
                request, requestContext));
        break;
      case THREAT_SCORING_CONFIG_TYPE_UNSPECIFIED:
        builder
            .setThreatActivityConfidenceScoringConfig(
                threatActivityConfidenceScoringConfigManager.getResolvedConfig(
                    request, requestContext))
            .setEventConfidenceScoringConfig(
                eventConfidenceScoringConfigManager.getResolvedConfig(request, requestContext));
        break;
      default:
        log.error(
            "Invalid Threat scoring config type {} for tenant ID {}",
            request.getFilter().getConfigType(),
            requestContext.getTenantId());
    }
    return ScopedThreatScoringConfigs.newBuilder()
        .setConfigScope(request.getConfigScope())
        .setConfigs(builder)
        .build();
  }

  private Exception decorateException(RequestContext requestContext, Exception exception) {
    return Status.fromThrowable(exception)
        .withCause(exception)
        .asException(requestContext.buildTrailers());
  }
}
