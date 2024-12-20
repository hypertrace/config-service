package ai.traceable.runner.logs.config.service;

import ai.traceable.runner.logs.config.service.v1.GetRunnerLogsPersistenceConfigRequest;
import ai.traceable.runner.logs.config.service.v1.GetRunnerLogsPersistenceConfigResponse;
import ai.traceable.runner.logs.config.service.v1.GetRunnerLogsPurgeConfigRequest;
import ai.traceable.runner.logs.config.service.v1.GetRunnerLogsPurgeConfigResponse;
import ai.traceable.runner.logs.config.service.v1.RunnerLogsConfigServiceGrpc;
import ai.traceable.runner.logs.config.service.v1.UpdateRunnerLogsPersistenceConfigRequest;
import ai.traceable.runner.logs.config.service.v1.UpdateRunnerLogsPersistenceConfigResponse;
import ai.traceable.runner.logs.config.service.v1.UpdateRunnerLogsPurgeConfigRequest;
import ai.traceable.runner.logs.config.service.v1.UpdateRunnerLogsPurgeConfigResponse;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
class RunnerLogsConfigServiceImpl
    extends RunnerLogsConfigServiceGrpc.RunnerLogsConfigServiceImplBase {

  private final RunnerLogsConfigManager runnerLogsConfigManager;
  private final RunnerLogsConfigServiceRequestValidator requestValidator;

  @Override
  public void getRunnerLogsPurgeConfig(
      final GetRunnerLogsPurgeConfigRequest request,
      final StreamObserver<GetRunnerLogsPurgeConfigResponse> responseObserver) {
    final RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          GetRunnerLogsPurgeConfigResponse.newBuilder()
              .setRunnerLogsPurgeConfig(
                  runnerLogsConfigManager.getRunnerLogsPurgeConfig(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.warn(
          String.format(
              "Unable to fetch runner logs purge config for request: [%s] within context: [%s]",
              request, requestContext),
          e);
      responseObserver.onError(decorateException(requestContext, e));
    }
  }

  @Override
  public void updateRunnerLogsPurgeConfig(
      final UpdateRunnerLogsPurgeConfigRequest request,
      final StreamObserver<UpdateRunnerLogsPurgeConfigResponse> responseObserver) {
    final RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          UpdateRunnerLogsPurgeConfigResponse.newBuilder()
              .setRunnerLogsPurgeConfig(
                  runnerLogsConfigManager.updateAndGetRunnerLogsPurgeConfig(
                      requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.warn(
          String.format(
              "Unable to update runner logs purge config for request: [%s] within context: [%s]",
              request, requestContext),
          e);
      responseObserver.onError(decorateException(requestContext, e));
    }
  }

  @Override
  public void getRunnerLogsPersistenceConfig(
      final GetRunnerLogsPersistenceConfigRequest request,
      final StreamObserver<GetRunnerLogsPersistenceConfigResponse> responseObserver) {
    final RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          GetRunnerLogsPersistenceConfigResponse.newBuilder()
              .setRunnerLogsPersistenceConfig(
                  runnerLogsConfigManager.getRunnerLogsPersistenceConfig(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.warn(
          String.format(
              "Unable to fetch runner logs persistence config for request: [%s] within context: [%s]",
              request, requestContext),
          e);
      responseObserver.onError(decorateException(requestContext, e));
    }
  }

  @Override
  public void updateRunnerLogsPersistenceConfig(
      final UpdateRunnerLogsPersistenceConfigRequest request,
      final StreamObserver<UpdateRunnerLogsPersistenceConfigResponse> responseObserver) {
    final RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          UpdateRunnerLogsPersistenceConfigResponse.newBuilder()
              .setRunnerLogsPersistenceConfig(
                  runnerLogsConfigManager.updateAndGetRunnerLogsPersistenceConfig(
                      requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.warn(
          String.format(
              "Unable to update runner logs persistence config for request: [%s] within context: [%s]",
              request, requestContext),
          e);
      responseObserver.onError(decorateException(requestContext, e));
    }
  }

  private StatusException decorateException(
      final RequestContext requestContext, final Exception exception) {
    return Status.fromThrowable(exception).asException(requestContext.buildTrailers());
  }
}
