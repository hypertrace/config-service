package ai.traceable.anomaly.config.service.global;

import ai.traceable.anomaly.config.service.global.status.AnomalyGlobalConfigStatusManager;
import ai.traceable.anomaly.config.service.global.status.AnomalyGlobalConfigStatusValidator;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.UpdateAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.UpdateAnomalyGlobalConfigStatusResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class AnomalyGlobalConfigServiceImpl
    extends AnomalyGlobalConfigServiceGrpc.AnomalyGlobalConfigServiceImplBase {

  private final AnomalyGlobalConfigStatusValidator configValidator;
  private final AnomalyGlobalConfigStatusManager configStatusManager;

  @Inject
  public AnomalyGlobalConfigServiceImpl(
      AnomalyGlobalConfigStatusValidator validator,
      AnomalyGlobalConfigStatusManager configStatusManager) {
    this.configValidator = validator;
    this.configStatusManager = configStatusManager;
  }

  @Override
  public void getAnomalyGlobalConfigStatus(
      GetAnomalyGlobalConfigStatusRequest request,
      StreamObserver<GetAnomalyGlobalConfigStatusResponse> responseObserver) {

    Status status = configValidator.validate(request);
    if (!status.isOk()) {
      log.error(
          "Get Anomaly Global Config Status Request is not valid: {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    try {
      GetAnomalyGlobalConfigStatusResponse response =
          GetAnomalyGlobalConfigStatusResponse.newBuilder()
              .setConfigStatus(
                  configStatusManager.getAnomalyConfigStatus(
                      RequestContext.CURRENT.get(), request.getConfigScope()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateAnomalyGlobalConfigStatus(
      UpdateAnomalyGlobalConfigStatusRequest request,
      StreamObserver<UpdateAnomalyGlobalConfigStatusResponse> responseObserver) {
    Status status = configValidator.validate(request);
    if (!status.isOk()) {
      log.error(
          "Update Anomaly Global Config Status Request is not valid: {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    try {
      UpdateAnomalyGlobalConfigStatusResponse response =
          UpdateAnomalyGlobalConfigStatusResponse.newBuilder()
              .setConfigStatus(
                  configStatusManager.updateAnomalyConfigStatus(
                      RequestContext.CURRENT.get(),
                      request.getConfigScope(),
                      request.getConfigStatus()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }
}
