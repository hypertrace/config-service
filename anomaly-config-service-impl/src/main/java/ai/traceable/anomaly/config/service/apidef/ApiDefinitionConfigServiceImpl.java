package ai.traceable.anomaly.config.service.apidef;

import ai.traceable.anomaly.config.service.apidef.trainer.ApiDefinitionTrainerConfigServiceValidator;
import ai.traceable.anomaly.config.service.apidef.trainer.ConfigManager;
import ai.traceable.anomaly.config.service.v1.apidef.*;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class ApiDefinitionConfigServiceImpl
    extends ApiDefinitionConfigServiceGrpc.ApiDefinitionConfigServiceImplBase {
  private final ApiDefinitionTrainerConfigServiceValidator validator;
  private final ConfigManager configManager;

  @Inject
  public ApiDefinitionConfigServiceImpl(
      ApiDefinitionTrainerConfigServiceValidator validator, ConfigManager configManager) {
    this.validator = validator;
    this.configManager = configManager;
  }

  @Override
  public void getApiDefinitionTrainerConfigs(
      GetApiDefinitionTrainerConfigsRequest request,
      StreamObserver<GetApiDefinitionTrainerConfigsResponse> responseObserver) {
    Status status = validator.validate(request);

    if (!status.isOk()) {
      log.error(
          "Get Api Definition Trainer Config Request is not valid: {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    try {
      GetApiDefinitionTrainerConfigsResponse response =
          GetApiDefinitionTrainerConfigsResponse.newBuilder()
              .setApiDefinitionTrainerConfig(
                  configManager.getApiDefinitionTrainerConfig(
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
  public void updateApiDefinitionTrainerConfigs(
      UpdateApiDefinitionTrainerConfigsRequest request,
      StreamObserver<UpdateApiDefinitionTrainerConfigsResponse> responseObserver) {
    Status status = validator.validate(request);

    if (!status.isOk()) {
      log.error(
          "Update Api Definition Trainer Config Request is not valid: {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    try {
      UpdateApiDefinitionTrainerConfigsResponse response =
          UpdateApiDefinitionTrainerConfigsResponse.newBuilder()
              .setApiDefinitionTrainerConfig(
                  configManager.updateApiDefinitionTrainerConfig(
                      RequestContext.CURRENT.get(),
                      request.getApiDefinitionApplierConfigsList(),
                      request.getConfigScope()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }
}
