package ai.traceable.data.obfuscation.config.service;

import ai.traceable.data.obfuscation.config.service.rules.ConfigManager;
import ai.traceable.data.obfuscation.config.service.v1.CreateDataObfuscationStrategyRequest;
import ai.traceable.data.obfuscation.config.service.v1.CreateDataObfuscationStrategyResponse;
import ai.traceable.data.obfuscation.config.service.v1.DataObfuscationConfigServiceGrpc.DataObfuscationConfigServiceImplBase;
import ai.traceable.data.obfuscation.config.service.v1.DeleteDataObfuscationStrategyRequest;
import ai.traceable.data.obfuscation.config.service.v1.DeleteDataObfuscationStrategyResponse;
import ai.traceable.data.obfuscation.config.service.v1.GetDataObfuscationStrategyRequest;
import ai.traceable.data.obfuscation.config.service.v1.GetDataObfuscationStrategyResponse;
import ai.traceable.data.obfuscation.config.service.v1.UpdateDataObfuscationStrategyRequest;
import ai.traceable.data.obfuscation.config.service.v1.UpdateDataObfuscationStrategyResponse;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class DataObfuscationConfigServiceImpl extends DataObfuscationConfigServiceImplBase {

  private final DataObfuscationConfigRequestValidator requestValidator;
  private final ConfigManager configManager;

  @Inject
  public DataObfuscationConfigServiceImpl(
      DataObfuscationConfigRequestValidator requestValidator, ConfigManager configManager) {
    this.requestValidator = requestValidator;
    this.configManager = configManager;
  }

  @Override
  public void createDataObfuscationStrategy(
      CreateDataObfuscationStrategyRequest request,
      StreamObserver<CreateDataObfuscationStrategyResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);
      CreateDataObfuscationStrategyResponse response =
          CreateDataObfuscationStrategyResponse.newBuilder()
              .setObfuscationStrategy(
                  configManager.createObfuscationStrategy(requestContext, request))
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Failed to create data obfuscation strategy", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getDataObfuscationStrategy(
      GetDataObfuscationStrategyRequest request,
      StreamObserver<GetDataObfuscationStrategyResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext);

      GetDataObfuscationStrategyResponse response =
          GetDataObfuscationStrategyResponse.newBuilder()
              .addAllObfuscationStrategies(configManager.getObfuscationStrategies(requestContext))
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Failed to get data obfuscation strategies", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateDataObfuscationStrategy(
      UpdateDataObfuscationStrategyRequest request,
      StreamObserver<UpdateDataObfuscationStrategyResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      UpdateDataObfuscationStrategyResponse response =
          UpdateDataObfuscationStrategyResponse.newBuilder()
              .setObfuscationStrategy(
                  configManager.updateObfuscationStrategy(requestContext, request))
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Failed to data obfuscation strategy", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteDataObfuscationStrategy(
      DeleteDataObfuscationStrategyRequest request,
      StreamObserver<DeleteDataObfuscationStrategyResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext);

      configManager.deleteObfuscationStrategy(requestContext);
      DeleteDataObfuscationStrategyResponse response =
          DeleteDataObfuscationStrategyResponse.newBuilder().build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Failed to delete data obfuscation strategy", e);
      responseObserver.onError(e);
    }
  }
}
