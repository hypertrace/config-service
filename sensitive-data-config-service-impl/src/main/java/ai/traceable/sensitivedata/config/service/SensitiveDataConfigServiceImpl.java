package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.PiiFilterConfigServiceImpl.SENSITIVE_DATA_CONFIG_SERVICE_CONFIG;

import ai.traceable.sensitivedata.config.service.v1.GetAutomaticSecretRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAutomaticSecretRedactionStrategyResponse;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyForTypeRequest;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyForTypeResponse;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.UpdateAutomaticSecretRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateAutomaticSecretRedactionStrategyResponse;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionStrategyForTypeRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionStrategyForTypeResponse;
import com.typesafe.config.Config;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class SensitiveDataConfigServiceImpl
    extends SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceImplBase {

  private final ConfigServiceCoordinator configServiceCoordinator;

  public SensitiveDataConfigServiceImpl(Channel configChannel, Config config) {
    this.configServiceCoordinator =
        new ConfigServiceCoordinatorImpl(
            configChannel, config.getConfig(SENSITIVE_DATA_CONFIG_SERVICE_CONFIG));
  }

  @Override
  public void updateRedactionStrategyForType(
      UpdateRedactionStrategyForTypeRequest request,
      StreamObserver<UpdateRedactionStrategyForTypeResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      ParamType paramType = request.getParamType();
      if (paramType != ParamType.PARAM_TYPE_HEADER) {
        throw new UnsupportedOperationException("This operation is only supported for headers");
      }
      RedactionStrategy redactionStrategy = request.getRedactionStrategy();
      ParamTypeRedactionStrategyConfig paramTypeRedactionStrategyConfig =
          new ParamTypeRedactionStrategyConfig(redactionStrategy);
      configServiceCoordinator.upsertParamTypeRedactionStrategyConfig(
          requestContext, paramType, paramTypeRedactionStrategyConfig);

      responseObserver.onNext(UpdateRedactionStrategyForTypeResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Update Redaction Strategy For Type RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getRedactionStrategyForType(
      GetRedactionStrategyForTypeRequest request,
      StreamObserver<GetRedactionStrategyForTypeResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      RedactionStrategy redactionStrategy =
          configServiceCoordinator.getParamTypeRedactionStrategy(
              requestContext, request.getParamType());
      responseObserver.onNext(
          GetRedactionStrategyForTypeResponse.newBuilder()
              .setRedactionStrategy(redactionStrategy)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get Redaction Strategy For Type RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateAutomaticSecretRedactionStrategy(
      UpdateAutomaticSecretRedactionStrategyRequest request,
      StreamObserver<UpdateAutomaticSecretRedactionStrategyResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      boolean automaticSecretRedactionStrategyEnabled = request.getEnabled();
      configServiceCoordinator.upsertAutomaticSecretRedactionStrategyConfig(
          requestContext,
          new AutomaticSecretRedactionStrategyConfig(automaticSecretRedactionStrategyEnabled));
      responseObserver.onNext(UpdateAutomaticSecretRedactionStrategyResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Update Automatic Secret Redaction Strategy For Type RPC failed for request:{}",
          request,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAutomaticSecretRedactionStrategy(
      GetAutomaticSecretRedactionStrategyRequest request,
      StreamObserver<GetAutomaticSecretRedactionStrategyResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      responseObserver.onNext(
          GetAutomaticSecretRedactionStrategyResponse.newBuilder()
              .setEnabled(
                  configServiceCoordinator.isAutomaticSecretRedactionStrategyEnabled(
                      requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Get Automatic Secret Redaction Strategy For Type RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
