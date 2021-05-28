package ai.traceable.sensitivedata.config.service;

import ai.traceable.sensitivedata.config.service.v1.CreateRedactionRuleRequest;
import ai.traceable.sensitivedata.config.service.v1.CreateRedactionRuleResponse;
import ai.traceable.sensitivedata.config.service.v1.DeleteRedactionRuleRequest;
import ai.traceable.sensitivedata.config.service.v1.DeleteRedactionRuleResponse;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesResponse;
import ai.traceable.sensitivedata.config.service.v1.GetAutomaticSecretRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAutomaticSecretRedactionStrategyResponse;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyForTypeRequest;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyForTypeResponse;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.UpdateAutomaticSecretRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateAutomaticSecretRedactionStrategyResponse;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionRuleRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionRuleResponse;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionStrategyForTypeRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionStrategyForTypeResponse;
import io.grpc.stub.StreamObserver;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class SensitiveDataConfigServiceImpl
    extends SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceImplBase {

  private final ConfigServiceCoordinator configServiceCoordinator;

  @Inject
  SensitiveDataConfigServiceImpl(ConfigServiceCoordinator configServiceCoordinator) {
    this.configServiceCoordinator = configServiceCoordinator;
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

  @Override
  public void createRedactionRule(
      CreateRedactionRuleRequest request,
      StreamObserver<CreateRedactionRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      responseObserver.onNext(
          CreateRedactionRuleResponse.newBuilder()
              .setRedactionRule(
                  configServiceCoordinator.createRedactionRule(
                      requestContext, request.getNewRedactionRule()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Create Redaction Rule RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateRedactionRule(
      UpdateRedactionRuleRequest request,
      StreamObserver<UpdateRedactionRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      responseObserver.onNext(
          UpdateRedactionRuleResponse.newBuilder()
              .setRedactionRule(
                  configServiceCoordinator.updateRedactionRule(
                      requestContext, request.getRedactionRule()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Update Redaction Rule RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAllRedactionRules(
      GetAllRedactionRulesRequest request,
      StreamObserver<GetAllRedactionRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      responseObserver.onNext(
          GetAllRedactionRulesResponse.newBuilder()
              .addAllRedactionRules(configServiceCoordinator.getAllRedactionRules(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get All Redaction Rules RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteRedactionRule(
      DeleteRedactionRuleRequest request,
      StreamObserver<DeleteRedactionRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      configServiceCoordinator.deleteRedactionRule(requestContext, request.getRedactionRuleId());
      responseObserver.onNext(DeleteRedactionRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Delete Redaction Rule RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
