package ai.traceable.localprocessing.config.service.ruleservice;

import ai.traceable.localprocessing.config.service.coordinator.ConfigServiceCoordinator;
import ai.traceable.localprocessing.config.service.v1.CreateLocalProcessingRuleRequest;
import ai.traceable.localprocessing.config.service.v1.CreateLocalProcessingRuleResponse;
import ai.traceable.localprocessing.config.service.v1.DeleteLocalProcessingRuleRequest;
import ai.traceable.localprocessing.config.service.v1.DeleteLocalProcessingRuleResponse;
import ai.traceable.localprocessing.config.service.v1.GetAllLocalProcessingRulesRequest;
import ai.traceable.localprocessing.config.service.v1.GetAllLocalProcessingRulesResponse;
import ai.traceable.localprocessing.config.service.v1.GetDefaultProtectionModeRequest;
import ai.traceable.localprocessing.config.service.v1.GetDefaultProtectionModeResponse;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRulesServiceGrpc;
import ai.traceable.localprocessing.config.service.v1.ProtectionMode;
import ai.traceable.localprocessing.config.service.v1.UpdateDefaultProtectionModeRequest;
import ai.traceable.localprocessing.config.service.v1.UpdateDefaultProtectionModeResponse;
import ai.traceable.localprocessing.config.service.v1.UpdateLocalProcessingRuleRequest;
import ai.traceable.localprocessing.config.service.v1.UpdateLocalProcessingRuleResponse;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class LocalProcessingRulesServiceImpl
    extends LocalProcessingRulesServiceGrpc.LocalProcessingRulesServiceImplBase {

  private final ConfigServiceCoordinator configServiceCoordinator;

  @Inject
  public LocalProcessingRulesServiceImpl(ConfigServiceCoordinator configServiceCoordinator) {
    this.configServiceCoordinator = configServiceCoordinator;
  }

  @Override
  public void createLocalProcessingRule(
      CreateLocalProcessingRuleRequest request,
      StreamObserver<CreateLocalProcessingRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      responseObserver.onNext(
          CreateLocalProcessingRuleResponse.newBuilder()
              .setLocalProcessingRuleDetails(
                  configServiceCoordinator.createLocalProcessingRule(
                      requestContext, request.getNewLocalProcessingRule()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Create Redaction Rule RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateLocalProcessingRule(
      UpdateLocalProcessingRuleRequest request,
      StreamObserver<UpdateLocalProcessingRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      responseObserver.onNext(
          UpdateLocalProcessingRuleResponse.newBuilder()
              .setLocalProcessingRuleDetails(
                  configServiceCoordinator.updateLocalProcessingRule(
                      requestContext, request.getLocalProcessingRule()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Update Redaction Rule RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAllLocalProcessingRules(
      GetAllLocalProcessingRulesRequest request,
      StreamObserver<GetAllLocalProcessingRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      responseObserver.onNext(
          GetAllLocalProcessingRulesResponse.newBuilder()
              .addAllLocalProcessingRulesDetails(
                  configServiceCoordinator.getAllLocalProcessingRules(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get All Redaction Rules RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteLocalProcessingRule(
      DeleteLocalProcessingRuleRequest request,
      StreamObserver<DeleteLocalProcessingRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      configServiceCoordinator.deleteLocalProcessingRule(
          requestContext, request.getLocalProcessingRuleId());
      responseObserver.onNext(DeleteLocalProcessingRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Delete Redaction Rule RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateDefaultProtectionMode(
      UpdateDefaultProtectionModeRequest request,
      StreamObserver<UpdateDefaultProtectionModeResponse> responseObserver) {

    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      LocalProcessingConfigRequestValidator.validateOrThrow(requestContext, request);
      ProtectionMode defaultProtectionMode = request.getDefaultProtectionMode();
      ProtectionMode updatedDefaultProtectionMode =
          configServiceCoordinator.upsertDefaultProtectionModeConfig(
              requestContext, defaultProtectionMode);
      responseObserver.onNext(
          UpdateDefaultProtectionModeResponse.newBuilder()
              .setDefaultProtectionMode(updatedDefaultProtectionMode)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Update Default Protection Mode Config RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getDefaultProtectionMode(
      GetDefaultProtectionModeRequest request,
      StreamObserver<GetDefaultProtectionModeResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      LocalProcessingConfigRequestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          GetDefaultProtectionModeResponse.newBuilder()
              .setDefaultProtectionMode(
                  configServiceCoordinator.getDefaultProtectionModeConfig(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get Default Protection Mode Config RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
