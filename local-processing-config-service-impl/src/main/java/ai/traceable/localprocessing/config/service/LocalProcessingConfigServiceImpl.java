package ai.traceable.localprocessing.config.service;

import ai.traceable.localprocessing.config.service.v1.GetDefaultProtectionModeRequest;
import ai.traceable.localprocessing.config.service.v1.GetDefaultProtectionModeResponse;
import ai.traceable.localprocessing.config.service.v1.GetLocalProcessingConfigRequest;
import ai.traceable.localprocessing.config.service.v1.GetLocalProcessingConfigResponse;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingConfigServiceGrpc;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRuleDetails;
import ai.traceable.localprocessing.config.service.v1.ProtectedEndpoint;
import ai.traceable.localprocessing.config.service.v1.ProtectionMode;
import ai.traceable.localprocessing.config.service.v1.ProtectionModeConfig;
import ai.traceable.localprocessing.config.service.v1.UpdateDefaultProtectionModeRequest;
import ai.traceable.localprocessing.config.service.v1.UpdateDefaultProtectionModeResponse;
import com.typesafe.config.Config;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class LocalProcessingConfigServiceImpl
    extends LocalProcessingConfigServiceGrpc.LocalProcessingConfigServiceImplBase {

  private final ConfigServiceCoordinator configServiceCoordinator;

  public LocalProcessingConfigServiceImpl(Channel configChannel, Config config) {
    this.configServiceCoordinator = new ConfigServiceCoordinatorImpl(configChannel, config);
  }

  @Override
  public void getLocalProcessingConfig(
      GetLocalProcessingConfigRequest request,
      StreamObserver<GetLocalProcessingConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();

      List<ProtectedEndpoint> protectedEndpoints =
          configServiceCoordinator.getAllLocalProcessingRules(requestContext).stream()
              .map(LocalProcessingRuleDetails::getRule)
              .map(
                  rule ->
                      ProtectedEndpoint.newBuilder()
                          .setProtectionMode(rule.getProtectionMode())
                          .setUrlPattern(rule.getUrlPattern())
                          .setHostHeader(rule.getHostHeader())
                          .build())
              .collect(Collectors.toList());

      responseObserver.onNext(
          GetLocalProcessingConfigResponse.newBuilder()
              .setProtectionModeConfig(
                  ProtectionModeConfig.newBuilder()
                      .setDefaultProtectionMode(
                          configServiceCoordinator.getDefaultProtectionModeConfig(requestContext))
                      .addAllProtectedEndpoints(protectedEndpoints)
                      .build())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get Local Processing Config RPC failed for request:{}", request, e);
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
