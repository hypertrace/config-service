package ai.traceable.localprocessing.config.service;

import ai.traceable.localprocessing.config.service.v1.GetLocalProcessingConfigRequest;
import ai.traceable.localprocessing.config.service.v1.GetLocalProcessingConfigResponse;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingConfigServiceGrpc;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRuleDetails;
import ai.traceable.localprocessing.config.service.v1.ProtectedEndpoint;
import ai.traceable.localprocessing.config.service.v1.ProtectionMode;
import ai.traceable.localprocessing.config.service.v1.ProtectionModeConfig;
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

  static final String LOCAL_PROCESSING_CONFIG_SERVICE_CONFIG = "local.processing.config.service";
  static final String DEFAULT_PROTECTION_MODE = "default.protection.mode";
  private final ConfigServiceCoordinator configServiceCoordinator;
  private final ProtectionMode defaultProtectionMode;

  public LocalProcessingConfigServiceImpl(Channel configChannel, Config config) {
    this.configServiceCoordinator = new ConfigServiceCoordinatorImpl(configChannel);
    this.defaultProtectionMode =
        ProtectionMode.valueOf(
            config
                .getConfig(LOCAL_PROCESSING_CONFIG_SERVICE_CONFIG)
                .getString(DEFAULT_PROTECTION_MODE));
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
                      .setDefaultProtectionMode(defaultProtectionMode)
                      .addAllProtectedEndpoints(protectedEndpoints)
                      .build())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get Local Processing Config RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
