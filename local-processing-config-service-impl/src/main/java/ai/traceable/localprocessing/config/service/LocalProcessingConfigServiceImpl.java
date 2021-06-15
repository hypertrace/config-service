package ai.traceable.localprocessing.config.service;

import static ai.traceable.localprocessing.config.service.v1.ProtectionMode.PROTECTION_MODE_CORE;

import ai.traceable.licensestatus.config.service.v1.GetLicenseStatusRequest;
import ai.traceable.licensestatus.config.service.v1.GetLicenseStatusResponse;
import ai.traceable.licensestatus.config.service.v1.LicenseLimit;
import ai.traceable.licensestatus.config.service.v1.LicenseStatusConfigServiceGrpc;
import ai.traceable.licensestatus.config.service.v1.LicenseStatusConfigServiceGrpc.LicenseStatusConfigServiceBlockingStub;
import ai.traceable.localprocessing.config.service.v1.GetLocalProcessingConfigRequest;
import ai.traceable.localprocessing.config.service.v1.GetLocalProcessingConfigResponse;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingConfigServiceGrpc;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRuleDetails;
import ai.traceable.localprocessing.config.service.v1.ProtectedEndpoint;
import ai.traceable.localprocessing.config.service.v1.ProtectionModeConfig;
import com.typesafe.config.Config;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class LocalProcessingConfigServiceImpl
    extends LocalProcessingConfigServiceGrpc.LocalProcessingConfigServiceImplBase {

  private final ConfigServiceCoordinator configServiceCoordinator;
  private final LicenseStatusConfigServiceBlockingStub licenseStatusConfigServiceBlockingStub;

  public LocalProcessingConfigServiceImpl(Channel configChannel, Config config) {
    this.configServiceCoordinator = new ConfigServiceCoordinatorImpl(configChannel, config);
    licenseStatusConfigServiceBlockingStub =
        LicenseStatusConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Override
  public void getLocalProcessingConfig(
      GetLocalProcessingConfigRequest request,
      StreamObserver<GetLocalProcessingConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();

      if (isLicenseLimitAvailable(requestContext)) {
        sendAllLocalProcessingRules(responseObserver, requestContext);
      } else {
        overrideAllLocalProcessingRulesWithDefaultCoreMode(responseObserver);
      }
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get Local Processing Config RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  private boolean isLicenseLimitAvailable(RequestContext requestContext) {
    GetLicenseStatusRequest request = GetLicenseStatusRequest.newBuilder().build();
    GetLicenseStatusResponse response =
        requestContext.call(() -> licenseStatusConfigServiceBlockingStub.getLicenseStatus(request));
    return response
        .getLicenseStatus()
        .getTracesLicenseLimit()
        .equals(LicenseLimit.LICENSE_LIMIT_AVAILABLE);
  }

  private void sendAllLocalProcessingRules(
      StreamObserver<GetLocalProcessingConfigResponse> responseObserver,
      RequestContext requestContext) {
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
  }

  private void overrideAllLocalProcessingRulesWithDefaultCoreMode(
      StreamObserver<GetLocalProcessingConfigResponse> responseObserver) {
    responseObserver.onNext(
        GetLocalProcessingConfigResponse.newBuilder()
            .setProtectionModeConfig(
                ProtectionModeConfig.newBuilder()
                    .setDefaultProtectionMode(PROTECTION_MODE_CORE)
                    .build())
            .build());
  }
}
