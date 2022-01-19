package ai.traceable.localprocessing.config.service;

import static ai.traceable.localprocessing.config.service.v1.ProtectionMode.PROTECTION_MODE_CORE;

import ai.traceable.licensestatus.config.service.v1.GetLicenseStatusRequest;
import ai.traceable.licensestatus.config.service.v1.GetLicenseStatusResponse;
import ai.traceable.licensestatus.config.service.v1.LicenseLimit;
import ai.traceable.licensestatus.config.service.v1.LicenseStatusConfigServiceGrpc;
import ai.traceable.licensestatus.config.service.v1.LicenseStatusConfigServiceGrpc.LicenseStatusConfigServiceBlockingStub;
import ai.traceable.localprocessing.config.service.apinaming.ApiNamingManager;
import ai.traceable.localprocessing.config.service.coordinator.ConfigServiceCoordinator;
import ai.traceable.localprocessing.config.service.customsignature.CustomModsecDetectionManager;
import ai.traceable.localprocessing.config.service.regularmodsec.RegularModsecDetectionManager;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.GetApiNamingModelRequest;
import ai.traceable.localprocessing.config.service.v1.GetApiNamingModelResponse;
import ai.traceable.localprocessing.config.service.v1.GetLocalProcessingConfigRequest;
import ai.traceable.localprocessing.config.service.v1.GetLocalProcessingConfigResponse;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingConfigServiceGrpc.LocalProcessingConfigServiceImplBase;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRuleDetails;
import ai.traceable.localprocessing.config.service.v1.ProtectedEndpoint;
import ai.traceable.localprocessing.config.service.v1.ProtectionModeConfig;
import ai.traceable.localprocessing.config.service.v1.SamplingPolicies;
import com.google.inject.Inject;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class LocalProcessingConfigServiceImpl extends LocalProcessingConfigServiceImplBase {

  private final ConfigServiceCoordinator configServiceCoordinator;
  private final LicenseStatusConfigServiceBlockingStub licenseStatusConfigServiceBlockingStub;
  private final RegularModsecDetectionManager regularModsecDetectionManager;
  private final CustomModsecDetectionManager customModsecDetectionManager;
  private final ApiNamingManager apiNamingManager;
  private final UuidGenerator uuidGenerator;
  private final LocalProcessingConfigRequestValidator localProcessingConfigRequestValidator;

  @Inject
  public LocalProcessingConfigServiceImpl(
      ManagedChannel channel,
      ConfigServiceCoordinator configServiceCoordinator,
      CustomModsecDetectionManager customModsecDetectionManager,
      RegularModsecDetectionManager regularModsecDetectionManager,
      ApiNamingManager apiNamingManager,
      UuidGenerator uuidGenerator,
      LocalProcessingConfigRequestValidator localProcessingConfigRequestValidator) {
    this.configServiceCoordinator = configServiceCoordinator;
    this.licenseStatusConfigServiceBlockingStub =
        LicenseStatusConfigServiceGrpc.newBlockingStub(channel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    this.regularModsecDetectionManager = regularModsecDetectionManager;
    this.customModsecDetectionManager = customModsecDetectionManager;
    this.apiNamingManager = apiNamingManager;
    this.uuidGenerator = uuidGenerator;
    this.localProcessingConfigRequestValidator = localProcessingConfigRequestValidator;
  }

  @Override
  public void getLocalProcessingConfig(
      GetLocalProcessingConfigRequest request,
      StreamObserver<GetLocalProcessingConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      responseObserver.onNext(
          GetLocalProcessingConfigResponse.newBuilder()
              .setProtectionModeConfig(
                  getProtectionModeConfig(request.getProtectionModeHash(), requestContext))
              .setCustomModsecDetectionRules(
                  customModsecDetectionManager.getEnabledRules(
                      request.getCustomModsecDetectionRulesHash()))
              .setRegularModsecDetectionRules(
                  regularModsecDetectionManager.getDetectionRules(
                      request.getRegularModsecDetectionRulesHash()))
              .setSamplingPolicies(getSamplingPoliciesConfig(request.getSamplingPoliciesHash()))
              .setModsecConfig(configServiceCoordinator.getModsecConfig())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get Local Processing Config RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getApiNamingModel(
      GetApiNamingModelRequest request,
      StreamObserver<GetApiNamingModelResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      localProcessingConfigRequestValidator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          GetApiNamingModelResponse.newBuilder()
              .addAllServiceResponses(
                  apiNamingManager.getServiceResponseList(requestContext, request))
              .addAllFallbackWildcardRegexes(apiNamingManager.getFallbackWildcardRegexes())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get Api Naming Model RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  private SamplingPolicies getSamplingPoliciesConfig(String requestHash) {
    SamplingPolicies samplingPolicies = configServiceCoordinator.getSamplingPoliciesConfig();
    String responseHash = uuidGenerator.generateId(samplingPolicies);
    if (!responseHash.equals(requestHash)) {
      return samplingPolicies.toBuilder().setSamplingPoliciesHash(responseHash).build();
    }
    // If config is up to date, return empty object with hash
    return SamplingPolicies.newBuilder().setSamplingPoliciesHash(responseHash).build();
  }

  private ProtectionModeConfig getProtectionModeConfig(
      String requestHash, RequestContext requestContext) {
    ProtectionModeConfig protectionModeConfig;
    if (isLicenseLimitAvailable(requestContext)) {
      protectionModeConfig = sendAllLocalProcessingRules(requestContext);
    } else {
      protectionModeConfig =
          ProtectionModeConfig.newBuilder().setDefaultProtectionMode(PROTECTION_MODE_CORE).build();
    }

    String responseHash = uuidGenerator.generateId(protectionModeConfig);
    if (!responseHash.equals(requestHash)) {
      return protectionModeConfig.toBuilder().setHash(responseHash).build();
    }

    // if config is up to date, return empty object with hash.
    return ProtectionModeConfig.newBuilder().setHash(responseHash).build();
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

  private ProtectionModeConfig sendAllLocalProcessingRules(RequestContext requestContext) {
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

    ProtectionModeConfig.Builder protectionModeConfigBuilder =
        ProtectionModeConfig.newBuilder()
            .setDefaultProtectionMode(
                configServiceCoordinator.getDefaultProtectionModeConfig(requestContext))
            .addAllProtectedEndpoints(protectedEndpoints);

    return protectionModeConfigBuilder.build();
  }
}
