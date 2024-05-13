package ai.traceable.ast.config.service;

import ai.traceable.ast.config.service.configs.AstConfigServiceConfig;
import ai.traceable.ast.config.service.rules.AstConfigServiceRequestValidator;
import ai.traceable.ast.config.service.rules.CustomTestPluginManager;
import ai.traceable.ast.config.service.rules.RulesManager;
import ai.traceable.ast.config.service.v1.AstConfigServiceGrpc.AstConfigServiceImplBase;
import ai.traceable.ast.config.service.v1.AstDisabledConfig;
import ai.traceable.ast.config.service.v1.AstFeatureConfig;
import ai.traceable.ast.config.service.v1.CreateCustomTestPluginRequest;
import ai.traceable.ast.config.service.v1.CreateCustomTestPluginResponse;
import ai.traceable.ast.config.service.v1.DeleteCustomTestPluginRequest;
import ai.traceable.ast.config.service.v1.DeleteCustomTestPluginResponse;
import ai.traceable.ast.config.service.v1.DeleteVulnerabilityMetadataOverridesConfigRequest;
import ai.traceable.ast.config.service.v1.DeleteVulnerabilityMetadataOverridesConfigResponse;
import ai.traceable.ast.config.service.v1.EditVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.EditVulnerabilityMetadataOverridesResponse;
import ai.traceable.ast.config.service.v1.GetAllCustomTestPluginsRequest;
import ai.traceable.ast.config.service.v1.GetAllCustomTestPluginsResponse;
import ai.traceable.ast.config.service.v1.GetAllVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetAllVulnerabilityMetadataOverridesResponse;
import ai.traceable.ast.config.service.v1.GetAstFeatureConfigsRequest;
import ai.traceable.ast.config.service.v1.GetAstFeatureConfigsResponse;
import ai.traceable.ast.config.service.v1.GetScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.GetScanPurgeConfigResponse;
import ai.traceable.ast.config.service.v1.GetVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetVulnerabilityMetadataOverridesResponse;
import ai.traceable.ast.config.service.v1.ScanPurgeConfig;
import ai.traceable.ast.config.service.v1.UpdateAstFeatureConfigRequest;
import ai.traceable.ast.config.service.v1.UpdateAstFeatureConfigResponse;
import ai.traceable.ast.config.service.v1.UpdateCustomTestPluginRequest;
import ai.traceable.ast.config.service.v1.UpdateCustomTestPluginResponse;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigResponse;
import ai.traceable.ast.config.service.v1.VulnerabilityMetadataOverrides;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
class AstConfigServiceImpl extends AstConfigServiceImplBase {
  private final AstConfigServiceRequestValidator requestValidator;
  private final RulesManager rulesManager;
  private final AstConfigServiceConfig config;
  private final CustomTestPluginManager customTestPluginManager;

  @Override
  public void updateScanPurgeConfig(
      UpdateScanPurgeConfigRequest request,
      StreamObserver<UpdateScanPurgeConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      ScanPurgeConfig scanPurgeConfig = rulesManager.updateScanPurgeConfig(requestContext, request);

      responseObserver.onNext(
          UpdateScanPurgeConfigResponse.newBuilder().setPurgeConfig(scanPurgeConfig).build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(
          "Unable to update scan purge config for request {} with context {}",
          request,
          requestContext,
          exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void getScanPurgeConfig(
      GetScanPurgeConfigRequest request,
      StreamObserver<GetScanPurgeConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      ScanPurgeConfig scanPurgeConfig =
          rulesManager
              .getScanPurgeConfig(requestContext)
              .orElseGet(this::getDefaultScanPurgeConfig);

      responseObserver.onNext(
          GetScanPurgeConfigResponse.newBuilder().setPurgeConfig(scanPurgeConfig).build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Unable to fetch scan purge config with context {}", requestContext, exception);
      responseObserver.onError(exception);
    }
  }

  private ScanPurgeConfig getDefaultScanPurgeConfig() {
    return ScanPurgeConfig.newBuilder()
        .setPurgeDuration(config.getDefaultPurgeDuration())
        .setScanRetentionLimitPerSuite(config.getDefaultScanRetentionLimitPerSuite())
        .build();
  }

  @Override
  public void editVulnerabilityMetadataOverrides(
      EditVulnerabilityMetadataOverridesRequest request,
      StreamObserver<EditVulnerabilityMetadataOverridesResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      VulnerabilityMetadataOverrides updatedMetadata =
          rulesManager.updateVulnerabilityMetadataOverridesConfig(requestContext, request);
      responseObserver.onNext(
          EditVulnerabilityMetadataOverridesResponse.newBuilder()
              .setVulnerabilityMetadataOverrides(updatedMetadata)
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(
          "Unable to update fields for plugin for request {} with context {}",
          request,
          requestContext,
          exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void getVulnerabilityMetadataOverrides(
      GetVulnerabilityMetadataOverridesRequest request,
      StreamObserver<GetVulnerabilityMetadataOverridesResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      Optional<VulnerabilityMetadataOverrides> vulnerabilityMetadataOptional =
          rulesManager.getVulnerabilityMetadataOverridesConfig(requestContext, request);
      if (vulnerabilityMetadataOptional.isPresent()) {
        responseObserver.onNext(
            GetVulnerabilityMetadataOverridesResponse.newBuilder()
                .setVulnerabilityMetadataOverrides(vulnerabilityMetadataOptional.get())
                .build());
      } else {
        responseObserver.onNext(GetVulnerabilityMetadataOverridesResponse.newBuilder().build());
      }
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(
          "Unable to get vulnerability metadata overrides for plugin for request {} with context {}",
          request,
          requestContext,
          exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteVulnerabilityMetadataOverridesConfig(
      DeleteVulnerabilityMetadataOverridesConfigRequest request,
      StreamObserver<DeleteVulnerabilityMetadataOverridesConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      rulesManager.deleteVulnerabilityMetadataOverridesConfig(requestContext, request);
      responseObserver.onNext(
          DeleteVulnerabilityMetadataOverridesConfigResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(
          "Unable to delete vulnerability metadata config for request {} with context {}",
          request,
          requestContext,
          exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void getAllVulnerabilityMetadataOverrides(
      GetAllVulnerabilityMetadataOverridesRequest request,
      StreamObserver<GetAllVulnerabilityMetadataOverridesResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          GetAllVulnerabilityMetadataOverridesResponse.newBuilder()
              .addAllVulnerabilityMetadataOverrides(
                  rulesManager.getAllVulnerabilityMetadataOverridesConfig(requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(
          "Unable to fetch all vulnerability metadata overrides for request {} with context {}",
          request,
          requestContext,
          exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void getAstFeatureConfigs(
      GetAstFeatureConfigsRequest request,
      StreamObserver<GetAstFeatureConfigsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      GetAstFeatureConfigsResponse.Builder getAstFeatureConfigsResponseBuilder =
          GetAstFeatureConfigsResponse.newBuilder()
              .addAllConfigs(rulesManager.getAstFeatureConfigs(requestContext, request));
      if (config.defaultIsAstEnabled()) {
        getAstFeatureConfigsResponseBuilder.setDefaultEnabledConfig(
            config.getDefaultAstEnabledConfig());
      } else {
        getAstFeatureConfigsResponseBuilder.setDefaultDisabledConfig(
            AstDisabledConfig.newBuilder().build());
      }
      responseObserver.onNext(getAstFeatureConfigsResponseBuilder.build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(
          "Unable to fetch ast feature configs for request {} with context {}",
          request,
          requestContext,
          exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateAstFeatureConfig(
      UpdateAstFeatureConfigRequest request,
      StreamObserver<UpdateAstFeatureConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      AstFeatureConfig updatedAstFeatureConfig =
          rulesManager.updateAstFeatureConfig(requestContext, request);
      responseObserver.onNext(
          UpdateAstFeatureConfigResponse.newBuilder()
              .setUpdatedConfig(updatedAstFeatureConfig)
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(
          "Unable to update ast feature configs for request {} with context {}",
          request,
          requestContext,
          exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void getAllCustomTestPlugins(
      GetAllCustomTestPluginsRequest request,
      StreamObserver<GetAllCustomTestPluginsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          GetAllCustomTestPluginsResponse.newBuilder()
              .addAllCustomTestPlugins(
                  customTestPluginManager.getAllCustomTestPlugins(requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(
          "Unable to fetch all custom test plugins for request {} with context {}",
          request,
          requestContext,
          exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void createCustomTestPlugin(
      CreateCustomTestPluginRequest request,
      StreamObserver<CreateCustomTestPluginResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          CreateCustomTestPluginResponse.newBuilder()
              .setCustomTestPlugin(
                  customTestPluginManager.createCustomTestPlugin(requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(
          "Unable to create custom test plugin for request {} with context {}",
          request,
          requestContext,
          exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateCustomTestPlugin(
      UpdateCustomTestPluginRequest request,
      StreamObserver<UpdateCustomTestPluginResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          UpdateCustomTestPluginResponse.newBuilder()
              .setCustomTestPlugin(
                  customTestPluginManager.updateCustomTestPlugin(requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(
          "Unable to update custom test plugin for request {} with context {}",
          request,
          requestContext,
          exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteCustomTestPlugin(
      DeleteCustomTestPluginRequest request,
      StreamObserver<DeleteCustomTestPluginResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      customTestPluginManager.deleteCustomTestPlugin(requestContext, request);
      responseObserver.onNext(DeleteCustomTestPluginResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(
          "Unable to delete custom test plugin for request {} with context {}",
          request,
          requestContext,
          exception);
      responseObserver.onError(exception);
    }
  }
}
