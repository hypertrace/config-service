package ai.traceable.data.protection.config.service;

import ai.traceable.data.protection.config.service.v1.DataProtectionConfig;
import ai.traceable.data.protection.config.service.v1.DataProtectionConfigServiceGrpc;
import ai.traceable.data.protection.config.service.v1.DataSensitivity;
import ai.traceable.data.protection.config.service.v1.DeleteScopedDataProtectionConfigRequest;
import ai.traceable.data.protection.config.service.v1.DeleteScopedDataProtectionConfigResponse;
import ai.traceable.data.protection.config.service.v1.GetResolvedScopedDataProtectionConfigRequest;
import ai.traceable.data.protection.config.service.v1.GetResolvedScopedDataProtectionConfigResponse;
import ai.traceable.data.protection.config.service.v1.ScopedDataProtectionConfig;
import ai.traceable.data.protection.config.service.v1.UpsertScopedDataProtectionConfigRequest;
import ai.traceable.data.protection.config.service.v1.UpsertScopedDataProtectionConfigResponse;
import io.grpc.stub.StreamObserver;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class DataProtectionConfigServiceImpl
    extends DataProtectionConfigServiceGrpc.DataProtectionConfigServiceImplBase {
  private static final ScopedDataProtectionConfig DEFAULT_SCOPED_DATA_PROTECTION_CONFIG =
      ScopedDataProtectionConfig.newBuilder()
          .setConfig(
              DataProtectionConfig.newBuilder()
                  .setMinDataSensitivity(DataSensitivity.DATA_SENSITIVITY_LOW)
                  .build())
          .build();
  private ConfigValidator dataProtectionConfigValidator;
  private ConfigManager configManager;

  @Override
  public void getResolvedScopedDataProtectionConfig(
      GetResolvedScopedDataProtectionConfigRequest request,
      StreamObserver<GetResolvedScopedDataProtectionConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      dataProtectionConfigValidator.validateGetRequest(requestContext, request);
      responseObserver.onNext(
          GetResolvedScopedDataProtectionConfigResponse.newBuilder()
              .setConfig(
                  configManager
                      .getResolvedScopedDataProtectionConfig(request, requestContext)
                      .orElse(DEFAULT_SCOPED_DATA_PROTECTION_CONFIG))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.warn(
          "Error retrieving resolved scoped data protection config for customer with request context {}",
          requestContext,
          exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void upsertScopedDataProtectionConfig(
      UpsertScopedDataProtectionConfigRequest request,
      StreamObserver<UpsertScopedDataProtectionConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      dataProtectionConfigValidator.validateUpsertRequest(requestContext, request);
      responseObserver.onNext(
          UpsertScopedDataProtectionConfigResponse.newBuilder()
              .setConfig(configManager.upsertScopedDataProtectionConfig(request, requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.warn(
          "Error upserting scoped data protection config: {} with request context {}",
          request,
          requestContext,
          exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteScopedDataProtectionConfig(
      DeleteScopedDataProtectionConfigRequest request,
      StreamObserver<DeleteScopedDataProtectionConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.dataProtectionConfigValidator.validateDeleteRequest(requestContext, request);
      configManager.deleteScopedDataProtectionConfig(request, requestContext);
      responseObserver.onNext(DeleteScopedDataProtectionConfigResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.warn(
          "Error deleting scoped data protection config: {} with request context {}",
          request,
          requestContext,
          exception);
      responseObserver.onError(exception);
    }
  }
}
