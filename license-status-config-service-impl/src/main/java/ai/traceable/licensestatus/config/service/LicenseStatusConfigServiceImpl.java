package ai.traceable.licensestatus.config.service;

import ai.traceable.licensestatus.config.service.v1.GetLicenseStatusRequest;
import ai.traceable.licensestatus.config.service.v1.GetLicenseStatusResponse;
import ai.traceable.licensestatus.config.service.v1.LicenseStatus;
import ai.traceable.licensestatus.config.service.v1.LicenseStatusConfigServiceGrpc;
import ai.traceable.licensestatus.config.service.v1.UpdateLicenseStatusRequest;
import ai.traceable.licensestatus.config.service.v1.UpdateLicenseStatusResponse;
import io.grpc.stub.StreamObserver;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class LicenseStatusConfigServiceImpl
    extends LicenseStatusConfigServiceGrpc.LicenseStatusConfigServiceImplBase {

  private final ConfigServiceCoordinator configServiceCoordinator;

  @Inject
  public LicenseStatusConfigServiceImpl(ConfigServiceCoordinator configServiceCoordinator) {
    this.configServiceCoordinator = configServiceCoordinator;
  }

  @Override
  public void updateLicenseStatus(
      UpdateLicenseStatusRequest request,
      StreamObserver<UpdateLicenseStatusResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      LicenseStatusConfigRequestValidator.validateOrThrow(requestContext, request);
      LicenseStatus licenseStatus = request.getLicenseStatus();
      LicenseStatus updatedLicenseStatus =
          configServiceCoordinator.upsertLicenseStatusConfig(requestContext, licenseStatus);
      responseObserver.onNext(
          UpdateLicenseStatusResponse.newBuilder().setLicenseStatus(updatedLicenseStatus).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Update License Status RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getLicenseStatus(
      GetLicenseStatusRequest request, StreamObserver<GetLicenseStatusResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      LicenseStatusConfigRequestValidator.validateOrThrow(requestContext);
      LicenseStatus updatedLicenseStatus =
          configServiceCoordinator.getLicenseStatusConfig(requestContext);
      responseObserver.onNext(
          GetLicenseStatusResponse.newBuilder().setLicenseStatus(updatedLicenseStatus).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get License Status RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
