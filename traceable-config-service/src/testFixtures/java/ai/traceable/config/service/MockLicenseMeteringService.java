package ai.traceable.config.service;

import ai.traceable.license.metering.service.api.v1.GetLicenseInfoRequest;
import ai.traceable.license.metering.service.api.v1.GetLicenseInfoResponse;
import ai.traceable.license.metering.service.api.v1.LicenseInfo;
import ai.traceable.license.metering.service.api.v1.LicenseMeteringServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class MockLicenseMeteringService
    extends LicenseMeteringServiceGrpc.LicenseMeteringServiceImplBase {
  public static final String TENANT_ID_PREFIX = "tenant_";
  public static final int TENANT_ID_PREFIX_LENGTH = TENANT_ID_PREFIX.length();

  @Override
  public void getLicenseInfo(
      GetLicenseInfoRequest request,
      StreamObserver<GetLicenseInfoResponse> responseStreamObserver) {
    String tenantId = RequestContext.CURRENT.get().getTenantId().get();
    if (tenantId.startsWith(TENANT_ID_PREFIX)) {
      responseStreamObserver.onNext(
          GetLicenseInfoResponse.newBuilder()
              .setLicenseInfo(
                  LicenseInfo.newBuilder()
                      .setTier(
                          LicenseInfo.Tier.valueOf(tenantId.substring(TENANT_ID_PREFIX_LENGTH))))
              .build());
      responseStreamObserver.onCompleted();
    }
    responseStreamObserver.onError(Status.NOT_FOUND.asException());
  }
}
