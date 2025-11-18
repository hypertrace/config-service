package ai.traceable.certificate.management.config.service.v1;

import ai.traceable.certificate.management.config.service.v1.CertificateManagementConfigServiceGrpc.CertificateManagementConfigServiceImplBase;
import ai.traceable.certificate.management.config.service.v1.manager.CertificateConfigManager;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class CertificateManagementConfigServiceImpl
    extends CertificateManagementConfigServiceImplBase {

  private final CertificateConfigManager certificateConfigManager;

  @Override
  public void createCertificate(
      CreateCertificateRequest request,
      StreamObserver<CreateCertificateResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      Certificate certificate = certificateConfigManager.createCertificate(ctx, request);

      responseObserver.onNext(
          CreateCertificateResponse.newBuilder().setCertificate(certificate).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Create certificate failed with request: {} and context: {}", request, ctx, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getCertificates(
      GetCertificatesRequest request, StreamObserver<GetCertificatesResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      List<Certificate> certificates =
          certificateConfigManager.getCertificates(ctx, request.getFilter());
      responseObserver.onNext(
          GetCertificatesResponse.newBuilder().addAllCertificates(certificates).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get certificates failed with request: {} and context: {}", request, ctx, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateCertificate(
      UpdateCertificateRequest request,
      StreamObserver<UpdateCertificateResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      Certificate certificate =
          certificateConfigManager.updateCertificate(ctx, request.getId(), request);

      responseObserver.onNext(
          UpdateCertificateResponse.newBuilder().setCertificate(certificate).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Update certificate failed with request: {} and context: {}", request, ctx, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteCertificate(
      DeleteCertificateRequest request,
      StreamObserver<DeleteCertificateResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      certificateConfigManager.deleteCertificate(ctx, request.getId());

      responseObserver.onNext(DeleteCertificateResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Delete certificate failed with request: {} and context: {}", request, ctx, e);
      responseObserver.onError(e);
    }
  }
}
