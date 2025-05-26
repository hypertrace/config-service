package ai.traceable.certificate.management.config.service.v1.manager;

import ai.traceable.certificate.management.config.service.v1.Certificate;
import ai.traceable.certificate.management.config.service.v1.CertificateFilter;
import ai.traceable.certificate.management.config.service.v1.CreateCertificateRequest;
import ai.traceable.certificate.management.config.service.v1.UpdateCertificateRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface CertificateConfigManager {

  Certificate createCertificate(
      RequestContext ctx, CreateCertificateRequest createCertificateRequest);

  List<Certificate> getCertificates(RequestContext ctx, CertificateFilter filter);

  Certificate updateCertificate(RequestContext ctx, String id, UpdateCertificateRequest request);

  void deleteCertificate(RequestContext ctx, String id);
}
