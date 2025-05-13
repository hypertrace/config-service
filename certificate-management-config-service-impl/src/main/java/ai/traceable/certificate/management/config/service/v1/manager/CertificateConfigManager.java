package ai.traceable.certificate.management.config.service.v1.manager;

import ai.traceable.certificate.management.config.service.v1.Certificate;
import ai.traceable.certificate.management.config.service.v1.CertificateFilter;
import ai.traceable.certificate.management.config.service.v1.CertificateMetadata;
import ai.traceable.certificate.management.config.service.v1.CertificateStatusDetails;
import ai.traceable.certificate.management.config.service.v1.CertificateStorageDetails;
import ai.traceable.certificate.management.config.service.v1.UpdateCertificateRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface CertificateConfigManager {

  Certificate createCertificate(
      RequestContext ctx,
      CertificateMetadata metadata,
      CertificateStatusDetails statusDetails,
      List<CertificateStorageDetails> storage);

  List<Certificate> getCertificates(RequestContext ctx, CertificateFilter filter);

  Certificate updateCertificate(RequestContext ctx, String id, UpdateCertificateRequest request);

  void deleteCertificate(RequestContext ctx, String id);
}
