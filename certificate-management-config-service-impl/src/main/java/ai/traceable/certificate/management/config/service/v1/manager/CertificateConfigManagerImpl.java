package ai.traceable.certificate.management.config.service.v1.manager;

import ai.traceable.certificate.management.config.service.v1.*;
import ai.traceable.certificate.management.config.service.v1.store.CertificateConfigStore;
import ai.traceable.config.utils.UuidGenerator;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class CertificateConfigManagerImpl implements CertificateConfigManager {

  private final CertificateConfigStore store;
  private final CertificateValidator validator;
  private final UuidGenerator uuidGenerator;

  @Override
  public Certificate createCertificate(RequestContext ctx, CreateCertificateRequest request) {
    Status validationStatus = validator.validate(request);
    if (!validationStatus.isOk()) {
      throw new StatusRuntimeException(validationStatus);
    }

    // Generate a unique ID
    String id = uuidGenerator.generateRandomId();

    Certificate certificate =
        Certificate.newBuilder()
            .setId(id)
            .setName(request.getName())
            .setMetadata(request.getMetadata())
            .addAllStorage(request.getStorageList())
            .setStatusDetails(request.getStatusDetails())
            .build();

    return store.createCertificate(ctx, certificate);
  }

  @Override
  public List<Certificate> getCertificates(RequestContext ctx, CertificateFilter filter) {
    return store.getCertificates(ctx, filter);
  }

  @Override
  public Certificate updateCertificate(
      RequestContext ctx, String id, UpdateCertificateRequest request) {
    Status validationStatus = validator.validate(request);
    if (!validationStatus.isOk()) {
      throw new StatusRuntimeException(validationStatus);
    }

    // Check if certificate with the given ID exists
    Certificate existingCertificate = store.getCertificate(ctx, id);
    if (existingCertificate == null) {
      throw new StatusRuntimeException(Status.NOT_FOUND.withDescription("Certificate not found"));
    }

    try {
      // Update certificate
      Certificate.Builder updatedCertificateBuilder = existingCertificate.toBuilder();
      CertificateUpdate certificateUpdate = request.getCertificateUpdate();

      // Update name if provided
      if (certificateUpdate.hasUpdatedName()) {
        updatedCertificateBuilder.setName(certificateUpdate.getUpdatedName());
      }

      // Update metadata if provided
      if (certificateUpdate.hasMetadataUpdate()) {
        CertificateMetadataUpdate metadataUpdate = certificateUpdate.getMetadataUpdate();
        CertificateMetadata.Builder metadataBuilder = existingCertificate.getMetadata().toBuilder();

        // Update domain names if provided
        if (metadataUpdate.hasUpdatedDomainNames()) {
          metadataBuilder.clearDomainNames();
          metadataBuilder.addAllDomainNames(
              metadataUpdate.getUpdatedDomainNames().getStringValuesList());
        }

        // Update key algorithm if provided
        if (metadataUpdate.hasUpdatedKeyAlgorithm()) {
          metadataBuilder.setKeyAlgorithm(metadataUpdate.getUpdatedKeyAlgorithm());
        }

        // Update labels if provided
        if (metadataUpdate.hasUpdatedLabels()) {
          metadataBuilder.clearLabels();
          metadataBuilder.putAllLabels(metadataUpdate.getUpdatedLabels().getMapMap());
        }

        updatedCertificateBuilder.setMetadata(metadataBuilder);
      }

      // Update status details
      CertificateStatusDetails.Builder statusDetailsBuilder =
          existingCertificate.getStatusDetails().toBuilder();

      // Update expiration timestamp if provided
      if (certificateUpdate.hasUpdatedExpirationTimestamp()) {
        statusDetailsBuilder.setExpirationTimestamp(
            certificateUpdate.getUpdatedExpirationTimestamp());
      }

      // Update status if provided
      if (certificateUpdate.hasUpdatedStatus()) {
        statusDetailsBuilder.setStatus(certificateUpdate.getUpdatedStatus());
      }

      updatedCertificateBuilder.setStatusDetails(statusDetailsBuilder);

      Certificate updatedCertificate = updatedCertificateBuilder.build();

      return store.updateCertificate(ctx, updatedCertificate);
    } catch (Exception e) {
      throw new StatusRuntimeException(
          Status.INTERNAL.withDescription("Failed to update certificate: " + e.getMessage()));
    }
  }

  @Override
  public void deleteCertificate(RequestContext ctx, String id) {
    DeleteCertificateRequest request = DeleteCertificateRequest.newBuilder().setId(id).build();
    Status validationStatus = validator.validate(request);
    if (!validationStatus.isOk()) {
      throw validationStatus.asRuntimeException();
    }

    // Check if certificate exists and delete it
    Certificate certificate = store.getCertificate(ctx, id);
    if (certificate == null) {
      throw Status.NOT_FOUND
          .withDescription("Certificate not found with id: " + id)
          .asRuntimeException();
    }

    try {
      store.deleteCertificate(ctx, id);
    } catch (Exception e) {
      throw new StatusRuntimeException(
          Status.INTERNAL.withDescription("Failed to delete certificate: " + e.getMessage()));
    }
  }
}
