package ai.traceable.certificate.management.config.service.v1.manager;

import ai.traceable.certificate.management.config.service.v1.*;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class CertificateValidator {

  public Status validate(CreateCertificateRequest request) {

    Status status = validateMetadata(request.getMetadata());
    if (!status.isOk()) {
      return status;
    }

    status = validateStatusDetails(request.getStatusDetails());
    if (!status.isOk()) {
      return status;
    }

    status = validateStorage(request.getStorageList());
    if (!status.isOk()) {
      return status;
    }

    if (request.getName().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Certificate name cannot be empty");
    }

    return Status.OK;
  }

  public Status validate(UpdateCertificateRequest request) {
    if (request.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Certificate ID cannot be empty");
    }

    // Validate certificate update
    if (!request.hasCertificateUpdate()) {
      return Status.INVALID_ARGUMENT.withDescription("Certificate update details are required");
    }

    Status certificateUpdateStatus = validateCertificateUpdate(request.getCertificateUpdate());
    if (!certificateUpdateStatus.isOk()) {
      return certificateUpdateStatus;
    }

    return Status.OK;
  }

  public Status validate(DeleteCertificateRequest request) {
    if (request.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Certificate ID cannot be empty");
    }
    return Status.OK;
  }

  private Status validateMetadata(CertificateMetadata metadata) {
    for (String domainName : metadata.getDomainNamesList()) {
      if (!domainName.contains(".")) {
        return Status.INVALID_ARGUMENT.withDescription("Invalid domain name received");
      }
    }

    if (metadata.getKeyAlgorithm().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Key algorithm cannot be empty");
    }

    return Status.OK;
  }

  private Status validateStatusDetails(CertificateStatusDetails statusDetails) {
    if (statusDetails.getStatus() == CertificateStatus.CERTIFICATE_STATUS_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription("Certificate status cannot be UNSPECIFIED");
    }

    // Validate expiry timestamp if present
    if (statusDetails.hasExpirationTimestamp()) {
      Timestamp expiryTimestamp = statusDetails.getExpirationTimestamp();
      if (expiryTimestamp.getSeconds() <= 0) {
        return Status.INVALID_ARGUMENT.withDescription(
            "Certificate expiry timestamp must be a valid future date");
      }
    }

    return Status.OK;
  }

  private Status validateStorage(List<CertificateStorageDetails> storageList) {
    if (storageList.isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Certificate must have storage details");
    }

    for (CertificateStorageDetails storage : storageList) {
      if (storage.hasAws()) {
        AwsStorageDetails aws = storage.getAws();
        if (aws.getArn().isEmpty()) {
          return Status.INVALID_ARGUMENT.withDescription("AWS ARN cannot be empty");
        }
        if (aws.getRegion().isEmpty()) {
          return Status.INVALID_ARGUMENT.withDescription("AWS region cannot be empty");
        }
      } else {
        return Status.INVALID_ARGUMENT.withDescription("Unsupported storage provider");
      }
    }

    return Status.OK;
  }

  private Status validateCertificateUpdate(CertificateUpdate certificateUpdate) {
    // Validate name if present
    if (certificateUpdate.hasUpdatedName() && certificateUpdate.getUpdatedName().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Certificate name cannot be empty");
    }

    // Validate metadata change if present
    if (certificateUpdate.hasMetadataUpdate()) {
      Status metadataChangeStatus = validateMetadataUpdate(certificateUpdate.getMetadataUpdate());
      if (!metadataChangeStatus.isOk()) {
        return metadataChangeStatus;
      }
    }

    // Validate status if present
    if (certificateUpdate.hasUpdatedStatus()
        && certificateUpdate.getUpdatedStatus()
            == CertificateStatus.CERTIFICATE_STATUS_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription("Certificate status cannot be UNSPECIFIED");
    }

    // Validate expiry timestamp if present
    if (certificateUpdate.hasUpdatedExpirationTimestamp()) {
      Timestamp expiryTimestamp = certificateUpdate.getUpdatedExpirationTimestamp();
      if (expiryTimestamp.getSeconds() <= 0) {
        return Status.INVALID_ARGUMENT.withDescription(
            "Certificate expiry timestamp must be a valid future date");
      }
    }

    return Status.OK;
  }

  private Status validateMetadataUpdate(CertificateMetadataUpdate metadataUpdate) {
    // Validate domain names if present
    if (metadataUpdate.hasUpdatedDomainNames()) {
      StringList domainNamesChange = metadataUpdate.getUpdatedDomainNames();
      if (domainNamesChange.getStringValuesList().isEmpty()) {
        return Status.INVALID_ARGUMENT.withDescription(
            "Certificate must have at least one domain name");
      }

      // Validate each domain name
      for (String domainName : domainNamesChange.getStringValuesList()) {
        if (!domainName.contains(".")) {
          return Status.INVALID_ARGUMENT.withDescription("Invalid domain name received");
        }
      }
    }

    // Validate key algorithm if present
    if (metadataUpdate.hasUpdatedKeyAlgorithm()
        && metadataUpdate.getUpdatedKeyAlgorithm().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Key algorithm cannot be empty if provided");
    }

    return Status.OK;
  }
}
