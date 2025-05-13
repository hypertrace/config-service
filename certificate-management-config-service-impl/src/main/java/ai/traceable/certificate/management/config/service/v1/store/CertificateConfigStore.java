package ai.traceable.certificate.management.config.service.v1.store;

import ai.traceable.certificate.management.config.service.v1.Certificate;
import ai.traceable.certificate.management.config.service.v1.CertificateFilter;
import ai.traceable.certificate.management.config.service.v1.CertificateStorageDetails;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class CertificateConfigStore extends IdentifiedObjectStore<Certificate> {

  private static final String CERTIFICATE_CONFIG_NAMESPACE = "certificateManagementConfig";
  private static final String CERTIFICATE_CONFIG_RESOURCE_NAME = "certificate-management";

  @Inject
  public CertificateConfigStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        CERTIFICATE_CONFIG_NAMESPACE,
        CERTIFICATE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<Certificate> buildDataFromValue(Value value) {
    try {
      Certificate.Builder builder = Certificate.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception exception) {
      log.error("Parsing the value {} into Certificate failed with an exception", value, exception);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(Certificate certificate) {
    return ConfigProtoConverter.convertToValue(certificate);
  }

  @Override
  protected String getContextFromData(Certificate certificate) {
    return certificate.getId();
  }

  public Certificate createCertificate(RequestContext ctx, Certificate certificate) {
    return upsertObject(ctx, certificate).getData();
  }

  public Certificate getCertificate(RequestContext ctx, String id) {
    return getData(ctx, id).orElse(null);
  }

  public List<Certificate> getCertificates(RequestContext ctx, CertificateFilter filter) {
    return getAllConfigData(ctx).stream()
        .filter(cert -> matchesFilter(cert, filter))
        .collect(Collectors.toList());
  }

  public Certificate updateCertificate(RequestContext ctx, Certificate certificate) {
    return upsertObject(ctx, certificate).getData();
  }

  public void deleteCertificate(RequestContext ctx, String id) {
    deleteObject(ctx, id);
  }

  private boolean matchesFilter(Certificate certificate, CertificateFilter filter) {
    // Filter by IDs if specified
    if (filter.getIdsCount() > 0 && !filter.getIdsList().contains(certificate.getId())) {
      return false;
    }

    // Filter by domain names if specified
    if (filter.getDomainNamesCount() > 0) {
      boolean matchesDomain =
          certificate.getMetadata().getDomainNamesList().stream()
              .anyMatch(filter.getDomainNamesList()::contains);
      if (!matchesDomain) {
        return false;
      }
    }

    // Filter by AWS-specific criteria if specified
    if (filter.hasAwsFilter()) {
      CertificateFilter.AwsFilter awsFilter = filter.getAwsFilter();

      // Filter by regions if specified
      if (awsFilter.getRegionsCount() > 0) {
        boolean matchesRegion =
            certificate.getStorageList().stream()
                .filter(CertificateStorageDetails::hasAws)
                .anyMatch(
                    storage -> awsFilter.getRegionsList().contains(storage.getAws().getRegion()));
        if (!matchesRegion) {
          return false;
        }
      }
    }

    // Check if all required label keys are present
    if (filter.getLabelKeysCount() > 0) {
      for (String requiredKey : filter.getLabelKeysList()) {
        if (!certificate.getMetadata().getLabelsMap().containsKey(requiredKey)) {
          return false;
        }
      }
    }

    return true;
  }
}
