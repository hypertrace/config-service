package ai.traceable.certificate.management.config.service.v1.store;

import ai.traceable.certificate.management.config.service.v1.Certificate;
import ai.traceable.certificate.management.config.service.v1.CertificateFilter;
import ai.traceable.certificate.management.config.service.v1.CertificateStorageDetails;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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

  protected boolean matchesFilter(Certificate certificate, CertificateFilter filter) {
    // Filter by IDs if specified
    if (filter.getIdsCount() > 0 && !filter.getIdsList().contains(certificate.getId())) {
      return false;
    }

    // Filter by domain names if specified
    if (filter.getDomainNamesCount() > 0) {
      List<String> filterDomains = filter.getDomainNamesList();
      boolean matchesDomain =
          certificate.getMetadata().getDomainNamesList().stream()
              .anyMatch(
                  certDomain ->
                      // Check for exact match
                      filterDomains.contains(certDomain)
                          ||
                          // Check for wildcard domain match
                          (certDomain.startsWith("*.")
                              && filterDomains.stream()
                                  .anyMatch(
                                      filterDomain ->
                                          filterDomain.endsWith(certDomain.substring(1)))));
      if (!matchesDomain) {
        return false;
      }
    }

    // Filter by AWS-specific criteria if specified
    if (filter.hasAwsFilter()) {
      CertificateFilter.AwsFilter awsFilter = filter.getAwsFilter();

      // Filter by regions if specified
      if (awsFilter.getRegionsCount() > 0) {
        // Get all AWS regions from the certificate as a Set for efficient lookups
        Set<String> certificateRegions =
            certificate.getStorageList().stream()
                .filter(CertificateStorageDetails::hasAws)
                .map(storage -> storage.getAws().getRegion())
                .collect(Collectors.toSet());

        // Check if all filter regions are present in the certificate
        if (!certificateRegions.containsAll(awsFilter.getRegionsList())) {
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
