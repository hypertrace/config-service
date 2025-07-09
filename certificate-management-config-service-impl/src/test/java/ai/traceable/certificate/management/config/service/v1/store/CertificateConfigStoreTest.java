package ai.traceable.certificate.management.config.service.v1.store;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.certificate.management.config.service.v1.AwsStorageDetails;
import ai.traceable.certificate.management.config.service.v1.Certificate;
import ai.traceable.certificate.management.config.service.v1.CertificateFilter;
import ai.traceable.certificate.management.config.service.v1.CertificateMetadata;
import ai.traceable.certificate.management.config.service.v1.CertificateStorageDetails;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

class CertificateConfigStoreTest {
  @Mock private ConfigServiceBlockingStub configServiceBlockingStub;
  @Mock private ConfigChangeEventGenerator configChangeEventGenerator;

  @Test
  void testWildcardDomainFiltering() {
    // Create a filter with specific domains
    CertificateFilter filter =
        CertificateFilter.newBuilder().addDomainNames("app.traceable.ai").build();

    CertificateConfigStore store =
        new CertificateConfigStore(configServiceBlockingStub, configChangeEventGenerator);

    // Test wildcard domain matching
    Certificate wildcardCertificate =
        Certificate.newBuilder()
            .setId("cert-1")
            .setMetadata(
                CertificateMetadata.newBuilder().addDomainNames("*.traceable.ai") // Wildcard domain
                )
            .build();

    assertTrue(store.matchesFilter(wildcardCertificate, filter));

    // Test exact domain matching
    Certificate exactMatchCertificate =
        Certificate.newBuilder()
            .setId("cert-2")
            .setMetadata(
                CertificateMetadata.newBuilder()
                    .addDomainNames("app.traceable.ai") // Exact match domain
                    .addDomainNames("other.domain.com") // Non-matching domain
                )
            .build();

    assertTrue(store.matchesFilter(exactMatchCertificate, filter));

    // Test non-matching domain
    Certificate nonMatchingCertificate =
        Certificate.newBuilder()
            .setId("cert-3")
            .setMetadata(
                CertificateMetadata.newBuilder()
                    .addDomainNames("*.split.com") // Non-matching domain
                )
            .build();

    // Non-matching domain should not match
    assertFalse(
        store.matchesFilter(nonMatchingCertificate, filter),
        "Domain *.split.com should not match any filter domains");
  }

  @Test
  void testRegionFilteringWithAndCondition() {
    // Create the store
    CertificateConfigStore store =
        new CertificateConfigStore(configServiceBlockingStub, configChangeEventGenerator);

    // Create a certificate with storage in us-east-1 and us-west-1
    Certificate multiRegionCertificate =
        Certificate.newBuilder()
            .setId("cert-1")
            .setMetadata(CertificateMetadata.newBuilder().addDomainNames("example.com"))
            .addStorage(
                CertificateStorageDetails.newBuilder()
                    .setAws(AwsStorageDetails.newBuilder().setArn("abc012").setRegion("us-east-1")))
            .addStorage(
                CertificateStorageDetails.newBuilder()
                    .setAws(AwsStorageDetails.newBuilder().setArn("asf012").setRegion("us-west-1")))
            .build();

    // Create a certificate with storage in us-east-1 only
    Certificate singleRegionCertificate =
        Certificate.newBuilder()
            .setId("cert-2")
            .setMetadata(CertificateMetadata.newBuilder().addDomainNames("example.com"))
            .addStorage(
                CertificateStorageDetails.newBuilder()
                    .setAws(AwsStorageDetails.newBuilder().setArn("gh").setRegion("us-east-1")))
            .build();

    // Test filter with a single region that both certificates have
    CertificateFilter singleRegionFilter =
        CertificateFilter.newBuilder()
            .setAwsFilter(CertificateFilter.AwsFilter.newBuilder().addRegions("us-east-1"))
            .build();

    // Both certificates should match the single region filter
    assertTrue(store.matchesFilter(multiRegionCertificate, singleRegionFilter));
    assertTrue(store.matchesFilter(singleRegionCertificate, singleRegionFilter));

    // Test filter with multiple regions (AND condition)
    CertificateFilter multiRegionFilter =
        CertificateFilter.newBuilder()
            .setAwsFilter(
                CertificateFilter.AwsFilter.newBuilder()
                    .addRegions("us-east-1")
                    .addRegions("us-west-1"))
            .build();

    // Only the certificate with both regions should match
    assertTrue(store.matchesFilter(multiRegionCertificate, multiRegionFilter));
    assertFalse(store.matchesFilter(singleRegionCertificate, multiRegionFilter));

    // Certificate without region filter will match anything
    assertTrue(store.matchesFilter(multiRegionCertificate, CertificateFilter.newBuilder().build()));
  }
}
