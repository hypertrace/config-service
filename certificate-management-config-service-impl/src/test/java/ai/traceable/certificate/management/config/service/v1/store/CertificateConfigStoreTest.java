package ai.traceable.certificate.management.config.service.v1.store;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.certificate.management.config.service.v1.AwsStorageDetails;
import ai.traceable.certificate.management.config.service.v1.Certificate;
import ai.traceable.certificate.management.config.service.v1.CertificateFilter;
import ai.traceable.certificate.management.config.service.v1.CertificateMetadata;
import ai.traceable.certificate.management.config.service.v1.CertificateStorageDetails;
import ai.traceable.certificate.management.config.service.v1.CertificateType;
import com.google.protobuf.Value;
import io.grpc.StatusRuntimeException;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class CertificateConfigStoreTest {
  @Mock private ConfigServiceBlockingStub configServiceBlockingStub;
  @Mock private ConfigChangeEventGenerator configChangeEventGenerator;
  @Mock private RequestContext requestContext;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
  }

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

    // Create a filter with multiple regions (AND condition)
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

  @Test
  void testRegionFilteringWithExplicitAllCondition() {
    CertificateConfigStore store =
        new CertificateConfigStore(configServiceBlockingStub, configChangeEventGenerator);

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

    Certificate singleRegionCertificate =
        Certificate.newBuilder()
            .setId("cert-2")
            .setMetadata(CertificateMetadata.newBuilder().addDomainNames("example.com"))
            .addStorage(
                CertificateStorageDetails.newBuilder()
                    .setAws(AwsStorageDetails.newBuilder().setArn("gh").setRegion("us-east-1")))
            .build();

    CertificateFilter explicitAllFilter =
        CertificateFilter.newBuilder()
            .setAwsFilter(
                CertificateFilter.AwsFilter.newBuilder()
                    .addRegions("us-east-1")
                    .addRegions("us-west-1")
                    .setRegionMatchType(
                        CertificateFilter.AwsFilter.RegionMatchType.REGION_MATCH_TYPE_ALL))
            .build();

    assertTrue(store.matchesFilter(multiRegionCertificate, explicitAllFilter));
    assertFalse(store.matchesFilter(singleRegionCertificate, explicitAllFilter));
  }

  @Test
  void testRegionFilteringWithExplicitUnspecifiedDefaultsToAll() {
    CertificateConfigStore store =
        new CertificateConfigStore(configServiceBlockingStub, configChangeEventGenerator);

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

    Certificate singleRegionCertificate =
        Certificate.newBuilder()
            .setId("cert-2")
            .setMetadata(CertificateMetadata.newBuilder().addDomainNames("example.com"))
            .addStorage(
                CertificateStorageDetails.newBuilder()
                    .setAws(AwsStorageDetails.newBuilder().setArn("gh").setRegion("us-east-1")))
            .build();

    CertificateFilter explicitUnspecifiedFilter =
        CertificateFilter.newBuilder()
            .setAwsFilter(
                CertificateFilter.AwsFilter.newBuilder()
                    .addRegions("us-east-1")
                    .addRegions("us-west-1")
                    .setRegionMatchType(
                        CertificateFilter.AwsFilter.RegionMatchType.REGION_MATCH_TYPE_UNSPECIFIED))
            .build();

    assertTrue(store.matchesFilter(multiRegionCertificate, explicitUnspecifiedFilter));
    assertFalse(store.matchesFilter(singleRegionCertificate, explicitUnspecifiedFilter));
  }

  @Test
  void testRegionFilteringWithOrCondition() {
    CertificateConfigStore store =
        new CertificateConfigStore(configServiceBlockingStub, configChangeEventGenerator);

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

    CertificateFilter orRegionFilter =
        CertificateFilter.newBuilder()
            .setAwsFilter(
                CertificateFilter.AwsFilter.newBuilder()
                    .addRegions("us-west-1")
                    .addRegions("eu-central-1")
                    .setRegionMatchType(
                        CertificateFilter.AwsFilter.RegionMatchType.REGION_MATCH_TYPE_ANY))
            .build();

    assertTrue(store.matchesFilter(multiRegionCertificate, orRegionFilter));
  }

  @Test
  void testRegionFilteringWithOrCondition_NoMatchingRegions() {
    CertificateConfigStore store =
        new CertificateConfigStore(configServiceBlockingStub, configChangeEventGenerator);

    Certificate certificate =
        Certificate.newBuilder()
            .setId("cert-1")
            .setMetadata(CertificateMetadata.newBuilder().addDomainNames("example.com"))
            .addStorage(
                CertificateStorageDetails.newBuilder()
                    .setAws(AwsStorageDetails.newBuilder().setArn("abc012").setRegion("us-east-1")))
            .build();

    CertificateFilter orRegionFilterNoMatch =
        CertificateFilter.newBuilder()
            .setAwsFilter(
                CertificateFilter.AwsFilter.newBuilder()
                    .addRegions("eu-central-1")
                    .addRegions("ap-south-1")
                    .setRegionMatchType(
                        CertificateFilter.AwsFilter.RegionMatchType.REGION_MATCH_TYPE_ANY))
            .build();

    assertFalse(store.matchesFilter(certificate, orRegionFilterNoMatch));
  }

  @Test
  void testLabelKeyFilteringAnyMatch() {
    CertificateConfigStore store =
        new CertificateConfigStore(configServiceBlockingStub, configChangeEventGenerator);

    Certificate certificate =
        Certificate.newBuilder()
            .setId("cert-1")
            .setMetadata(
                CertificateMetadata.newBuilder()
                    .addDomainNames("example.com")
                    .putLabels("k1", "v1"))
            .build();

    CertificateFilter anyMatchFilter =
        CertificateFilter.newBuilder().addLabelKeys("k1").addLabelKeys("k2").build();
    assertTrue(store.matchesFilter(certificate, anyMatchFilter));

    CertificateFilter noMatchFilter = CertificateFilter.newBuilder().addLabelKeys("k2").build();
    assertFalse(store.matchesFilter(certificate, noMatchFilter));
  }

  @Test
  void testCertificateTypeFiltering() {
    // Create the store
    CertificateConfigStore store =
        new CertificateConfigStore(configServiceBlockingStub, configChangeEventGenerator);

    // Create a hosted certificate
    Certificate hostedCertificate =
        Certificate.newBuilder()
            .setId("cert-hosted")
            .setMetadata(CertificateMetadata.newBuilder().addDomainNames("hosted.example.com"))
            .setCertificateType(CertificateType.CERTIFICATE_TYPE_HOSTED)
            .build();

    // Create a managed certificate
    Certificate managedCertificate =
        Certificate.newBuilder()
            .setId("cert-managed")
            .setMetadata(CertificateMetadata.newBuilder().addDomainNames("managed.example.com"))
            .setCertificateType(CertificateType.CERTIFICATE_TYPE_MANAGED)
            .build();

    // Create an AUTO certificate
    Certificate autoCertificate =
        Certificate.newBuilder()
            .setId("cert-auto")
            .setMetadata(CertificateMetadata.newBuilder().addDomainNames("auto.example.com"))
            .setCertificateType(CertificateType.CERTIFICATE_TYPE_AUTO)
            .build();

    // Create a certificate with unspecified type
    Certificate unspecifiedCertificate =
        Certificate.newBuilder()
            .setId("cert-unspecified")
            .setMetadata(CertificateMetadata.newBuilder().addDomainNames("unspecified.example.com"))
            .setCertificateType(CertificateType.CERTIFICATE_TYPE_UNSPECIFIED)
            .build();

    // Test filter for hosted certificates
    CertificateFilter hostedFilter =
        CertificateFilter.newBuilder()
            .setCertificateType(CertificateType.CERTIFICATE_TYPE_HOSTED)
            .build();

    assertTrue(store.matchesFilter(hostedCertificate, hostedFilter));
    assertFalse(store.matchesFilter(managedCertificate, hostedFilter));
    assertFalse(store.matchesFilter(autoCertificate, hostedFilter));
    assertFalse(store.matchesFilter(unspecifiedCertificate, hostedFilter));

    // Test filter for managed certificates
    CertificateFilter managedFilter =
        CertificateFilter.newBuilder()
            .setCertificateType(CertificateType.CERTIFICATE_TYPE_MANAGED)
            .build();

    assertFalse(store.matchesFilter(hostedCertificate, managedFilter));
    assertTrue(store.matchesFilter(managedCertificate, managedFilter));
    assertFalse(store.matchesFilter(autoCertificate, managedFilter));
    assertFalse(store.matchesFilter(unspecifiedCertificate, managedFilter));

    // Test filter for AUTO certificates
    CertificateFilter autoFilter =
        CertificateFilter.newBuilder()
            .setCertificateType(CertificateType.CERTIFICATE_TYPE_AUTO)
            .build();

    assertFalse(store.matchesFilter(hostedCertificate, autoFilter));
    assertFalse(store.matchesFilter(managedCertificate, autoFilter));
    assertTrue(store.matchesFilter(autoCertificate, autoFilter));
    assertFalse(store.matchesFilter(unspecifiedCertificate, autoFilter));

    // Test filter with unspecified type (should match all certificates)
    CertificateFilter unspecifiedFilter =
        CertificateFilter.newBuilder()
            .setCertificateType(CertificateType.CERTIFICATE_TYPE_UNSPECIFIED)
            .build();

    assertTrue(store.matchesFilter(hostedCertificate, unspecifiedFilter));
    assertTrue(store.matchesFilter(managedCertificate, unspecifiedFilter));
    assertTrue(store.matchesFilter(autoCertificate, unspecifiedFilter));
    assertTrue(store.matchesFilter(unspecifiedCertificate, unspecifiedFilter));
  }

  @Test
  void testUpdateCertificateSilently_Success() {
    CertificateConfigStore store =
        spy(new CertificateConfigStore(configServiceBlockingStub, configChangeEventGenerator));
    Certificate certificate = createTestCertificate("cert-123");

    doAnswer(invocation -> invocation.getArgument(0, java.util.concurrent.Callable.class).call())
        .when(requestContext)
        .call(any());
    when(configServiceBlockingStub.withDeadline(any())).thenReturn(configServiceBlockingStub);
    when(configServiceBlockingStub.upsertConfig(any(UpsertConfigRequest.class)))
        .thenAnswer(
            invocation -> {
              UpsertConfigRequest request = invocation.getArgument(0);
              return UpsertConfigResponse.newBuilder().setConfig(request.getConfig()).build();
            });

    Certificate result = store.updateCertificateSilently(requestContext, certificate);

    assertNotNull(result);
    verify(configServiceBlockingStub).upsertConfig(any(UpsertConfigRequest.class));
  }

  @Test
  void testUpdateCertificateSilently_ThrowsExceptionOnEmptyResponse() {
    CertificateConfigStore store =
        spy(new CertificateConfigStore(configServiceBlockingStub, configChangeEventGenerator));
    Certificate certificate = createTestCertificate("cert-123");
    Value emptyValue = Value.newBuilder().build();
    UpsertConfigResponse response = UpsertConfigResponse.newBuilder().setConfig(emptyValue).build();

    doAnswer(invocation -> invocation.getArgument(0, java.util.concurrent.Callable.class).call())
        .when(requestContext)
        .call(any());
    when(configServiceBlockingStub.withDeadline(any())).thenReturn(configServiceBlockingStub);
    when(configServiceBlockingStub.upsertConfig(any(UpsertConfigRequest.class)))
        .thenReturn(response);

    assertThrows(
        StatusRuntimeException.class,
        () -> store.updateCertificateSilently(requestContext, certificate));
  }

  private Certificate createTestCertificate(String id) {
    return Certificate.newBuilder()
        .setId(id)
        .setName("Test Certificate")
        .setMetadata(
            CertificateMetadata.newBuilder()
                .addDomainNames("example.com")
                .setKeyAlgorithm("RSA-2048")
                .build())
        .addStorage(
            CertificateStorageDetails.newBuilder()
                .setAws(
                    AwsStorageDetails.newBuilder()
                        .setArn("arn:aws:acm:us-east-1:123456789012:certificate/test")
                        .setRegion("us-east-1")
                        .build())
                .build())
        .build();
  }
}
