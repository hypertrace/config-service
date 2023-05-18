package ai.traceable.ast.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import ai.traceable.ast.config.service.v1.DeleteVulnerabilityMetadataOverridesConfigRequest;
import ai.traceable.ast.config.service.v1.EditVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetAllVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.GetVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.IdentifyingAttributes;
import ai.traceable.ast.config.service.v1.ScanPurgeConfig;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.VulnerabilityMetadataOverrides;
import com.google.protobuf.Duration;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class AstRulesValidatorTest {
  private static final String TENANT_ID = "default-tenant";
  private AstRulesValidator rulesValidator = new AstRulesValidator();
  private RequestContext mockRequestContext = Mockito.mock(RequestContext.class);

  @Test
  void validateUpdateScanPurgeConfigRequest() {

    // invalid request context
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext, UpdateScanPurgeConfigRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TENANT_ID));

    // no purge config
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext, UpdateScanPurgeConfigRequest.getDefaultInstance()));

    // no purge duration
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext,
                UpdateScanPurgeConfigRequest.newBuilder()
                    .setPurgeConfig(ScanPurgeConfig.getDefaultInstance())
                    .build()));

    // valid request
    assertDoesNotThrow(
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext,
                UpdateScanPurgeConfigRequest.newBuilder()
                    .setPurgeConfig(
                        ScanPurgeConfig.newBuilder()
                            .setPurgeDuration(Duration.newBuilder().setSeconds(1)))
                    .build()));
  }

  @Test
  void validateGetScanPurgeConfigRequest() {

    // invalid request context
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext, GetScanPurgeConfigRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TENANT_ID));

    // valid request
    assertDoesNotThrow(
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext, GetScanPurgeConfigRequest.getDefaultInstance()));
  }

  @Test
  void validateEditVulnerabilityMetadataOverridesRequest() {

    // invalid request context
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext,
                EditVulnerabilityMetadataOverridesRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TENANT_ID));

    // no identifying attributes
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext,
                EditVulnerabilityMetadataOverridesRequest.getDefaultInstance()));

    // no metadata id
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext,
                EditVulnerabilityMetadataOverridesRequest.newBuilder()
                    .setVulnerabilityMetadataOverrides(
                        VulnerabilityMetadataOverrides.newBuilder()
                            .setIdentifyingAttributes(
                                IdentifyingAttributes.newBuilder().setCategory("category").build())
                            .build())
                    .build()));

    // valid request
    assertDoesNotThrow(
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext,
                EditVulnerabilityMetadataOverridesRequest.newBuilder()
                    .setVulnerabilityMetadataOverrides(
                        VulnerabilityMetadataOverrides.newBuilder()
                            .setIdentifyingAttributes(
                                IdentifyingAttributes.newBuilder()
                                    .setMetadataId("metadata_id")
                                    .setCategory("category")
                                    .setSubcategory("sub_category")
                                    .build())
                            .build())
                    .build()));
  }

  @Test
  void validateDeleteVulnerabilityMetadataOverridesConfigRequest() {

    // invalid request context
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext,
                DeleteVulnerabilityMetadataOverridesConfigRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TENANT_ID));

    // no metadata id
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext,
                DeleteVulnerabilityMetadataOverridesConfigRequest.getDefaultInstance()));

    // valid request
    assertDoesNotThrow(
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext,
                DeleteVulnerabilityMetadataOverridesConfigRequest.newBuilder()
                    .setMetadataId("metadata_id")
                    .build()));
  }

  @Test
  void validateGetVulnerabilityMetadataOverridesRequest() {

    // invalid request context
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext, GetVulnerabilityMetadataOverridesRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TENANT_ID));

    // no metadata id
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext, GetVulnerabilityMetadataOverridesRequest.getDefaultInstance()));

    // valid request
    assertDoesNotThrow(
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext,
                GetVulnerabilityMetadataOverridesRequest.newBuilder()
                    .setMetadataId("metadata_id")
                    .build()));
  }

  @Test
  void validateGetAllVulnerabilityMetadataOverridesRequest() {

    // invalid request context
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext,
                GetAllVulnerabilityMetadataOverridesRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TENANT_ID));

    // valid request
    assertDoesNotThrow(
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext,
                GetAllVulnerabilityMetadataOverridesRequest.getDefaultInstance()));
  }
}
