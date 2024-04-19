package ai.traceable.ast.config.service.rules;

import static ai.traceable.ast.config.service.v1.VulnerabilitySeverity.VULNERABILITY_SEVERITY_HIGH;
import static ai.traceable.ast.config.service.v1.VulnerabilitySeverity.VULNERABILITY_SEVERITY_UNSPECIFIED;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import ai.traceable.ast.config.service.v1.AstEnabledConfig;
import ai.traceable.ast.config.service.v1.AstReplayConfig;
import ai.traceable.ast.config.service.v1.CodeSnippetDetails;
import ai.traceable.ast.config.service.v1.CodeSnippetType;
import ai.traceable.ast.config.service.v1.CreateCustomTestPlugin;
import ai.traceable.ast.config.service.v1.CreateCustomTestPluginRequest;
import ai.traceable.ast.config.service.v1.CustomerDefinedTagsMap;
import ai.traceable.ast.config.service.v1.DeleteCustomTestPluginRequest;
import ai.traceable.ast.config.service.v1.DeleteVulnerabilityMetadataOverridesConfigRequest;
import ai.traceable.ast.config.service.v1.EditVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetAllCustomTestPluginsRequest;
import ai.traceable.ast.config.service.v1.GetAllVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.GetVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.IdentifyingAttributes;
import ai.traceable.ast.config.service.v1.KeyValuePredicate;
import ai.traceable.ast.config.service.v1.KeyValuePredicateWithLocation;
import ai.traceable.ast.config.service.v1.Location;
import ai.traceable.ast.config.service.v1.RelationalOperator;
import ai.traceable.ast.config.service.v1.ScanPurgeConfig;
import ai.traceable.ast.config.service.v1.SpanFilters;
import ai.traceable.ast.config.service.v1.StringPredicate;
import ai.traceable.ast.config.service.v1.TagValue;
import ai.traceable.ast.config.service.v1.UpdateAstFeatureConfigRequest;
import ai.traceable.ast.config.service.v1.UpdateCustomTestPlugin;
import ai.traceable.ast.config.service.v1.UpdateCustomTestPluginRequest;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.VulnerabilityMetadataOverrides;
import com.google.protobuf.Duration;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class AstConfigServiceRequestValidatorImplTest {
  private static final String TENANT_ID = "default-tenant";
  private AstConfigServiceRequestValidatorImpl rulesValidator =
      new AstConfigServiceRequestValidatorImpl();
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

    // invalid severity
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
                            .setSeverity(VULNERABILITY_SEVERITY_UNSPECIFIED)
                            .build())
                    .build()));

    // invalid cvss vector string
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
                            .setCvssVectorString("")
                            .build())
                    .build()));

    // invalid estimated fix time
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
                            .setEstimatedFixTime(Duration.getDefaultInstance())
                            .build())
                    .build()));

    // invalid customer defined tags
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
                            .setCustomerDefinedTags(
                                CustomerDefinedTagsMap.newBuilder()
                                    .putCustomerDefinedTags("key1", TagValue.newBuilder().build()))
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
                            .setSeverity(VULNERABILITY_SEVERITY_HIGH)
                            .setCvssScore(9.6)
                            .setCvssVectorString("CVSS:3.1/AV:N/AC:H/PR:N/UI:N/S:U/C:H/I:H/A:L")
                            .setEstimatedFixTime(Duration.newBuilder().setSeconds(100L).build())
                            .setCustomerDefinedTags(
                                CustomerDefinedTagsMap.newBuilder()
                                    .putCustomerDefinedTags(
                                        "key1", TagValue.newBuilder().addValue("value1").build()))
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

  @Test
  void validateUpdateCustomTestPluginRequest() {

    // invalid request context
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext, UpdateCustomTestPluginRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TENANT_ID));

    // no code snippet details
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext,
                UpdateCustomTestPluginRequest.newBuilder()
                    .setUpdateCustomTestPlugin(
                        UpdateCustomTestPlugin.newBuilder()
                            .setId("test-id")
                            .setName("test-name")
                            .setCodeSnippetDetails(
                                CodeSnippetDetails.newBuilder()
                                    .setCodeSnippetType(
                                        CodeSnippetType.CODE_SNIPPET_TYPE_UNSPECIFIED)))
                    .build()));

    // valid request
    assertDoesNotThrow(
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext,
                UpdateCustomTestPluginRequest.newBuilder()
                    .setUpdateCustomTestPlugin(
                        UpdateCustomTestPlugin.newBuilder()
                            .setId("test-id")
                            .setName("test-name")
                            .setCodeSnippetDetails(
                                CodeSnippetDetails.newBuilder()
                                    .setCodeSnippet("dummy-code-snippet")
                                    .setCodeSnippetType(
                                        CodeSnippetType
                                            .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT)))
                    .build()));
  }

  @Test
  void validateCreateCustomTestPluginRequest() {

    // invalid request context
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext, UpdateCustomTestPluginRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TENANT_ID));

    // no code snippet details
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext,
                CreateCustomTestPluginRequest.newBuilder()
                    .setCreateCustomTestPlugin(
                        CreateCustomTestPlugin.newBuilder()
                            .setName("test-name")
                            .setCodeSnippetDetails(
                                CodeSnippetDetails.newBuilder()
                                    .setCodeSnippetType(
                                        CodeSnippetType.CODE_SNIPPET_TYPE_UNSPECIFIED)))
                    .build()));

    // valid request
    assertDoesNotThrow(
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext,
                CreateCustomTestPluginRequest.newBuilder()
                    .setCreateCustomTestPlugin(
                        CreateCustomTestPlugin.newBuilder()
                            .setName("test-name")
                            .setCodeSnippetDetails(
                                CodeSnippetDetails.newBuilder()
                                    .setCodeSnippet("dummy-code-snippet")
                                    .setCodeSnippetType(
                                        CodeSnippetType
                                            .CODE_SNIPPET_TYPE_VULNERABILITY_METADATA_INCLUDED_PYTHON_SCRIPT)))
                    .build()));
  }

  @Test
  void validateDeleteCustomTestPluginRequest() {

    // invalid request context
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext, DeleteCustomTestPluginRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TENANT_ID));

    // no id
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext, DeleteCustomTestPluginRequest.newBuilder().build()));

    // valid request
    assertDoesNotThrow(
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext,
                DeleteCustomTestPluginRequest.newBuilder().setId("id").build()));
  }

  @Test
  void validateGetAllCustomTestPluginsRequest() {

    // invalid request context
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext, GetAllCustomTestPluginsRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TENANT_ID));

    // valid request
    assertDoesNotThrow(
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext, GetAllCustomTestPluginsRequest.newBuilder().build()));
  }

  @Test
  @DisplayName("Invalid key regex request for Span Filters")
  void validateKeyRegexSpanFilters() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TENANT_ID));
    UpdateAstFeatureConfigRequest request =
        UpdateAstFeatureConfigRequest.newBuilder()
            .setEnvironmentId("env_id")
            .setEnabledConfig(
                AstEnabledConfig.newBuilder()
                    .setReplayConfig(
                        AstReplayConfig.newBuilder()
                            .setSpanFilters(
                                SpanFilters.newBuilder()
                                    .addAllConditions(
                                        List.of(
                                            KeyValuePredicateWithLocation.newBuilder()
                                                .setLocation(Location.LOCATION_REQUEST_HEADER)
                                                .setKeyValuePredicate(
                                                    KeyValuePredicate.newBuilder()
                                                        .setKeyPredicate(
                                                            StringPredicate.newBuilder()
                                                                .setOperator(
                                                                    RelationalOperator
                                                                        .RELATIONAL_OPERATOR_MATCHES_REGEX)
                                                                .setValue("*")
                                                                .build())
                                                        .build())
                                                .build()))
                                    .build())
                            .build())
                    .build())
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> rulesValidator.validateOrThrow(mockRequestContext, request));
  }

  @Test
  @DisplayName("Invalid value regex request for Span Filters")
  void validateValueRegexSpanFilters() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TENANT_ID));
    UpdateAstFeatureConfigRequest request =
        UpdateAstFeatureConfigRequest.newBuilder()
            .setEnvironmentId("env_id")
            .setEnabledConfig(
                AstEnabledConfig.newBuilder()
                    .setReplayConfig(
                        AstReplayConfig.newBuilder()
                            .setSpanFilters(
                                SpanFilters.newBuilder()
                                    .addAllConditions(
                                        List.of(
                                            KeyValuePredicateWithLocation.newBuilder()
                                                .setLocation(Location.LOCATION_REQUEST_HEADER)
                                                .setKeyValuePredicate(
                                                    KeyValuePredicate.newBuilder()
                                                        .setKeyPredicate(
                                                            StringPredicate.newBuilder()
                                                                .setOperator(
                                                                    RelationalOperator
                                                                        .RELATIONAL_OPERATOR_EQUALS)
                                                                .build())
                                                        .setValuePredicate(
                                                            StringPredicate.newBuilder()
                                                                .setOperator(
                                                                    RelationalOperator
                                                                        .RELATIONAL_OPERATOR_MATCHES_REGEX)
                                                                .setValue("*")
                                                                .build())
                                                        .build())
                                                .build()))
                                    .build())
                            .build())
                    .build())
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> rulesValidator.validateOrThrow(mockRequestContext, request));
  }

  @Test
  @DisplayName("Invalid location missing request for Span Filters")
  void validateLocationSpanFilters() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TENANT_ID));
    UpdateAstFeatureConfigRequest request =
        UpdateAstFeatureConfigRequest.newBuilder()
            .setEnvironmentId("env_id")
            .setEnabledConfig(
                AstEnabledConfig.newBuilder()
                    .setReplayConfig(
                        AstReplayConfig.newBuilder()
                            .setSpanFilters(
                                SpanFilters.newBuilder()
                                    .addAllConditions(
                                        List.of(
                                            KeyValuePredicateWithLocation.newBuilder()
                                                .setKeyValuePredicate(
                                                    KeyValuePredicate.newBuilder()
                                                        .setKeyPredicate(
                                                            StringPredicate.newBuilder()
                                                                .setOperator(
                                                                    RelationalOperator
                                                                        .RELATIONAL_OPERATOR_EQUALS)
                                                                .build())
                                                        .build())
                                                .build()))
                                    .build())
                            .build())
                    .build())
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> rulesValidator.validateOrThrow(mockRequestContext, request));
  }
}
