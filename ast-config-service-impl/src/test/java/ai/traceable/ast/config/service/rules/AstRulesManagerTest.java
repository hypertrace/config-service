package ai.traceable.ast.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import ai.traceable.ast.config.service.configs.AstConfigServiceConfig;
import ai.traceable.ast.config.service.v1.CustomerDefinedTagsMap;
import ai.traceable.ast.config.service.v1.DeleteVulnerabilityMetadataOverridesConfigRequest;
import ai.traceable.ast.config.service.v1.EditVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetAllVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.IdentifyingAttributes;
import ai.traceable.ast.config.service.v1.ScanPurgeConfig;
import ai.traceable.ast.config.service.v1.TagValue;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.VulnerabilityMetadataOverrides;
import com.google.protobuf.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AstRulesManagerTest {
  private MockGenericConfigService mockConfigService;
  private RulesManager rulesManager;
  private RequestContext requestContext;
  private ConfigChangeEventGenerator mockConfigChangeEventGenerator;
  private AstConfigServiceConfig mockAstConfigServiceConfig;

  @BeforeEach
  void setup() {
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    mockAstConfigServiceConfig = mock(AstConfigServiceConfig.class);
    mockConfigService.start();
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());

    ScanPurgeConfigStore scanPurgeConfigStore = new ScanPurgeConfigStore(configServiceBlockingStub);
    AstFeatureConfigStore astFeatureConfigStore =
        new AstFeatureConfigStore(configServiceBlockingStub, mockConfigChangeEventGenerator);
    VulnerabilityMetadataOverridesStore vulnerabilityMetadataOverridesStore =
        new VulnerabilityMetadataOverridesStore(
            configServiceBlockingStub, mockConfigChangeEventGenerator);
    rulesManager =
        new AstRulesManager(
            scanPurgeConfigStore,
            vulnerabilityMetadataOverridesStore,
            astFeatureConfigStore,
            mockAstConfigServiceConfig);
    requestContext = RequestContext.forTenantId("default-tenant");
  }

  @AfterEach
  void teardown() {
    mockConfigService.shutdown();
  }

  @Test
  void testUpdateScanPurgeConfig() {
    ScanPurgeConfig purgeConfig =
        ScanPurgeConfig.newBuilder()
            .setPurgeDuration(Duration.newBuilder().setSeconds(123))
            .build();

    UpdateScanPurgeConfigRequest request =
        UpdateScanPurgeConfigRequest.newBuilder().setPurgeConfig(purgeConfig).build();

    ScanPurgeConfig updatedPurgeConfig =
        rulesManager.updateScanPurgeConfig(requestContext, request);
    assertEquals(
        purgeConfig.getPurgeDuration().getSeconds(),
        updatedPurgeConfig.getPurgeDuration().getSeconds());
  }

  @Test
  void testGetScanPurgeConfig() {
    ScanPurgeConfig purgeConfig =
        ScanPurgeConfig.newBuilder()
            .setPurgeDuration(Duration.newBuilder().setSeconds(123))
            .build();

    UpdateScanPurgeConfigRequest request =
        UpdateScanPurgeConfigRequest.newBuilder().setPurgeConfig(purgeConfig).build();

    rulesManager.updateScanPurgeConfig(requestContext, request);
    ScanPurgeConfig returnedPurgeConfig = rulesManager.getScanPurgeConfig(requestContext).get();

    assertEquals(
        purgeConfig.getPurgeDuration().getSeconds(),
        returnedPurgeConfig.getPurgeDuration().getSeconds());
  }

  @Test
  void testUpdateVulnerabilityMetadataConfig() {
    IdentifyingAttributes identifyingAttributes =
        IdentifyingAttributes.newBuilder()
            .setCategory("category")
            .setSubcategory("subcategory")
            .setMetadataId("metadataId")
            .build();
    VulnerabilityMetadataOverrides vulnerabilityMetadata =
        VulnerabilityMetadataOverrides.newBuilder()
            .setIdentifyingAttributes(identifyingAttributes)
            .setCvssScore(7.7)
            .setCustomerDefinedTags(
                CustomerDefinedTagsMap.newBuilder()
                    .putAllCustomerDefinedTags(
                        Map.of(
                            "TAG_NAME_1",
                            TagValue.newBuilder()
                                .addAllValue(List.of("VALUE1", "VALUE2", "VALUE3"))
                                .build(),
                            "TAG_NAME_2",
                            TagValue.newBuilder()
                                .addAllValue(List.of("VALUE4", "VALUE5"))
                                .build())))
            .build();
    EditVulnerabilityMetadataOverridesRequest request =
        EditVulnerabilityMetadataOverridesRequest.newBuilder()
            .setVulnerabilityMetadataOverrides(vulnerabilityMetadata)
            .build();

    VulnerabilityMetadataOverrides returnedVulnerabilityMetadataOverrides =
        rulesManager.updateVulnerabilityMetadataOverridesConfig(requestContext, request);
    // Check if being stored.
    assertEquals(vulnerabilityMetadata, returnedVulnerabilityMetadataOverrides);

    VulnerabilityMetadataOverrides vulnerabilityMetadataUpdates =
        VulnerabilityMetadataOverrides.newBuilder()
            .setIdentifyingAttributes(identifyingAttributes)
            .setCustomerDefinedTags(
                CustomerDefinedTagsMap.newBuilder()
                    .putAllCustomerDefinedTags(
                        Map.of(
                            "TAG_NAME_1",
                            TagValue.newBuilder().addAllValue(List.of("VALUE1", "VALUE2")).build(),
                            "TAG_NAME_2",
                            TagValue.newBuilder()
                                .addAllValue(List.of("VALUE4", "VALUE5"))
                                .build())))
            .build();
    request =
        EditVulnerabilityMetadataOverridesRequest.newBuilder()
            .setVulnerabilityMetadataOverrides(vulnerabilityMetadataUpdates)
            .build();

    returnedVulnerabilityMetadataOverrides =
        rulesManager.updateVulnerabilityMetadataOverridesConfig(requestContext, request);
    // Check if updated
    assertEquals(
        VulnerabilityMetadataOverrides.newBuilder()
            .setIdentifyingAttributes(identifyingAttributes)
            .setCvssScore(7.7)
            .setCustomerDefinedTags(
                CustomerDefinedTagsMap.newBuilder()
                    .putAllCustomerDefinedTags(
                        Map.of(
                            "TAG_NAME_1",
                            TagValue.newBuilder().addAllValue(List.of("VALUE1", "VALUE2")).build(),
                            "TAG_NAME_2",
                            TagValue.newBuilder()
                                .addAllValue(List.of("VALUE4", "VALUE5"))
                                .build())))
            .build(),
        returnedVulnerabilityMetadataOverrides);
  }

  @Test
  void testDeleteVulnerabilityMetadataOverridesConfig() {
    IdentifyingAttributes identifyingAttributes =
        IdentifyingAttributes.newBuilder()
            .setCategory("category")
            .setSubcategory("subcategory")
            .setMetadataId("metadataId")
            .build();
    String metadataId = "metadataId";
    VulnerabilityMetadataOverrides vulnerabilityMetadata =
        VulnerabilityMetadataOverrides.newBuilder()
            .setIdentifyingAttributes(identifyingAttributes)
            .setCvssScore(7.7)
            .setCustomerDefinedTags(
                CustomerDefinedTagsMap.newBuilder()
                    .putAllCustomerDefinedTags(
                        Map.of(
                            "TAG_NAME_1",
                            TagValue.newBuilder()
                                .addAllValue(List.of("VALUE1", "VALUE2", "VALUE3"))
                                .build(),
                            "TAG_NAME_2",
                            TagValue.newBuilder()
                                .addAllValue(List.of("VALUE4", "VALUE5"))
                                .build())))
            .build();
    EditVulnerabilityMetadataOverridesRequest request =
        EditVulnerabilityMetadataOverridesRequest.newBuilder()
            .setVulnerabilityMetadataOverrides(vulnerabilityMetadata)
            .build();

    VulnerabilityMetadataOverrides returnedVulnerabilityMetadataOverrides =
        rulesManager.updateVulnerabilityMetadataOverridesConfig(requestContext, request);
    // Check if being stored.
    assertEquals(vulnerabilityMetadata, returnedVulnerabilityMetadataOverrides);

    DeleteVulnerabilityMetadataOverridesConfigRequest
        deleteVulnerabilityMetadataOverridesConfigRequest =
            DeleteVulnerabilityMetadataOverridesConfigRequest.newBuilder()
                .setMetadataId("metadataId")
                .build();
    Optional<VulnerabilityMetadataOverrides> deletedVulnerabilityMetadataOverrides =
        rulesManager.deleteVulnerabilityMetadataOverridesConfig(
            requestContext, deleteVulnerabilityMetadataOverridesConfigRequest);

    // Check if deleted
    assertTrue(deletedVulnerabilityMetadataOverrides.isPresent());
    assertEquals(deletedVulnerabilityMetadataOverrides.get(), vulnerabilityMetadata);

    VulnerabilityMetadataOverrides vulnerabilityMetadataUpdates =
        VulnerabilityMetadataOverrides.newBuilder()
            .setIdentifyingAttributes(identifyingAttributes)
            .setCustomerDefinedTags(
                CustomerDefinedTagsMap.newBuilder()
                    .putAllCustomerDefinedTags(
                        Map.of(
                            "TAG_NAME_1",
                            TagValue.newBuilder().addAllValue(List.of("VALUE1", "VALUE2")).build(),
                            "TAG_NAME_2",
                            TagValue.newBuilder()
                                .addAllValue(List.of("VALUE4", "VALUE5"))
                                .build())))
            .build();
    request =
        EditVulnerabilityMetadataOverridesRequest.newBuilder()
            .setVulnerabilityMetadataOverrides(vulnerabilityMetadataUpdates)
            .build();

    returnedVulnerabilityMetadataOverrides =
        rulesManager.updateVulnerabilityMetadataOverridesConfig(requestContext, request);
    // Check if being stored newly.
    assertEquals(
        VulnerabilityMetadataOverrides.newBuilder()
            .setIdentifyingAttributes(identifyingAttributes)
            .setCustomerDefinedTags(
                CustomerDefinedTagsMap.newBuilder()
                    .putAllCustomerDefinedTags(
                        Map.of(
                            "TAG_NAME_1",
                            TagValue.newBuilder().addAllValue(List.of("VALUE1", "VALUE2")).build(),
                            "TAG_NAME_2",
                            TagValue.newBuilder()
                                .addAllValue(List.of("VALUE4", "VALUE5"))
                                .build())))
            .build(),
        returnedVulnerabilityMetadataOverrides);
  }

  @Test
  void testGetVulnerabilityMetadataOverrides() {
    IdentifyingAttributes identifyingAttributes =
        IdentifyingAttributes.newBuilder()
            .setCategory("category")
            .setSubcategory("subcategory")
            .setMetadataId("metadataId")
            .build();
    String metadataId = "metadataId";
    VulnerabilityMetadataOverrides vulnerabilityMetadata =
        VulnerabilityMetadataOverrides.newBuilder()
            .setIdentifyingAttributes(identifyingAttributes)
            .setCvssScore(7.7)
            .setCustomerDefinedTags(
                CustomerDefinedTagsMap.newBuilder()
                    .putAllCustomerDefinedTags(
                        Map.of(
                            "TAG_NAME_1",
                            TagValue.newBuilder()
                                .addAllValue(List.of("VALUE1", "VALUE2", "VALUE3"))
                                .build(),
                            "TAG_NAME_2",
                            TagValue.newBuilder()
                                .addAllValue(List.of("VALUE4", "VALUE5"))
                                .build())))
            .build();
    EditVulnerabilityMetadataOverridesRequest request =
        EditVulnerabilityMetadataOverridesRequest.newBuilder()
            .setVulnerabilityMetadataOverrides(vulnerabilityMetadata)
            .build();

    VulnerabilityMetadataOverrides returnedVulnerabilityMetadataOverrides =
        rulesManager.updateVulnerabilityMetadataOverridesConfig(requestContext, request);
    // Check if being stored.
    assertEquals(vulnerabilityMetadata, returnedVulnerabilityMetadataOverrides);

    GetVulnerabilityMetadataOverridesRequest getVulnerabilityMetadataOverridesRequest =
        GetVulnerabilityMetadataOverridesRequest.newBuilder().setMetadataId(metadataId).build();
    Optional<VulnerabilityMetadataOverrides> responseVulnerabilityMetadataOverrides =
        rulesManager.getVulnerabilityMetadataOverridesConfig(
            requestContext, getVulnerabilityMetadataOverridesRequest);

    // Check if present
    assertTrue(responseVulnerabilityMetadataOverrides.isPresent());
    assertEquals(responseVulnerabilityMetadataOverrides.get(), vulnerabilityMetadata);

    DeleteVulnerabilityMetadataOverridesConfigRequest
        deleteVulnerabilityMetadataOverridesConfigRequest =
            DeleteVulnerabilityMetadataOverridesConfigRequest.newBuilder()
                .setMetadataId(metadataId)
                .build();
    Optional<VulnerabilityMetadataOverrides> deletedVulnerabilityMetadataOverrides =
        rulesManager.deleteVulnerabilityMetadataOverridesConfig(
            requestContext, deleteVulnerabilityMetadataOverridesConfigRequest);

    // Check if deleted
    assertTrue(deletedVulnerabilityMetadataOverrides.isPresent());
    assertEquals(deletedVulnerabilityMetadataOverrides.get(), vulnerabilityMetadata);

    getVulnerabilityMetadataOverridesRequest =
        GetVulnerabilityMetadataOverridesRequest.newBuilder().setMetadataId(metadataId).build();
    responseVulnerabilityMetadataOverrides =
        rulesManager.getVulnerabilityMetadataOverridesConfig(
            requestContext, getVulnerabilityMetadataOverridesRequest);

    // Check if not present
    assertFalse(responseVulnerabilityMetadataOverrides.isPresent());
  }

  @Test
  void testGetAllVulnerabilityMetadataOverrides() {
    IdentifyingAttributes identifyingAttributes =
        IdentifyingAttributes.newBuilder()
            .setCategory("category")
            .setSubcategory("subcategory")
            .setMetadataId("metadataId")
            .build();
    String metadataId = "metadataId";
    VulnerabilityMetadataOverrides vulnerabilityMetadata =
        VulnerabilityMetadataOverrides.newBuilder()
            .setIdentifyingAttributes(identifyingAttributes)
            .setCvssScore(7.7)
            .setCustomerDefinedTags(
                CustomerDefinedTagsMap.newBuilder()
                    .putAllCustomerDefinedTags(
                        Map.of(
                            "TAG_NAME_1",
                            TagValue.newBuilder()
                                .addAllValue(List.of("VALUE1", "VALUE2", "VALUE3"))
                                .build(),
                            "TAG_NAME_2",
                            TagValue.newBuilder()
                                .addAllValue(List.of("VALUE4", "VALUE5"))
                                .build())))
            .build();
    EditVulnerabilityMetadataOverridesRequest request =
        EditVulnerabilityMetadataOverridesRequest.newBuilder()
            .setVulnerabilityMetadataOverrides(vulnerabilityMetadata)
            .build();

    VulnerabilityMetadataOverrides returnedVulnerabilityMetadataOverrides =
        rulesManager.updateVulnerabilityMetadataOverridesConfig(requestContext, request);
    // Check if being stored.
    assertEquals(vulnerabilityMetadata, returnedVulnerabilityMetadataOverrides);

    GetAllVulnerabilityMetadataOverridesRequest getAllVulnerabilityMetadataOverridesRequest =
        GetAllVulnerabilityMetadataOverridesRequest.newBuilder().build();
    List<VulnerabilityMetadataOverrides> responseVulnerabilityMetadataOverrides =
        rulesManager.getAllVulnerabilityMetadataOverridesConfig(
            requestContext, getAllVulnerabilityMetadataOverridesRequest);

    // Check if present
    assertFalse(responseVulnerabilityMetadataOverrides.isEmpty());
    assertEquals(responseVulnerabilityMetadataOverrides, List.of(vulnerabilityMetadata));

    DeleteVulnerabilityMetadataOverridesConfigRequest
        deleteVulnerabilityMetadataOverridesConfigRequest =
            DeleteVulnerabilityMetadataOverridesConfigRequest.newBuilder()
                .setMetadataId(metadataId)
                .build();
    Optional<VulnerabilityMetadataOverrides> deletedVulnerabilityMetadataOverrides =
        rulesManager.deleteVulnerabilityMetadataOverridesConfig(
            requestContext, deleteVulnerabilityMetadataOverridesConfigRequest);

    // Check if deleted
    assertTrue(deletedVulnerabilityMetadataOverrides.isPresent());
    assertEquals(deletedVulnerabilityMetadataOverrides.get(), vulnerabilityMetadata);

    getAllVulnerabilityMetadataOverridesRequest =
        GetAllVulnerabilityMetadataOverridesRequest.newBuilder().build();
    responseVulnerabilityMetadataOverrides =
        rulesManager.getAllVulnerabilityMetadataOverridesConfig(
            requestContext, getAllVulnerabilityMetadataOverridesRequest);

    // Check if not present
    assertTrue(responseVulnerabilityMetadataOverrides.isEmpty());
  }
}
