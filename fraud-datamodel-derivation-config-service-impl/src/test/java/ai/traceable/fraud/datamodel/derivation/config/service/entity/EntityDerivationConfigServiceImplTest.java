package ai.traceable.fraud.datamodel.derivation.config.service.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.DeleteEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityCategory;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigData;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigFilter;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigServiceGrpc;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EnvironmentScope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EventDerivationConfigDetails;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.ExtractionLocation;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.ExtractionLocationType;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigSummariesRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigSummariesResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.KeyMatchType;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.Scope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanBasedExtraction;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanProjection;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.UpdateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.UpdateEntityDerivationConfigResponse;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunctionInvocation;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationPipeline;
import com.google.protobuf.Value;
import io.grpc.StatusRuntimeException;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class EntityDerivationConfigServiceImplTest {

  private EntityDerivationConfigServiceGrpc.EntityDerivationConfigServiceBlockingStub serviceStub;
  private MockGenericConfigService mockGenericConfigService;

  @BeforeEach
  void beforeEach() {
    UuidGenerator uuidGenerator = new UuidGenerator();

    this.mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());

    ConfigChangeEventGenerator eventGenerator = Mockito.mock(ConfigChangeEventGenerator.class);
    EntityDerivationConfigServiceImpl service =
        getEntityDerivationConfigService(genericStub, eventGenerator, uuidGenerator);

    this.mockGenericConfigService.addService(service).start();

    this.serviceStub =
        EntityDerivationConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  private static EntityDerivationConfigServiceImpl getEntityDerivationConfigService(
      ConfigServiceBlockingStub genericStub,
      ConfigChangeEventGenerator eventGenerator,
      UuidGenerator uuidGenerator) {
    EntityDerivationConfigStore store =
        new EntityDerivationConfigStore(genericStub, eventGenerator);
    DefaultEntityDerivationProvider defaultEntityDerivationProvider =
        new DefaultEntityDerivationProvider();
    EntityDerivationConfigRequestValidator validator =
        new EntityDerivationConfigRequestValidator(defaultEntityDerivationProvider);
    EntityDerivationConfigStoreManager storeManager =
        new EntityDerivationConfigStoreManager(
            store, uuidGenerator, defaultEntityDerivationProvider);

    EntityDerivationConfigServiceImpl service =
        new EntityDerivationConfigServiceImpl(storeManager, validator);
    return service;
  }

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  @Test
  void testCreateEntityDerivationConfig_Success() {
    CreateEntityDerivationConfigRequest request =
        CreateEntityDerivationConfigRequest.newBuilder()
            .setData(createValidConfigData("User Email"))
            .build();

    CreateEntityDerivationConfigResponse response =
        RequestContext.forTenantId("test-tenant")
            .call(() -> serviceStub.createEntityDerivationConfig(request));

    assertNotNull(response);
    assertFalse(response.getEntityDerivationConfig().getId().isEmpty());
    assertEquals("user_email", response.getEntityDerivationConfig().getColumnName());
  }

  @Test
  void testCreateEntityDerivationConfig_ValidationFailure() {
    CreateEntityDerivationConfigRequest request =
        CreateEntityDerivationConfigRequest.newBuilder().build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                RequestContext.forTenantId("test-tenant")
                    .call(() -> serviceStub.createEntityDerivationConfig(request)));
    assertTrue(exception.getMessage().contains("Entity derivation configuration data is required"));
  }

  @Test
  void testUpdateEntityDerivationConfig_Success() {
    CreateEntityDerivationConfigResponse createResponse =
        RequestContext.forTenantId("test-tenant")
            .call(
                () ->
                    serviceStub.createEntityDerivationConfig(
                        CreateEntityDerivationConfigRequest.newBuilder()
                            .setData(createValidConfigData("Original Name"))
                            .build()));

    String configId = createResponse.getEntityDerivationConfig().getId();

    UpdateEntityDerivationConfigRequest updateRequest =
        UpdateEntityDerivationConfigRequest.newBuilder()
            .setId(configId)
            .setData(createValidConfigData("Updated Name"))
            .build();

    UpdateEntityDerivationConfigResponse updateResponse =
        RequestContext.forTenantId("test-tenant")
            .call(() -> serviceStub.updateEntityDerivationConfig(updateRequest));

    assertEquals("updated_name", updateResponse.getEntityDerivationConfig().getColumnName());
  }

  @Test
  void testUpdateEntityDerivationConfig_NotFound() {
    UpdateEntityDerivationConfigRequest request =
        UpdateEntityDerivationConfigRequest.newBuilder()
            .setId("non-existent-id")
            .setData(createValidConfigData("Test"))
            .build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                RequestContext.forTenantId("test-tenant")
                    .call(() -> serviceStub.updateEntityDerivationConfig(request)));
    assertTrue(exception.getMessage().contains("No entity derivation config found"));
  }

  @Test
  void testGetEntityDerivationConfigSummaries_Success() {
    RequestContext.forTenantId("test-tenant")
        .call(
            () ->
                serviceStub.createEntityDerivationConfig(
                    CreateEntityDerivationConfigRequest.newBuilder()
                        .setData(createValidConfigData("Entity One"))
                        .build()));

    GetEntityDerivationConfigSummariesRequest request =
        GetEntityDerivationConfigSummariesRequest.newBuilder()
            .setFilter(EntityDerivationConfigFilter.newBuilder().build())
            .build();

    GetEntityDerivationConfigSummariesResponse response =
        RequestContext.forTenantId("test-tenant")
            .call(() -> serviceStub.getEntityDerivationConfigSummaries(request));

    // Should include user-created entity plus all default entities
    assertTrue(response.getSummariesCount() > 0);
    assertTrue(
        response.getSummariesList().stream()
            .anyMatch(s -> s.getDisplayName().equals("Entity One")));
  }

  @Test
  void testGetEntityDerivationConfigs_Success() {
    RequestContext.forTenantId("test-tenant")
        .call(
            () ->
                serviceStub.createEntityDerivationConfig(
                    CreateEntityDerivationConfigRequest.newBuilder()
                        .setData(createValidConfigData("Test Entity"))
                        .build()));

    GetEntityDerivationConfigsRequest request =
        GetEntityDerivationConfigsRequest.newBuilder()
            .setFilter(EntityDerivationConfigFilter.newBuilder().build())
            .build();

    GetEntityDerivationConfigsResponse response =
        RequestContext.forTenantId("test-tenant")
            .call(() -> serviceStub.getEntityDerivationConfigs(request));

    // Should include user-created entity plus all default entities
    assertTrue(response.getEntityDerivationConfigsCount() > 0);
    assertTrue(
        response.getEntityDerivationConfigsList().stream()
            .anyMatch(e -> e.getData().getDisplayName().equals("Test Entity")));
  }

  @Test
  void testDeleteEntityDerivationConfig_Success() {
    CreateEntityDerivationConfigResponse createResponse =
        RequestContext.forTenantId("test-tenant")
            .call(
                () ->
                    serviceStub.createEntityDerivationConfig(
                        CreateEntityDerivationConfigRequest.newBuilder()
                            .setData(createValidConfigData("Test Entity"))
                            .build()));

    String configId = createResponse.getEntityDerivationConfig().getId();

    DeleteEntityDerivationConfigRequest deleteRequest =
        DeleteEntityDerivationConfigRequest.newBuilder()
            .setEntityDerivationConfigId(configId)
            .build();

    RequestContext.forTenantId("test-tenant")
        .call(() -> serviceStub.deleteEntityDerivationConfig(deleteRequest));

    UpdateEntityDerivationConfigRequest updateRequest =
        UpdateEntityDerivationConfigRequest.newBuilder()
            .setId(configId)
            .setData(createValidConfigData("Updated"))
            .build();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                RequestContext.forTenantId("test-tenant")
                    .call(() -> serviceStub.updateEntityDerivationConfig(updateRequest)));
    assertTrue(exception.getMessage().contains("No entity derivation config found"));
  }

  @Test
  void testFilterExcludesInternalEntities() {
    GetEntityDerivationConfigSummariesRequest request =
        GetEntityDerivationConfigSummariesRequest.newBuilder()
            .setFilter(EntityDerivationConfigFilter.newBuilder().setIncludeInternal(false).build())
            .build();

    GetEntityDerivationConfigSummariesResponse response =
        RequestContext.forTenantId("test-tenant")
            .call(() -> serviceStub.getEntityDerivationConfigSummaries(request));

    assertFalse(
        response.getSummariesList().stream()
            .anyMatch(s -> s.getId().equals("system_entity_customer_id")));
    assertFalse(
        response.getSummariesList().stream()
            .anyMatch(s -> s.getId().equals("system_entity_span_id")));
  }

  @Test
  void testFilterIncludesInternalEntities() {
    GetEntityDerivationConfigSummariesRequest request =
        GetEntityDerivationConfigSummariesRequest.newBuilder()
            .setFilter(EntityDerivationConfigFilter.newBuilder().setIncludeInternal(true).build())
            .build();

    GetEntityDerivationConfigSummariesResponse response =
        RequestContext.forTenantId("test-tenant")
            .call(() -> serviceStub.getEntityDerivationConfigSummaries(request));

    assertTrue(
        response.getSummariesList().stream()
            .anyMatch(s -> s.getId().equals("system_entity_customer_id")));
    assertTrue(
        response.getSummariesList().stream()
            .anyMatch(s -> s.getId().equals("system_entity_span_id")));
  }

  @Test
  void testCustomerCanOverrideRecommendedEntity() {
    // Create a custom entity with same ID as recommended entity to override it
    CreateEntityDerivationConfigRequest createRequest =
        CreateEntityDerivationConfigRequest.newBuilder()
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setDisplayName("Custom Email Override")
                    .setCategory(EntityCategory.ENTITY_CATEGORY_CUSTOM)
                    .setDisabled(false)
                    .setEventKind(
                        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_email"))
                    .setSpanProjection(
                        SpanProjection.newBuilder()
                            .addEventDerivationConfigs(
                                EventDerivationConfigDetails.newBuilder()
                                    .setName("Custom Email Extraction")))
                    .build())
            .build();

    CreateEntityDerivationConfigResponse createResponse =
        RequestContext.forTenantId("test-tenant")
            .call(() -> serviceStub.createEntityDerivationConfig(createRequest));

    // Override the recommended entity by updating with same ID
    String recommendedEntityId = "recommended_entity_email_address";
    UpdateEntityDerivationConfigRequest updateRequest =
        UpdateEntityDerivationConfigRequest.newBuilder()
            .setId(recommendedEntityId)
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setDisplayName("Customer Enabled Email")
                    .setCategory(EntityCategory.ENTITY_CATEGORY_RECOMMENDED)
                    .setDisabled(false)
                    .setEventKind(
                        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_email"))
                    .setSpanProjection(
                        SpanProjection.newBuilder()
                            .addEventDerivationConfigs(
                                EventDerivationConfigDetails.newBuilder()
                                    .setName("Customer Email Rule")))
                    .build())
            .build();

    // This should create/override the recommended entity in MongoDB
    RequestContext.forTenantId("test-tenant")
        .call(() -> serviceStub.updateEntityDerivationConfig(updateRequest));

    // Verify the overridden entity is returned with customer's modifications
    GetEntityDerivationConfigsRequest getRequest =
        GetEntityDerivationConfigsRequest.newBuilder()
            .addIds(recommendedEntityId)
            .setFilter(EntityDerivationConfigFilter.newBuilder().setIncludeDisabled(false).build())
            .build();

    GetEntityDerivationConfigsResponse response =
        RequestContext.forTenantId("test-tenant")
            .call(() -> serviceStub.getEntityDerivationConfigs(getRequest));

    assertEquals(1, response.getEntityDerivationConfigsCount());
    EntityDerivationConfig overriddenEntity = response.getEntityDerivationConfigs(0);
    assertEquals(recommendedEntityId, overriddenEntity.getId());
    assertEquals("Customer Enabled Email", overriddenEntity.getData().getDisplayName());
    assertFalse(overriddenEntity.getData().getDisabled());
  }

  @Test
  void testComplexBodyExtractionWithParseJsonPipeline() {
    // Span extraction with nested key path
    CreateEntityDerivationConfigRequest createRequest =
        CreateEntityDerivationConfigRequest.newBuilder()
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setDisplayName("Account ID from Transaction")
                    .setCategory(EntityCategory.ENTITY_CATEGORY_CUSTOM)
                    .setEventKind(
                        ComplexDataModelEventKind.newBuilder()
                            .setKindId("system_event_kind_string"))
                    .setSpanProjection(
                        SpanProjection.newBuilder()
                            .addEventDerivationConfigs(
                                EventDerivationConfigDetails.newBuilder()
                                    .setName("Extract Account ID from Response")
                                    .setScope(
                                        Scope.newBuilder()
                                            .setEnvironmentScope(EnvironmentScope.newBuilder()))
                                    .setSpanExtraction(
                                        SpanBasedExtraction.newBuilder()
                                            .setLocation(
                                                ExtractionLocation.newBuilder()
                                                    .setLocationType(
                                                        ExtractionLocationType
                                                            .EXTRACTION_LOCATION_TYPE_RESPONSE_BODY)
                                                    .setKey("transactionDetails.accountId")
                                                    .setKeyMatchType(
                                                        KeyMatchType.KEY_MATCH_TYPE_EXACT))))))
            .build();

    CreateEntityDerivationConfigResponse createResponse =
        RequestContext.forTenantId("test-tenant")
            .call(() -> serviceStub.createEntityDerivationConfig(createRequest));
    assertNotNull(createResponse.getEntityDerivationConfig().getId());

    // Verify round-trip: fetch and check span extraction
    GetEntityDerivationConfigsResponse getResponse =
        RequestContext.forTenantId("test-tenant")
            .call(
                () ->
                    serviceStub.getEntityDerivationConfigs(
                        GetEntityDerivationConfigsRequest.newBuilder()
                            .addIds(createResponse.getEntityDerivationConfig().getId())
                            .build()));

    assertEquals(1, getResponse.getEntityDerivationConfigsCount());
    EntityDerivationConfig config = getResponse.getEntityDerivationConfigs(0);
    assertEquals("Account ID from Transaction", config.getData().getDisplayName());

    EventDerivationConfigDetails details =
        config.getData().getSpanProjection().getEventDerivationConfigs(0);
    assertEquals(
        ExtractionLocationType.EXTRACTION_LOCATION_TYPE_RESPONSE_BODY,
        details.getSpanExtraction().getLocation().getLocationType());
    assertEquals(
        "transactionDetails.accountId", details.getSpanExtraction().getLocation().getKey());
    assertEquals(
        KeyMatchType.KEY_MATCH_TYPE_EXACT,
        details.getSpanExtraction().getLocation().getKeyMatchType());
  }

  @Test
  void testJsonPathExtractionPipeline() {
    // traceable:getJsonPathValue($s.response_body, '$.transaction.details.accountId')
    CreateEntityDerivationConfigRequest createRequest =
        CreateEntityDerivationConfigRequest.newBuilder()
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setDisplayName("Account ID via JsonPath")
                    .setCategory(EntityCategory.ENTITY_CATEGORY_CUSTOM)
                    .setEventKind(
                        ComplexDataModelEventKind.newBuilder()
                            .setKindId("system_event_kind_string"))
                    .setSpanProjection(
                        SpanProjection.newBuilder()
                            .addEventDerivationConfigs(
                                EventDerivationConfigDetails.newBuilder()
                                    .setName("Extract Account ID via JsonPath")
                                    .setScope(
                                        Scope.newBuilder()
                                            .setEnvironmentScope(EnvironmentScope.newBuilder()))
                                    .setSpanExtraction(
                                        SpanBasedExtraction.newBuilder()
                                            .setLocation(
                                                ExtractionLocation.newBuilder()
                                                    .setLocationType(
                                                        ExtractionLocationType
                                                            .EXTRACTION_LOCATION_TYPE_RESPONSE_BODY)))
                                    .setPipeline(
                                        TransformationPipeline.newBuilder()
                                            .addTransformationPipeline(
                                                TransformationFunctionInvocation.newBuilder()
                                                    .setFunctionId(
                                                        "system_defined_function_json_path")
                                                    .putParameterValues(
                                                        "path",
                                                        Value.newBuilder()
                                                            .setStringValue(
                                                                "$.transaction.details.accountId")
                                                            .build()))))))
            .build();

    CreateEntityDerivationConfigResponse createResponse =
        RequestContext.forTenantId("test-tenant")
            .call(() -> serviceStub.createEntityDerivationConfig(createRequest));
    assertNotNull(createResponse.getEntityDerivationConfig().getId());

    GetEntityDerivationConfigsResponse getResponse =
        RequestContext.forTenantId("test-tenant")
            .call(
                () ->
                    serviceStub.getEntityDerivationConfigs(
                        GetEntityDerivationConfigsRequest.newBuilder()
                            .addIds(createResponse.getEntityDerivationConfig().getId())
                            .build()));

    assertEquals(1, getResponse.getEntityDerivationConfigsCount());
    EntityDerivationConfig config = getResponse.getEntityDerivationConfigs(0);
    assertEquals("Account ID via JsonPath", config.getData().getDisplayName());

    EventDerivationConfigDetails details =
        config.getData().getSpanProjection().getEventDerivationConfigs(0);
    assertEquals(1, details.getPipeline().getTransformationPipelineCount());
    assertEquals(
        "system_defined_function_json_path",
        details.getPipeline().getTransformationPipeline(0).getFunctionId());
    assertEquals(
        "$.transaction.details.accountId",
        details
            .getPipeline()
            .getTransformationPipeline(0)
            .getParameterValuesMap()
            .get("path")
            .getStringValue());
  }

  private EntityDerivationConfigData createValidConfigData(String displayName) {
    return EntityDerivationConfigData.newBuilder()
        .setDisplayName(displayName)
        .setCategory(EntityCategory.ENTITY_CATEGORY_CUSTOM)
        .setEventKind(ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string"))
        .setSpanProjection(
            SpanProjection.newBuilder()
                .addEventDerivationConfigs(
                    EventDerivationConfigDetails.newBuilder().setName("Test Derivation Rule")))
        .build();
  }
}
