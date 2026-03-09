package ai.traceable.fraud.datamodel.derivation.config.service.entity;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityCategory;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultEntityDerivationProviderTest {

  private DefaultEntityDerivationProvider provider;

  @BeforeEach
  void setUp() {
    provider = new DefaultEntityDerivationProvider();
  }

  @Test
  void yamlFileIsValidAndParseable() {
    assertDoesNotThrow(() -> new DefaultEntityDerivationProvider());
  }

  @Test
  void getDefaultEntityDerivations_returnsNonEmptyList() {
    List<EntityDerivationConfig> entities = provider.getDefaultEntityDerivations();

    assertNotNull(entities);
    assertFalse(entities.isEmpty(), "Default entity derivations list should not be empty");
  }

  @Test
  void allDefaultEntitiesHaveRequiredFields() {
    List<EntityDerivationConfig> entities = provider.getDefaultEntityDerivations();

    for (EntityDerivationConfig entity : entities) {
      assertFalse(entity.getId().isEmpty(), "Entity ID should not be empty: " + entity);
      assertFalse(
          entity.getColumnName().isEmpty(), "Entity column_name should not be empty: " + entity);
      assertFalse(
          entity.getData().getDisplayName().isEmpty(),
          "Entity display_name should not be empty: " + entity);
      assertTrue(entity.getData().hasEventKind(), "Entity should have event_kind set: " + entity);
      assertTrue(
          entity.getData().getCategory() != EntityCategory.ENTITY_CATEGORY_UNSPECIFIED,
          "Entity category should be specified: " + entity);
    }
  }

  @Test
  void containsExpectedMandatoryEntities() {
    List<EntityDerivationConfig> entities = provider.getDefaultEntityDerivations();

    assertTrue(entities.stream().anyMatch(e -> e.getId().equals("mandatory_entity_jwt_payload")));
  }

  @Test
  void containsExpectedSystemEntities() {
    List<EntityDerivationConfig> entities = provider.getDefaultEntityDerivations();

    assertTrue(entities.stream().anyMatch(e -> e.getId().equals("system_entity_customer_id")));
    assertTrue(entities.stream().anyMatch(e -> e.getId().equals("system_entity_span_id")));
    assertTrue(entities.stream().anyMatch(e -> e.getId().equals("system_entity_api_id")));
    assertTrue(entities.stream().anyMatch(e -> e.getId().equals("system_entity_ip_address")));
  }

  @Test
  void mandatoryEntitiesHaveCorrectCategory() {
    List<EntityDerivationConfig> mandatoryEntities =
        provider.getDefaultEntityDerivations().stream()
            .filter(e -> e.getId().startsWith("mandatory_entity_"))
            .collect(java.util.stream.Collectors.toList());

    assertFalse(mandatoryEntities.isEmpty());
    mandatoryEntities.forEach(
        e -> assertEquals(EntityCategory.ENTITY_CATEGORY_MANDATORY, e.getData().getCategory()));
  }

  @Test
  void systemEntitiesHaveCorrectCategory() {
    List<EntityDerivationConfig> systemEntities =
        provider.getDefaultEntityDerivations().stream()
            .filter(e -> e.getId().startsWith("system_entity_"))
            .collect(java.util.stream.Collectors.toList());

    assertFalse(systemEntities.isEmpty());
    systemEntities.forEach(
        e -> assertEquals(EntityCategory.ENTITY_CATEGORY_SYSTEM, e.getData().getCategory()));
  }

  @Test
  void isDefaultEntity_returnsCorrectValue() {
    assertTrue(provider.isDefaultEntity("system_entity_ip_address"));
    assertTrue(provider.isDefaultEntity("mandatory_entity_jwt_payload"));
    assertFalse(provider.isDefaultEntity("non_existent_entity"));
  }

  @Test
  void getDefaultEntity_returnsCorrectEntity() {
    EntityDerivationConfig jwtEntity = provider.getDefaultEntity("mandatory_entity_jwt_payload");
    assertNotNull(jwtEntity);
    assertEquals("mandatory_entity_jwt_payload", jwtEntity.getId());
    assertEquals("JWT Payload", jwtEntity.getData().getDisplayName());

    assertNull(provider.getDefaultEntity("non_existent_entity"));
  }

  @Test
  void systemEntity_hasPrepopulatedAttribute() {
    EntityDerivationConfig spanId = provider.getDefaultEntity("system_entity_span_id");
    assertNotNull(spanId);
    assertTrue(spanId.getData().hasPrepopulatedSpanAttribute());
    assertFalse(spanId.getData().hasSpanProjection());
    assertFalse(spanId.getData().hasParentDerivation());
    assertTrue(spanId.getData().getInternal());
  }

  @Test
  void systemEntitiesAreReadOnly() {
    EntityDerivationConfig apiId = provider.getDefaultEntity("system_entity_api_id");
    assertNotNull(apiId);
    assertEquals(EntityCategory.ENTITY_CATEGORY_SYSTEM, apiId.getData().getCategory());
    assertEquals("API ID", apiId.getData().getDisplayName());
    assertEquals("api_id", apiId.getColumnName());
  }

  @Test
  void allEntitiesHaveValidEventKind() {
    List<EntityDerivationConfig> entities = provider.getDefaultEntityDerivations();
    List<String> entitiesWithoutKind =
        entities.stream()
            .filter(
                e ->
                    !e.getData().hasEventKind() || e.getData().getEventKind().getKindId().isEmpty())
            .map(e -> e.getId() + " (" + e.getData().getDisplayName() + ")")
            .collect(Collectors.toUnmodifiableList());

    assertTrue(
        entitiesWithoutKind.isEmpty(),
        "Entities missing event kind: " + String.join(", ", entitiesWithoutKind));
  }

  @Test
  void jwtEntity_transformationPipelineIsParsedCorrectly() {
    EntityDerivationConfig jwt = provider.getDefaultEntity("mandatory_entity_jwt_payload");
    assertNotNull(jwt);
    assertTrue(jwt.getData().hasSpanProjection());
    assertTrue(jwt.getData().getSpanProjection().getEventDerivationConfigsCount() > 0);

    var pipeline = jwt.getData().getSpanProjection().getEventDerivationConfigs(0).getPipeline();
    assertEquals(4, pipeline.getTransformationPipelineCount());

    // Verify first function: replace
    var replaceFunc = pipeline.getTransformationPipeline(0);
    assertEquals("system_defined_function_replace", replaceFunc.getFunctionId());
    assertEquals(2, replaceFunc.getParameterValuesCount());
    assertTrue(replaceFunc.getParameterValuesMap().containsKey("pattern"));
    assertTrue(replaceFunc.getParameterValuesMap().containsKey("replacement"));
    assertEquals("Bearer ", replaceFunc.getParameterValuesMap().get("pattern").getStringValue());
    assertEquals("", replaceFunc.getParameterValuesMap().get("replacement").getStringValue());

    // Verify second function: split
    var splitFunc = pipeline.getTransformationPipeline(1);
    assertEquals("system_defined_function_split", splitFunc.getFunctionId());
    assertEquals(1, splitFunc.getParameterValuesCount());
    assertTrue(splitFunc.getParameterValuesMap().containsKey("delimiter"));
    assertEquals(".", splitFunc.getParameterValuesMap().get("delimiter").getStringValue());

    // Verify third function: get
    var getFunc = pipeline.getTransformationPipeline(2);
    assertEquals("system_defined_function_get", getFunc.getFunctionId());
    assertEquals(1, getFunc.getParameterValuesCount());
    assertTrue(getFunc.getParameterValuesMap().containsKey("index"));
    assertEquals(1.0, getFunc.getParameterValuesMap().get("index").getNumberValue(), 0.001);

    // Verify fourth function: base64_decode (no parameters)
    var decodeFunc = pipeline.getTransformationPipeline(3);
    assertEquals("system_defined_function_base64_decode", decodeFunc.getFunctionId());
    assertEquals(0, decodeFunc.getParameterValuesCount());
  }
}
