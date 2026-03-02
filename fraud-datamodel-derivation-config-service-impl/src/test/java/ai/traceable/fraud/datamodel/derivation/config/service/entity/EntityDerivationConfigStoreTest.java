package ai.traceable.fraud.datamodel.derivation.config.service.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityCategory;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigData;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigFilter;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EventDerivationConfigDetails;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanProjection;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EntityDerivationConfigStoreTest {

  private EntityDerivationConfigStore store;

  @BeforeEach
  void setUp() {
    ConfigServiceGrpc.ConfigServiceBlockingStub stub =
        mock(ConfigServiceGrpc.ConfigServiceBlockingStub.class);
    ConfigChangeEventGenerator eventGenerator = mock(ConfigChangeEventGenerator.class);
    store = new EntityDerivationConfigStore(stub, eventGenerator);
  }

  @Test
  void testBuildDataFromValue_Success() throws InvalidProtocolBufferException {
    EntityDerivationConfig config =
        EntityDerivationConfig.newBuilder()
            .setId("config-123")
            .setColumnName("test-entity")
            .setData(createValidConfigData("Test Entity", false))
            .build();

    Value value = ConfigProtoConverter.convertToValue(config);
    Optional<EntityDerivationConfig> result = store.buildDataFromValue(value);

    assertTrue(result.isPresent());
    assertEquals("config-123", result.get().getId());
    assertEquals("test-entity", result.get().getColumnName());
  }

  @Test
  void testFilterConfigData_IncludeDisabled_False_FiltersOutDisabled() {
    EntityDerivationConfig config =
        EntityDerivationConfig.newBuilder()
            .setId("config-123")
            .setData(createValidConfigData("Test Entity", true))
            .build();

    GetEntityDerivationConfigsRequest request =
        GetEntityDerivationConfigsRequest.newBuilder()
            .setFilter(EntityDerivationConfigFilter.newBuilder().setIncludeDisabled(false).build())
            .build();

    Optional<EntityDerivationConfig> result = store.filterConfigData(config, request);
    assertFalse(result.isPresent());
  }

  @Test
  void testFilterConfigData_FilterByIds_Matches() {
    EntityDerivationConfig config =
        EntityDerivationConfig.newBuilder()
            .setId("config-123")
            .setData(createValidConfigData("Test Entity", false))
            .build();

    GetEntityDerivationConfigsRequest request =
        GetEntityDerivationConfigsRequest.newBuilder().addIds("config-123").build();

    Optional<EntityDerivationConfig> result = store.filterConfigData(config, request);
    assertTrue(result.isPresent());
  }

  private EntityDerivationConfigData createValidConfigData(String displayName, boolean disabled) {
    return EntityDerivationConfigData.newBuilder()
        .setDisplayName(displayName)
        .setDisabled(disabled)
        .setCategory(EntityCategory.ENTITY_CATEGORY_CUSTOM)
        .setEventKind(ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string"))
        .setSpanProjection(
            SpanProjection.newBuilder()
                .addEventDerivationConfigs(
                    EventDerivationConfigDetails.newBuilder().setName("Test Derivation Rule")))
        .build();
  }
}
