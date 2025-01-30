package ai.traceable.fraud.datamodel.config.service.clients.attributeservice;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import ai.traceable.fraud.datamodel.config.service.v1.ColumnMapping;
import ai.traceable.fraud.datamodel.config.service.v1.ColumnMappingMeta;
import ai.traceable.fraud.datamodel.config.service.v1.EventType;
import ai.traceable.fraud.datamodel.config.service.v1.FieldMetadata;
import ai.traceable.fraud.datamodel.config.service.v1.FieldType;
import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.attribute.service.v1.AttributeCreateRequest;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeMetadataFilter;
import org.hypertrace.core.attribute.service.v1.AttributeServiceGrpc;
import org.hypertrace.core.attribute.service.v1.Empty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EventTypeToAttributeMetadataAdapterTest {

  @Mock private AttributeServiceGrpc.AttributeServiceBlockingStub attributeServiceBlockingStub;

  @Mock private Config mockConfig;

  private EventTypeToAttributeMetadataAdapter adapter;

  @BeforeEach
  void setup() {
    when(mockConfig.getConfig(any(String.class))).thenReturn(mockConfig);
    when(mockConfig.getString(any(String.class))).thenReturn("abc");
    adapter = new EventTypeToAttributeMetadataAdapter(attributeServiceBlockingStub, mockConfig);
  }

  @Test
  void testOnUpsertEventType_Success() {
    when(attributeServiceBlockingStub.delete(any(AttributeMetadataFilter.class)))
        .thenReturn(org.hypertrace.core.attribute.service.v1.Empty.getDefaultInstance());

    when(attributeServiceBlockingStub.create(any(AttributeCreateRequest.class)))
        .thenReturn(org.hypertrace.core.attribute.service.v1.Empty.getDefaultInstance());

    Map<String, FieldMetadata> fieldsMeta = new HashMap<>();
    fieldsMeta.put(
        "field1",
        FieldMetadata.newBuilder()
            .setFieldType(FieldType.FIELD_TYPE_INT)
            .setReserved(true)
            .build());
    fieldsMeta.put(
        "field2",
        FieldMetadata.newBuilder()
            .setFieldType(FieldType.FIELD_TYPE_STR)
            .setReserved(false)
            .build());

    EventType eventType =
        EventType.newBuilder()
            .setId("test_event")
            .setColumnMappingMeta(
                ColumnMappingMeta.newBuilder()
                    .putColumnMapping(
                        "field2", ColumnMapping.newBuilder().setColumnId("str_col2").build())
                    .build())
            .putAllFieldsMeta(fieldsMeta)
            .build();

    ArgumentCaptor<AttributeCreateRequest> attributeCreateRequestCaptor =
        ArgumentCaptor.forClass(AttributeCreateRequest.class);

    Empty response = adapter.onUpsertEventType(eventType);
    verify(attributeServiceBlockingStub).create(attributeCreateRequestCaptor.capture());

    assertNotNull(response);
    verify(attributeServiceBlockingStub, times(1)).delete(any(AttributeMetadataFilter.class));
    verify(attributeServiceBlockingStub, times(1)).create(any(AttributeCreateRequest.class));

    List<AttributeMetadata> list = attributeCreateRequestCaptor.getValue().getAttributesList();
    assertEquals(4, list.size()); // +2 for startTime and type_id fields
    assertEquals(
        "GENERIC_EVENT.field1", list.get(0).getDefinition().getProjection().getAttributeId());
    assertEquals("TEST_EVENT.field1", list.get(0).getFqn());
    assertEquals(
        "GENERIC_EVENT.str_col2", list.get(1).getDefinition().getProjection().getAttributeId());
    assertEquals("TEST_EVENT.field2", list.get(1).getFqn());
  }
}
