package ai.traceable.fraud.datamodel.config.service.clients.attributeservice;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.fraud.datamodel.config.service.v1.ColumnMapping;
import ai.traceable.fraud.datamodel.config.service.v1.ColumnMappingMeta;
import ai.traceable.fraud.datamodel.config.service.v1.FieldMetadata;
import ai.traceable.fraud.datamodel.config.service.v1.FieldType;
import ai.traceable.fraud.datamodel.config.service.v1.MetricDataType;
import ai.traceable.fraud.datamodel.config.service.v1.MetricType;
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
import org.mockito.ArgumentCaptor;

class MetricTypeToAttributeMetadataAdapterTest {

  private AttributeServiceGrpc.AttributeServiceBlockingStub attributeServiceBlockingStub;

  private Config mockConfig;

  private MetricTypeToAttributeMetadataAdapter adapter;

  @BeforeEach
  void setup() {
    attributeServiceBlockingStub = mock(AttributeServiceGrpc.AttributeServiceBlockingStub.class);
    mockConfig = mock(Config.class);
    when(mockConfig.getConfig(any(String.class))).thenReturn(mockConfig);
    when(mockConfig.getString(any(String.class))).thenReturn("abc");
    adapter = new MetricTypeToAttributeMetadataAdapter(attributeServiceBlockingStub, mockConfig);
  }

  @Test
  void testOnUpsertMetricType_Success() {
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

    MetricType metricType =
        MetricType.newBuilder()
            .setId("test_metric")
            .setMetricDataType(MetricDataType.METRIC_DATA_TYPE_COUNTER)
            .setColumnMappingMeta(
                ColumnMappingMeta.newBuilder()
                    .putColumnMapping(
                        "field2", ColumnMapping.newBuilder().setColumnId("str_col2").build())
                    .build())
            .putAllFieldsMeta(fieldsMeta)
            .build();

    ArgumentCaptor<AttributeCreateRequest> attributeCreateRequestCaptor =
        ArgumentCaptor.forClass(AttributeCreateRequest.class);

    Empty response = adapter.onUpsertMetricType(metricType);
    verify(attributeServiceBlockingStub).create(attributeCreateRequestCaptor.capture());

    assertNotNull(response);
    verify(attributeServiceBlockingStub, times(1)).delete(any(AttributeMetadataFilter.class));
    verify(attributeServiceBlockingStub, times(1)).create(any(AttributeCreateRequest.class));

    List<AttributeMetadata> list = attributeCreateRequestCaptor.getValue().getAttributesList();
    assertEquals(5, list.size()); // +3 for startTime, type_id, counter_metric_value fields
    assertEquals(
        "GENERIC_METRIC.field1", list.get(0).getDefinition().getProjection().getAttributeId());
    assertEquals("TEST_METRIC.field1", list.get(0).getFqn());
    assertEquals(
        "GENERIC_METRIC.str_col2", list.get(1).getDefinition().getProjection().getAttributeId());
    assertEquals("TEST_METRIC.field2", list.get(1).getFqn());
    assertEquals(
        "GENERIC_METRIC.counter_metric_value",
        list.get(2).getDefinition().getProjection().getAttributeId());
    assertEquals(
        "GENERIC_METRIC.startTime", list.get(3).getDefinition().getProjection().getAttributeId());
    assertEquals(
        "GENERIC_METRIC.type_id", list.get(4).getDefinition().getProjection().getAttributeId());
  }
}
