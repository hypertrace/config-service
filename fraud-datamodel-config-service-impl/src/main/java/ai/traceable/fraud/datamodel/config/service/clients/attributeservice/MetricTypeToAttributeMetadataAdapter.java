package ai.traceable.fraud.datamodel.config.service.clients.attributeservice;

import ai.traceable.fraud.datamodel.config.service.v1.ColumnMapping;
import ai.traceable.fraud.datamodel.config.service.v1.FieldType;
import ai.traceable.fraud.datamodel.config.service.v1.MetricDataType;
import ai.traceable.fraud.datamodel.config.service.v1.MetricType;
import com.typesafe.config.Config;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.attribute.service.v1.AttributeCreateRequest;
import org.hypertrace.core.attribute.service.v1.AttributeDefinition;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeMetadataFilter;
import org.hypertrace.core.attribute.service.v1.AttributeServiceGrpc;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.core.attribute.service.v1.Empty;
import org.hypertrace.core.attribute.service.v1.Projection;

@Slf4j
public class MetricTypeToAttributeMetadataAdapter {
  public static final String GENERIC_METRIC = "GENERIC_METRIC";
  public static final String DOT = ".";
  private final AttributeServiceGrpc.AttributeServiceBlockingStub attributeServiceBlockingStub;
  private ClientHostPortConfig config;

  @Inject
  public MetricTypeToAttributeMetadataAdapter(
      AttributeServiceGrpc.AttributeServiceBlockingStub attributeServiceBlockingStub,
      Config config) {
    this.attributeServiceBlockingStub = attributeServiceBlockingStub;
    this.config = new ClientHostPortConfig(config);
    long durationMs = this.config.getTimeout().toMillis();
    this.attributeServiceBlockingStub.withInterceptors(
        new ClientInterceptor() {
          @Override
          public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
              MethodDescriptor<ReqT, RespT> methodDescriptor,
              CallOptions callOptions,
              Channel channel) {
            return channel.newCall(
                methodDescriptor, callOptions.withDeadlineAfter(durationMs, TimeUnit.MILLISECONDS));
          }
        });
  }

  public Empty onDeleteMetricType(MetricType metricType) {
    return attributeServiceBlockingStub.delete(
        AttributeMetadataFilter.newBuilder()
            .addScopeString(getScopeForMetricType(metricType))
            .build());
  }

  public Empty onUpsertMetricType(MetricType metricType) {
    onDeleteMetricType(metricType);

    AttributeCreateRequest.Builder builder = AttributeCreateRequest.newBuilder();
    for (Map.Entry<String, ColumnMapping> entry :
        metricType.getColumnMappingMeta().getColumnMappingMap().entrySet()) {
      builder.addAttributes(
          AttributeMetadata.newBuilder()
              .setValueKind(from(entry.getValue().getFieldType()))
              .setScopeString(getScopeForMetricType(metricType))
              .addSources(AttributeSource.QS)
              .setType(AttributeType.ATTRIBUTE)
              .setGroupable(true)
              .setFqn(getScopeForMetricType(metricType) + DOT + entry.getKey())
              .setKey(entry.getKey())
              .setDisplayName(entry.getKey())
              .setDefinition(
                  AttributeDefinition.newBuilder()
                      .setProjection(
                          Projection.newBuilder()
                              .setAttributeId(GENERIC_METRIC + DOT + entry.getValue().getColumnId())
                              .build())
                      .build())
              .setInternal(false)
              .build());
    }

    if (metricType.getMetricDataType() == MetricDataType.METRIC_DATA_TYPE_COUNTER) {
      builder.addAttributes(
          AttributeMetadata.newBuilder()
              .setValueKind(AttributeKind.TYPE_INT64)
              .setScopeString(getScopeForMetricType(metricType))
              .addSources(AttributeSource.QS)
              .setType(AttributeType.ATTRIBUTE)
              .setGroupable(true)
              .setFqn(getScopeForMetricType(metricType) + DOT + "counter_metric_value")
              .setKey("counter_metric_value")
              .setDisplayName("counter_metric_value")
              .setInternal(false)
              .setDefinition(
                  AttributeDefinition.newBuilder()
                      .setProjection(
                          Projection.newBuilder()
                              .setAttributeId(GENERIC_METRIC + DOT + "counter_metric_value")
                              .build()))
              .build());
    } else if (metricType.getMetricDataType() == MetricDataType.METRIC_DATA_TYPE_GAUGE) {
      builder.addAttributes(
          AttributeMetadata.newBuilder()
              .setValueKind(AttributeKind.TYPE_INT64)
              .setScopeString(getScopeForMetricType(metricType))
              .addSources(AttributeSource.QS)
              .setType(AttributeType.ATTRIBUTE)
              .setGroupable(true)
              .setFqn(getScopeForMetricType(metricType) + DOT + "gauge_metric_value")
              .setKey("gauge_metric_value")
              .setDisplayName("gauge_metric_value")
              .setDefinition(
                  AttributeDefinition.newBuilder()
                      .setProjection(
                          Projection.newBuilder()
                              .setAttributeId(GENERIC_METRIC + DOT + "gauge_metric_value")
                              .build()))
              .setInternal(false)
              .build());
    }

    // these time attributes are needed by Gateway Service
    builder.addAttributes(
        AttributeMetadata.newBuilder()
            .setValueKind(AttributeKind.TYPE_STRING)
            .setScopeString(getScopeForMetricType(metricType))
            .addSources(AttributeSource.QS)
            .setType(AttributeType.ATTRIBUTE)
            .setGroupable(true)
            .setFqn(getScopeForMetricType(metricType) + DOT + "startTime")
            .setKey("startTime")
            .setDisplayName("startTime")
            .setDefinition(
                AttributeDefinition.newBuilder()
                    .setProjection(
                        Projection.newBuilder()
                            .setAttributeId(GENERIC_METRIC + DOT + "startTime")
                            .build()))
            .setInternal(false)
            .build());

    builder.addAttributes(
        AttributeMetadata.newBuilder()
            .setValueKind(AttributeKind.TYPE_STRING)
            .setScopeString(getScopeForMetricType(metricType))
            .addSources(AttributeSource.QS)
            .setType(AttributeType.ATTRIBUTE)
            .setGroupable(true)
            .setFqn(getScopeForMetricType(metricType) + DOT + "type_id")
            .setKey("type_id")
            .setDisplayName("type_id")
            .setDefinition(
                AttributeDefinition.newBuilder()
                    .setProjection(
                        Projection.newBuilder()
                            .setAttributeId(GENERIC_METRIC + DOT + "type_id")
                            .build()))
            .setInternal(false)
            .build());

    return attributeServiceBlockingStub.create(builder.build());
  }

  private AttributeKind from(FieldType fieldType) {
    switch (fieldType) {
      case FIELD_TYPE_INT:
      case FIELD_TYPE_LONG:
        return AttributeKind.TYPE_INT64;
      case FIELD_TYPE_BINARY:
        return AttributeKind.TYPE_BYTES;
      case FIELD_TYPE_BOOL:
        return AttributeKind.TYPE_BOOL;
      case FIELD_TYPE_STR:
        return AttributeKind.TYPE_STRING;
      case FIELD_TYPE_DOUBLE:
        return AttributeKind.TYPE_DOUBLE;
      case FIELD_TYPE_LIST:
        return AttributeKind.TYPE_STRING_ARRAY;
      case FIELD_TYPE_TIMESTAMP:
        return AttributeKind.TYPE_TIMESTAMP;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(String.format("Unknown fieldType: %s", fieldType))
            .asRuntimeException();
    }
  }

  private String getScopeForMetricType(MetricType metricType) {
    return metricType.getId().toUpperCase();
  }
}
