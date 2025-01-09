package ai.traceable.fraud.datamodel.config.service.clients.attributeservice;

import ai.traceable.fraud.datamodel.config.service.v1.ColumnMapping;
import ai.traceable.fraud.datamodel.config.service.v1.EventType;
import ai.traceable.fraud.datamodel.config.service.v1.FieldType;
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
public class EventTypeToAttributeMetadataAdapter {
  public static final String GENERIC_EVENT = "GENERIC_EVENT";
  public static final String DOT = ".";
  private final AttributeServiceGrpc.AttributeServiceBlockingStub attributeServiceBlockingStub;
  private final ClientHostPortConfig config;

  @Inject
  public EventTypeToAttributeMetadataAdapter(
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

  public Empty onDeleteEventType(EventType eventType) {
    return attributeServiceBlockingStub.delete(
        AttributeMetadataFilter.newBuilder()
            .addScopeString(getScopeForEventType(eventType))
            .build());
  }

  public Empty onUpsertEventType(EventType eventType) {
    onDeleteEventType(eventType);

    AttributeCreateRequest.Builder builder = AttributeCreateRequest.newBuilder();
    for (Map.Entry<String, ColumnMapping> entry :
        eventType.getColumnMappingMeta().getColumnMappingMap().entrySet()) {
      builder.addAttributes(
          AttributeMetadata.newBuilder()
              .setValueKind(from(entry.getValue().getFieldType()))
              .setScopeString(getScopeForEventType(eventType))
              .addSources(AttributeSource.QS)
              .setType(AttributeType.ATTRIBUTE)
              .setGroupable(true)
              .setFqn(getScopeForEventType(eventType) + DOT + entry.getKey())
              .setKey(entry.getKey())
              .setDisplayName(entry.getKey())
              .setDefinition(
                  AttributeDefinition.newBuilder()
                      .setProjection(
                          Projection.newBuilder()
                              .setAttributeId(GENERIC_EVENT + DOT + entry.getValue().getColumnId())
                              .build())
                      .build())
              .setInternal(false)
              .build());
    }

    // these time attributes are needed by Gateway Service
    builder.addAttributes(
        AttributeMetadata.newBuilder()
            .setValueKind(AttributeKind.TYPE_STRING)
            .setScopeString(getScopeForEventType(eventType))
            .addSources(AttributeSource.QS)
            .setType(AttributeType.ATTRIBUTE)
            .setGroupable(true)
            .setFqn(getScopeForEventType(eventType) + DOT + "startTime")
            .setKey("startTime")
            .setDisplayName("startTime")
            .setDefinition(
                AttributeDefinition.newBuilder()
                    .setProjection(
                        Projection.newBuilder()
                            .setAttributeId(GENERIC_EVENT + DOT + "startTime")
                            .build()))
            .setInternal(false)
            .build());

    builder.addAttributes(
        AttributeMetadata.newBuilder()
            .setValueKind(AttributeKind.TYPE_STRING)
            .setScopeString(getScopeForEventType(eventType))
            .addSources(AttributeSource.QS)
            .setType(AttributeType.ATTRIBUTE)
            .setGroupable(true)
            .setFqn(getScopeForEventType(eventType) + DOT + "type_id")
            .setKey("type_id")
            .setDisplayName("type_id")
            .setDefinition(
                AttributeDefinition.newBuilder()
                    .setProjection(
                        Projection.newBuilder()
                            .setAttributeId(GENERIC_EVENT + DOT + "type_id")
                            .build()))
            .setInternal(false)
            .build());

    return attributeServiceBlockingStub.create(builder.build());
  }

  private String getScopeForEventType(EventType eventType) {
    return eventType.getId().toUpperCase();
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
      case FIELD_TYPE_MAP:
        return AttributeKind.TYPE_STRING_MAP;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(String.format("Unknown fieldType: %s", fieldType))
            .asRuntimeException();
    }
  }
}
