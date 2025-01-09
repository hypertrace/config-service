package ai.traceable.fraud.datamodel.config.service;

import ai.traceable.fraud.datamodel.config.service.clients.attributeservice.EventTypeToAttributeMetadataAdapter;
import ai.traceable.fraud.datamodel.config.service.clients.attributeservice.MetricTypeToAttributeMetadataAdapter;
import ai.traceable.fraud.datamodel.config.service.column.mapping.ColumnMapper;
import ai.traceable.fraud.datamodel.config.service.store.FraudObjectTypesStore;
import ai.traceable.fraud.datamodel.config.service.v1.BaselineType;
import ai.traceable.fraud.datamodel.config.service.v1.DeleteDataModelRequest;
import ai.traceable.fraud.datamodel.config.service.v1.DeleteDataModelResponse;
import ai.traceable.fraud.datamodel.config.service.v1.EntityType;
import ai.traceable.fraud.datamodel.config.service.v1.EventType;
import ai.traceable.fraud.datamodel.config.service.v1.FraudDataModelConfigServiceGrpc;
import ai.traceable.fraud.datamodel.config.service.v1.GetBaselineTypesRequest;
import ai.traceable.fraud.datamodel.config.service.v1.GetBaselineTypesResponse;
import ai.traceable.fraud.datamodel.config.service.v1.GetEntityTypesRequest;
import ai.traceable.fraud.datamodel.config.service.v1.GetEntityTypesResponse;
import ai.traceable.fraud.datamodel.config.service.v1.GetEventTypesRequest;
import ai.traceable.fraud.datamodel.config.service.v1.GetEventTypesResponse;
import ai.traceable.fraud.datamodel.config.service.v1.GetMetricTypesRequest;
import ai.traceable.fraud.datamodel.config.service.v1.GetMetricTypesResponse;
import ai.traceable.fraud.datamodel.config.service.v1.GetRelationshipTypesRequest;
import ai.traceable.fraud.datamodel.config.service.v1.GetRelationshipTypesResponse;
import ai.traceable.fraud.datamodel.config.service.v1.GetTypesRequest;
import ai.traceable.fraud.datamodel.config.service.v1.GetTypesResponse;
import ai.traceable.fraud.datamodel.config.service.v1.MetricType;
import ai.traceable.fraud.datamodel.config.service.v1.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.ObjectType;
import ai.traceable.fraud.datamodel.config.service.v1.ObjectTypeReference;
import ai.traceable.fraud.datamodel.config.service.v1.RelationshipType;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertBaselineTypeRequest;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertBaselineTypeResponse;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertEntityTypeRequest;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertEntityTypeResponse;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertEventTypeRequest;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertEventTypeResponse;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertMetricTypeRequest;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertMetricTypeResponse;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertRelationshipTypeRequest;
import ai.traceable.fraud.datamodel.config.service.v1.UpsertRelationshipTypeResponse;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class FraudDataModelConfigServiceImpl
    extends FraudDataModelConfigServiceGrpc.FraudDataModelConfigServiceImplBase {
  private final FraudObjectTypesStore fraudObjectTypesStore;
  private final FraudDataModelConfigServiceRequestValidator validator;
  private final ColumnMapper<EntityType> entityTypeColumnMapper;
  private final ColumnMapper<RelationshipType> relationshipTypeColumnMapper;
  private final ColumnMapper<EventType> eventTypeColumnMapper;
  private final ColumnMapper<MetricType> metricTypeColumnMapper;
  private final ColumnMapper<BaselineType> baselineTypeColumnMapper;
  private final MetricTypeToAttributeMetadataAdapter metricTypeToAttributeMetadataAdapter;
  private final EventTypeToAttributeMetadataAdapter eventTypeToAttributeMetadataAdapter;

  @Inject
  public FraudDataModelConfigServiceImpl(
      FraudObjectTypesStore fraudObjectTypesStore,
      FraudDataModelConfigServiceRequestValidator validator,
      ColumnMapper<EntityType> entityTypeColumnMapper,
      ColumnMapper<RelationshipType> relationshipTypeColumnMapper,
      ColumnMapper<EventType> eventTypeColumnMapper,
      ColumnMapper<MetricType> metricTypeColumnMapper,
      ColumnMapper<BaselineType> baselineTypeColumnMapper,
      MetricTypeToAttributeMetadataAdapter metricTypeToAttributeMetadataAdapter,
      EventTypeToAttributeMetadataAdapter eventTypeToAttributeMetadataAdapter) {
    this.fraudObjectTypesStore = fraudObjectTypesStore;
    this.validator = validator;
    this.entityTypeColumnMapper = entityTypeColumnMapper;
    this.relationshipTypeColumnMapper = relationshipTypeColumnMapper;
    this.eventTypeColumnMapper = eventTypeColumnMapper;
    this.metricTypeColumnMapper = metricTypeColumnMapper;
    this.baselineTypeColumnMapper = baselineTypeColumnMapper;
    this.metricTypeToAttributeMetadataAdapter = metricTypeToAttributeMetadataAdapter;
    this.eventTypeToAttributeMetadataAdapter = eventTypeToAttributeMetadataAdapter;
  }

  @Override
  public void upsertEntityType(
      UpsertEntityTypeRequest request, StreamObserver<UpsertEntityTypeResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateOrThrow(requestContext, request);
      EntityType entityType =
          EntityType.newBuilder()
              .setId(request.getId())
              .setIdSet(request.getIdSet())
              .setLifecycle(request.getLifecycle())
              .putAllFieldsMeta(request.getFieldsMetaMap())
              .build();
      ObjectType finalTypeForUpsert = generateMappings(requestContext, entityType);
      fraudObjectTypesStore.upsertObjectTypes(
          requestContext, Collections.singletonList(finalTypeForUpsert));
      responseObserver.onNext(
          UpsertEntityTypeResponse.newBuilder()
              .setEntityType(finalTypeForUpsert.getEntityType())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Put Entity type failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void upsertRelationshipType(
      UpsertRelationshipTypeRequest request,
      StreamObserver<UpsertRelationshipTypeResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateOrThrow(requestContext, request);
      RelationshipType relationshipType =
          RelationshipType.newBuilder()
              .setId(request.getId())
              .setLifecycle(request.getLifecycle())
              .setLeftEntityTypeId(request.getLeftEntityTypeId())
              .setRightEntityTypeId(request.getRightEntityTypeId())
              .setLeftCardinality(request.getLeftCardinality())
              .setRightCardinality(request.getRightCardinality())
              .setLeftToRightNavName(request.getLeftToRightNavName())
              .setRightToLeftNavName(request.getRightToLeftNavName())
              .addAllTags(FraudDataModelUtils.getDefaultTagsForRelationshipType())
              .build();
      ObjectType finalTypeForUpsert = generateMappings(requestContext, relationshipType);
      fraudObjectTypesStore.upsertObjectTypes(
          requestContext,
          Collections.singletonList(
              ObjectType.newBuilder()
                  .setRelationshipType(finalTypeForUpsert.getRelationshipType())
                  .build()));
      responseObserver.onNext(
          UpsertRelationshipTypeResponse.newBuilder()
              .setRelationshipType(finalTypeForUpsert.getRelationshipType())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Put Relationship type failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void upsertEventType(
      UpsertEventTypeRequest request, StreamObserver<UpsertEventTypeResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateOrThrow(requestContext, request);
      EventType eventType =
          EventType.newBuilder()
              .setId(request.getId())
              .putAllFieldsMeta(request.getFieldsMetaMap())
              .setTimestampField(request.getTimestampField())
              .build();
      ObjectType finalTypeForUpsert = generateMappings(requestContext, eventType);
      fraudObjectTypesStore.upsertObjectTypes(
          requestContext,
          Collections.singletonList(
              ObjectType.newBuilder().setEventType(finalTypeForUpsert.getEventType()).build()));
      responseObserver.onNext(
          UpsertEventTypeResponse.newBuilder()
              .setEventType(finalTypeForUpsert.getEventType())
              .build());
      responseObserver.onCompleted();
      eventTypeToAttributeMetadataAdapter.onUpsertEventType(finalTypeForUpsert.getEventType());
    } catch (Exception e) {
      log.error("Put Event type failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void upsertMetricType(
      UpsertMetricTypeRequest request, StreamObserver<UpsertMetricTypeResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateOrThrow(requestContext, request);
      MetricType metricType =
          MetricType.newBuilder()
              .setId(request.getId())
              .setMetricDataType(request.getMetricDataType())
              .setTimestampField(request.getTimestampField())
              .putAllFieldsMeta(request.getFieldsMetaMap())
              .build();
      ObjectType finalTypeForUpsert = generateMappings(requestContext, metricType);
      fraudObjectTypesStore.upsertObjectTypes(
          requestContext,
          Collections.singletonList(
              ObjectType.newBuilder().setMetricType(finalTypeForUpsert.getMetricType()).build()));
      responseObserver.onNext(
          UpsertMetricTypeResponse.newBuilder()
              .setMetricType(finalTypeForUpsert.getMetricType())
              .build());
      responseObserver.onCompleted();
      metricTypeToAttributeMetadataAdapter.onUpsertMetricType(finalTypeForUpsert.getMetricType());
    } catch (Exception e) {
      log.error("Put Metric type failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void upsertBaselineType(
      UpsertBaselineTypeRequest request,
      StreamObserver<UpsertBaselineTypeResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateOrThrow(requestContext, request);
      ObjectType finalTypeForUpsert = generateMappings(requestContext, request.getBaselineType());
      fraudObjectTypesStore.upsertObjectTypes(
          requestContext,
          Collections.singletonList(
              ObjectType.newBuilder()
                  .setBaselineType(finalTypeForUpsert.getBaselineType())
                  .build()));
      responseObserver.onNext(
          UpsertBaselineTypeResponse.newBuilder()
              .setBaselineType(finalTypeForUpsert.getBaselineType())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Put Baseline type failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getTypes(GetTypesRequest request, StreamObserver<GetTypesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateRequestContext(requestContext);
      List<ObjectType> allTypes =
          fraudObjectTypesStore.getAllObjectTypes(
              requestContext, ObjectKind.OBJECT_KIND_UNSPECIFIED);
      GetTypesResponse.Builder response = GetTypesResponse.newBuilder();
      for (ObjectType type : allTypes) {
        if (type.hasEntityType()) {
          response.addEntityTypes(type.getEntityType());
        } else if (type.hasRelationshipType()) {
          response.addRelationshipTypes(type.getRelationshipType());
        } else if (type.hasEventType()) {
          response.addEventTypes(type.getEventType());
        } else if (type.hasMetricType()) {
          response.addMetricTypes(type.getMetricType());
        }
      }
      responseObserver.onNext(response.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get object types failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteDataModel(
      DeleteDataModelRequest request, StreamObserver<DeleteDataModelResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateRequestContext(requestContext);
      List<ObjectType> allTypes =
          fraudObjectTypesStore.getAllObjectTypes(
              requestContext, ObjectKind.OBJECT_KIND_UNSPECIFIED);
      List<ObjectTypeReference> allTypeRefs =
          allTypes.stream()
              .map(FraudDataModelUtils::getObjectTypeReference)
              .collect(Collectors.toList());
      fraudObjectTypesStore.deleteObjectTypes(requestContext, allTypeRefs);
      allTypes.stream()
          .filter(ObjectType::hasMetricType)
          .forEach(
              objectType ->
                  metricTypeToAttributeMetadataAdapter.onDeleteMetricType(
                      objectType.getMetricType()));
      allTypes.stream()
          .filter(ObjectType::hasEventType)
          .forEach(
              objectType ->
                  eventTypeToAttributeMetadataAdapter.onDeleteEventType(objectType.getEventType()));
    } catch (Exception e) {
      log.error("Delete data model failed:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getEntityTypes(
      GetEntityTypesRequest request, StreamObserver<GetEntityTypesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateRequestContext(requestContext);
      List<ObjectType> allTypes =
          fraudObjectTypesStore.getAllObjectTypes(requestContext, ObjectKind.OBJECT_KIND_ENTITY);
      GetEntityTypesResponse.Builder entitiesResponse = GetEntityTypesResponse.newBuilder();
      for (ObjectType type : allTypes) {
        entitiesResponse.addEntityTypes(type.getEntityType());
      }
      responseObserver.onNext(entitiesResponse.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get entity types failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getRelationshipTypes(
      GetRelationshipTypesRequest request,
      StreamObserver<GetRelationshipTypesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateRequestContext(requestContext);
      List<ObjectType> allTypes =
          fraudObjectTypesStore.getAllObjectTypes(
              requestContext, ObjectKind.OBJECT_KIND_RELATIONSHIP);
      GetRelationshipTypesResponse.Builder relationshipsResponse =
          GetRelationshipTypesResponse.newBuilder();
      for (ObjectType type : allTypes) {
        relationshipsResponse.addRelationshipTypes(type.getRelationshipType());
      }
      responseObserver.onNext(relationshipsResponse.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get relationship types failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getEventTypes(
      GetEventTypesRequest request, StreamObserver<GetEventTypesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateRequestContext(requestContext);
      List<ObjectType> allTypes =
          fraudObjectTypesStore.getAllObjectTypes(requestContext, ObjectKind.OBJECT_KIND_EVENT);
      GetEventTypesResponse.Builder eventTypesResponse = GetEventTypesResponse.newBuilder();
      for (ObjectType type : allTypes) {
        eventTypesResponse.addEventTypes(type.getEventType());
      }
      responseObserver.onNext(eventTypesResponse.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get event types failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getMetricTypes(
      GetMetricTypesRequest request, StreamObserver<GetMetricTypesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateRequestContext(requestContext);
      List<ObjectType> allTypes =
          fraudObjectTypesStore.getAllObjectTypes(requestContext, ObjectKind.OBJECT_KIND_METRIC);
      GetMetricTypesResponse.Builder metricsResponse = GetMetricTypesResponse.newBuilder();
      for (ObjectType type : allTypes) {
        metricsResponse.addMetricTypes(type.getMetricType());
      }
      responseObserver.onNext(metricsResponse.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get metric types failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getBaselineTypes(
      GetBaselineTypesRequest request, StreamObserver<GetBaselineTypesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateRequestContext(requestContext);
      List<ObjectType> allTypes =
          fraudObjectTypesStore.getAllObjectTypes(requestContext, ObjectKind.OBJECT_KIND_BASELINE);
      GetBaselineTypesResponse.Builder baselinesResponse = GetBaselineTypesResponse.newBuilder();
      for (ObjectType type : allTypes) {
        baselinesResponse.addBaselineTypes(type.getBaselineType());
      }
      responseObserver.onNext(baselinesResponse.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get metric types failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  private ObjectType generateMappings(RequestContext requestContext, EntityType entityType)
      throws Exception {
    return fraudObjectTypesStore
        .getObjectType(
            requestContext,
            FraudDataModelUtils.getObjectTypeReference(
                ObjectKind.OBJECT_KIND_ENTITY, entityType.getId()))
        .map(objectType -> this.updateAndGetObjectType(requestContext, entityType, objectType))
        .orElseGet(() -> createAndGetObjectType(requestContext, entityType));
  }

  @SneakyThrows
  private ObjectType createAndGetObjectType(RequestContext requestContext, EntityType entityType) {
    EntityType mappedEntityType = entityTypeColumnMapper.forCreate(requestContext, entityType);
    return ObjectType.newBuilder().setEntityType(mappedEntityType).build();
  }

  @SneakyThrows
  private ObjectType updateAndGetObjectType(
      RequestContext requestContext, EntityType entityType, ObjectType existingObjectType) {
    EntityType mappedEntityType =
        entityTypeColumnMapper.forUpdate(
            requestContext, existingObjectType.getEntityType(), entityType);
    return ObjectType.newBuilder().setEntityType(mappedEntityType).build();
  }

  private ObjectType generateMappings(
      RequestContext requestContext, RelationshipType relationshipType) throws Exception {
    return fraudObjectTypesStore
        .getObjectType(
            requestContext,
            FraudDataModelUtils.getObjectTypeReference(
                ObjectKind.OBJECT_KIND_RELATIONSHIP, relationshipType.getId()))
        .map(
            objectType -> this.updateAndGetObjectType(requestContext, relationshipType, objectType))
        .orElseGet(() -> createAndGetObjectType(requestContext, relationshipType));
  }

  @SneakyThrows
  private ObjectType createAndGetObjectType(
      RequestContext requestContext, RelationshipType relationshipType) {
    RelationshipType mappedRelationshipType =
        relationshipTypeColumnMapper.forCreate(requestContext, relationshipType);
    return ObjectType.newBuilder().setRelationshipType(mappedRelationshipType).build();
  }

  @SneakyThrows
  private ObjectType updateAndGetObjectType(
      RequestContext requestContext,
      RelationshipType relationshipType,
      ObjectType existingObjectType) {
    RelationshipType mappedRelationshipType =
        relationshipTypeColumnMapper.forUpdate(
            requestContext, existingObjectType.getRelationshipType(), relationshipType);
    return ObjectType.newBuilder().setRelationshipType(mappedRelationshipType).build();
  }

  private ObjectType generateMappings(RequestContext requestContext, EventType eventType)
      throws Exception {
    return fraudObjectTypesStore
        .getObjectType(
            requestContext,
            FraudDataModelUtils.getObjectTypeReference(
                ObjectKind.OBJECT_KIND_RELATIONSHIP, eventType.getId()))
        .map(objectType -> this.updateAndGetObjectType(requestContext, eventType, objectType))
        .orElseGet(() -> createAndGetObjectType(requestContext, eventType));
  }

  @SneakyThrows
  private ObjectType createAndGetObjectType(RequestContext requestContext, EventType eventType) {
    EventType mappedEventType = eventTypeColumnMapper.forCreate(requestContext, eventType);
    return ObjectType.newBuilder().setEventType(mappedEventType).build();
  }

  @SneakyThrows
  private ObjectType updateAndGetObjectType(
      RequestContext requestContext, EventType eventType, ObjectType existingObjectType) {
    EventType mappedEventType =
        eventTypeColumnMapper.forUpdate(
            requestContext, existingObjectType.getEventType(), eventType);
    return ObjectType.newBuilder().setEventType(mappedEventType).build();
  }

  private ObjectType generateMappings(RequestContext requestContext, MetricType metricType)
      throws Exception {
    return fraudObjectTypesStore
        .getObjectType(
            requestContext,
            FraudDataModelUtils.getObjectTypeReference(
                ObjectKind.OBJECT_KIND_METRIC, metricType.getId()))
        .map(objectType -> this.updateAndGetObjectType(requestContext, metricType, objectType))
        .orElseGet(() -> createAndGetObjectType(requestContext, metricType));
  }

  @SneakyThrows
  private ObjectType createAndGetObjectType(RequestContext requestContext, MetricType metricType) {
    MetricType mappedMetricType = metricTypeColumnMapper.forCreate(requestContext, metricType);
    return ObjectType.newBuilder().setMetricType(mappedMetricType).build();
  }

  @SneakyThrows
  private ObjectType updateAndGetObjectType(
      RequestContext requestContext, MetricType metricType, ObjectType existingObjectType) {
    MetricType mappedMetricType =
        metricTypeColumnMapper.forUpdate(
            requestContext, existingObjectType.getMetricType(), metricType);
    return ObjectType.newBuilder().setMetricType(mappedMetricType).build();
  }

  private ObjectType generateMappings(RequestContext requestContext, BaselineType baselineType)
      throws Exception {
    return fraudObjectTypesStore
        .getObjectType(
            requestContext,
            FraudDataModelUtils.getObjectTypeReference(
                ObjectKind.OBJECT_KIND_BASELINE, baselineType.getId()))
        .map(objectType -> this.updateAndGetObjectType(requestContext, baselineType, objectType))
        .orElseGet(() -> createAndGetObjectType(requestContext, baselineType));
  }

  @SneakyThrows
  private ObjectType createAndGetObjectType(
      RequestContext requestContext, BaselineType baselineType) {
    BaselineType mappedBaselineType =
        baselineTypeColumnMapper.forCreate(requestContext, baselineType);
    return ObjectType.newBuilder().setBaselineType(mappedBaselineType).build();
  }

  @SneakyThrows
  private ObjectType updateAndGetObjectType(
      RequestContext requestContext, BaselineType baselineType, ObjectType existingObjectType) {
    BaselineType mappedBaselineType =
        baselineTypeColumnMapper.forUpdate(
            requestContext, existingObjectType.getBaselineType(), baselineType);
    return ObjectType.newBuilder().setBaselineType(mappedBaselineType).build();
  }
}
