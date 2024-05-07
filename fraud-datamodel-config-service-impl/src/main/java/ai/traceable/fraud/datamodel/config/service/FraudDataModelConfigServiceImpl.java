package ai.traceable.fraud.datamodel.config.service;

import ai.traceable.fraud.datamodel.config.service.store.FraudObjectTypesStore;
import ai.traceable.fraud.datamodel.config.service.v1.*;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectType;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class FraudDataModelConfigServiceImpl
    extends FraudDataModelConfigServiceGrpc.FraudDataModelConfigServiceImplBase {
  private final FraudObjectTypesStore fraudObjectTypesStore;
  private final FraudDataModelConfigServiceRequestValidator validator;

  @Inject
  public FraudDataModelConfigServiceImpl(
      FraudObjectTypesStore fraudObjectTypesStore,
      FraudDataModelConfigServiceRequestValidator validator) {
    this.fraudObjectTypesStore = fraudObjectTypesStore;
    this.validator = validator;
  }

  @Override
  public void upsertEntityType(
      UpsertEntityTypeRequest request, StreamObserver<UpsertEntityTypeResponse> responseObserver) {
    try {
      validator.validateOrThrow(request);
      // todo: implement column mapping
      var entityType =
          EntityType.newBuilder()
              .setId(request.getId())
              .setIdSet(request.getIdSet())
              .setLifecycle(request.getLifecycle())
              .putAllFieldsMeta(request.getFieldsMetaMap())
              .build();
      fraudObjectTypesStore.putObjectTypes(
          getTenantId(),
          Collections.singletonList(ObjectType.newBuilder().setEntityType(entityType).build()));
      responseObserver.onNext(
          UpsertEntityTypeResponse.newBuilder().setEntityType(entityType).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Put object type failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void upsertRelationshipType(
      UpsertRelationshipTypeRequest request,
      StreamObserver<UpsertRelationshipTypeResponse> responseObserver) {
    try {
      validator.validateOrThrow(request);
      // todo: implement column mapping
      var relationshipType =
          RelationshipType.newBuilder()
              .setId(request.getId())
              .setLifecycle(request.getLifecycle())
              .setLeftEntityTypeId(request.getLeftEntityTypeId())
              .setRightEntityTypeId(request.getRightEntityTypeId())
              .setLeftCardinality(request.getLeftCardinality())
              .setRightCardinality(request.getRightCardinality())
              .setLeftToRightNavName(request.getLeftToRightNavName())
              .setRightToLeftNavName(request.getRightToLeftNavName())
              .build();
      fraudObjectTypesStore.putObjectTypes(
          getTenantId(),
          Collections.singletonList(
              ObjectType.newBuilder().setRelationshipType(relationshipType).build()));
      responseObserver.onNext(
          UpsertRelationshipTypeResponse.newBuilder()
              .setRelationshipType(relationshipType)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Put object type failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void upsertEventType(
      UpsertEventTypeRequest request, StreamObserver<UpsertEventTypeResponse> responseObserver) {
    try {
      validator.validateOrThrow(request);
      // todo: implement column mapping
      var eventType =
          EventType.newBuilder()
              .setId(request.getId())
              .putAllFieldsMeta(request.getFieldsMetaMap())
              .setTimestampField(request.getTimestampField())
              .build();
      fraudObjectTypesStore.putObjectTypes(
          getTenantId(),
          Collections.singletonList(ObjectType.newBuilder().setEventType(eventType).build()));
      responseObserver.onNext(UpsertEventTypeResponse.newBuilder().setEventType(eventType).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Put object type failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void upsertMetricType(
      UpsertMetricTypeRequest request, StreamObserver<UpsertMetricTypeResponse> responseObserver) {
    try {
      validator.validateOrThrow(request);
      // todo: implement column mapping
      var metricType =
          MetricType.newBuilder()
              .setId(request.getId())
              .setMetricDataType(request.getMetricDataType())
              .setTimestampField(request.getTimestampField())
              .putAllFieldsMeta(request.getFieldsMetaMap())
              .build();
      fraudObjectTypesStore.putObjectTypes(
          getTenantId(),
          Collections.singletonList(ObjectType.newBuilder().setMetricType(metricType).build()));
      responseObserver.onNext(
          UpsertMetricTypeResponse.newBuilder().setMetricType(metricType).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Put object type failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getTypes(GetTypesRequest request, StreamObserver<GetTypesResponse> responseObserver) {
    try {
      var allTypes =
          fraudObjectTypesStore.getAllObjectTypes(
              getTenantId(), ObjectKind.OBJECT_KIND_UNSPECIFIED);
      var response = GetTypesResponse.newBuilder();
      for (var type : allTypes) {
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
  public void getEntityTypes(
      GetEntityTypesRequest request, StreamObserver<GetEntityTypesResponse> responseObserver) {
    try {
      var allTypes =
          fraudObjectTypesStore.getAllObjectTypes(getTenantId(), ObjectKind.OBJECT_KIND_ENTITY);
      var entitiesResponse = GetEntityTypesResponse.newBuilder();
      for (var type : allTypes) {
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
      var allTypes =
          fraudObjectTypesStore.getAllObjectTypes(
              getTenantId(), ObjectKind.OBJECT_KIND_RELATIONSHIP);
      var relationshipsResponse = GetRelationshipTypesResponse.newBuilder();
      for (var type : allTypes) {
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
      var allTypes =
          fraudObjectTypesStore.getAllObjectTypes(getTenantId(), ObjectKind.OBJECT_KIND_EVENT);
      var eventTypesResponse = GetEventTypesResponse.newBuilder();
      for (var type : allTypes) {
        eventTypesResponse.addEventTypes(type.getEventType());
      }
      responseObserver.onNext(eventTypesResponse.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get dataset types failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getMetricTypes(
      GetMetricTypesRequest request, StreamObserver<GetMetricTypesResponse> responseObserver) {
    try {
      var allTypes =
          fraudObjectTypesStore.getAllObjectTypes(getTenantId(), ObjectKind.OBJECT_KIND_METRIC);
      var metricsResponse = GetMetricTypesResponse.newBuilder();
      for (var type : allTypes) {
        metricsResponse.addMetricTypes(type.getMetricType());
      }
      responseObserver.onNext(metricsResponse.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get metric types failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  private String getTenantId() {
    return RequestContext.CURRENT
        .get()
        .getTenantId()
        .orElseThrow(
            Status.INVALID_ARGUMENT.withDescription("Tenant ID is missing in the request")
                ::asRuntimeException);
  }
}
