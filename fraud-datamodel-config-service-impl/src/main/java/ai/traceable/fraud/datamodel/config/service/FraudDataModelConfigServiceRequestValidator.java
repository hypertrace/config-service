package ai.traceable.fraud.datamodel.config.service;

import ai.traceable.fraud.datamodel.config.service.v1.*;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectTypeReference;
import io.grpc.Status;
import java.util.Map;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class FraudDataModelConfigServiceRequestValidator {

  private void validateOrThrow(ObjectTypeReference objectTypeReference, ObjectKind objectKind) {
    if (objectTypeReference.getObjectKind() != objectKind) {
      throw Status.INVALID_ARGUMENT
          .withDescription("typeReference.objectKind cannot be empty")
          .asRuntimeException();
    }
    if (objectTypeReference.getId().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("typeReference.id cannot be empty")
          .asRuntimeException();
    }
  }

  private void validateOrThrow(String id) {
    if (id.isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("typeReference.id cannot be empty")
          .asRuntimeException();
    }
  }

  private void validateOrThrow(Map<String, FieldMetadata> fieldsMeta) {
    for (var entry : fieldsMeta.entrySet()) {
      var fieldName = entry.getKey();
      var fieldMeta = entry.getValue();
      if (fieldMeta.getFieldType() == FieldType.FIELD_TYPE_UNSPECIFIED
          || fieldMeta.getFieldType() == FieldType.UNRECOGNIZED) {
        throw Status.INVALID_ARGUMENT
            .withDescription("fieldType is required for fieldName=" + fieldName)
            .asRuntimeException();
      }
    }
  }

  private void validateOrThrow(String field, Map<String, FieldMetadata> fieldsMeta) {
    if (!fieldsMeta.containsKey(field)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("unrecognized field " + field)
          .asRuntimeException();
    }
  }

  private void validateOrThrowEntityMeta(Map<String, EntityFieldMetadata> fieldsMeta) {
    for (var entry : fieldsMeta.entrySet()) {
      var fieldName = entry.getKey();
      var fieldMeta = entry.getValue();
      if (fieldMeta.getFieldType() == FieldType.FIELD_TYPE_UNSPECIFIED
          || fieldMeta.getFieldType() == FieldType.UNRECOGNIZED) {
        throw Status.INVALID_ARGUMENT
            .withDescription("fieldType is required for fieldName=" + fieldName)
            .asRuntimeException();
      }
    }
  }

  private void validateOrThrowEntityMeta(
      String field, Map<String, EntityFieldMetadata> fieldsMeta) {
    if (!fieldsMeta.containsKey(field)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("unrecognized field " + field)
          .asRuntimeException();
    }
  }

  private void validateOrThrow(Cardinality cardinality) {
    if (cardinality != Cardinality.CARDINALITY_ONE && cardinality != Cardinality.CARDINALITY_MANY) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Cardinality must be ONE or MANY")
          .asRuntimeException();
    }
  }

  public void validateRequestContext(RequestContext requestContext) {
    if (requestContext.getTenantId().isEmpty()) {
      throw new IllegalArgumentException("Missing expected Tenant ID in request");
    }
  }

  public void validateOrThrow(
      RequestContext requestContext, UpsertEntityTypeRequest upsertEntityTypeRequest) {
    validateRequestContext(requestContext);
    validateOrThrow(upsertEntityTypeRequest.getId());
    var idFieldSet = upsertEntityTypeRequest.getIdSet();
    if (idFieldSet.getFieldCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("idFieldSet cannot be empty")
          .asRuntimeException();
    }
    if (upsertEntityTypeRequest.getFieldsMetaCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("fieldsMeta cannot be empty")
          .asRuntimeException();
    }
    validateOrThrowEntityMeta(upsertEntityTypeRequest.getFieldsMetaMap());
    for (var field : idFieldSet.getFieldList()) {
      validateOrThrowEntityMeta(field, upsertEntityTypeRequest.getFieldsMetaMap());
    }
  }

  public void validateOrThrow(
      RequestContext requestContext, UpsertRelationshipTypeRequest upsertRelationshipTypeRequest) {
    validateRequestContext(requestContext);
    validateOrThrow(upsertRelationshipTypeRequest.getId());
    if (upsertRelationshipTypeRequest.getLeftEntityTypeId().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("leftEntityTypeId cannot be empty")
          .asRuntimeException();
    }
    if (upsertRelationshipTypeRequest.getRightEntityTypeId().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("rightEntityTypeId cannot be empty")
          .asRuntimeException();
    }
    validateOrThrow(upsertRelationshipTypeRequest.getLeftCardinality());
    validateOrThrow(upsertRelationshipTypeRequest.getRightCardinality());
    if (upsertRelationshipTypeRequest.getRightToLeftNavName().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("rightToLeftNavName cannot be empty")
          .asRuntimeException();
    }
    if (upsertRelationshipTypeRequest.getLeftToRightNavName().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("leftToRightNavName cannot be empty")
          .asRuntimeException();
    }
  }

  public void validateOrThrow(
      RequestContext requestContext, UpsertEventTypeRequest upsertEventTypeRequest) {
    validateRequestContext(requestContext);
    validateOrThrow(upsertEventTypeRequest.getId());
    if (upsertEventTypeRequest.getTimestampField().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("timestampField cannot be empty")
          .asRuntimeException();
    }
    validateOrThrow(upsertEventTypeRequest.getFieldsMetaMap());
    validateOrThrow(
        upsertEventTypeRequest.getTimestampField(), upsertEventTypeRequest.getFieldsMetaMap());
  }

  public void validateOrThrow(
      RequestContext requestContext, UpsertMetricTypeRequest upsertMetricTypeRequest) {
    validateRequestContext(requestContext);
    validateOrThrow(upsertMetricTypeRequest.getId());
    if (upsertMetricTypeRequest.getTimestampField().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("timestampField cannot be empty")
          .asRuntimeException();
    }
    validateOrThrow(upsertMetricTypeRequest.getFieldsMetaMap());
    validateOrThrow(
        upsertMetricTypeRequest.getTimestampField(), upsertMetricTypeRequest.getFieldsMetaMap());
  }
}
