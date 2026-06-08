package ai.traceable.fraud.datamodel.config.service;

import ai.traceable.fraud.datamodel.config.service.v1.*;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FraudDataModelConfigServiceRequestValidatorTest {
  private FraudDataModelConfigServiceRequestValidator target;
  private RequestContext requestContext;

  @BeforeEach
  public void setup() {
    requestContext = RequestContext.forTenantId("tenant1");
    target = new FraudDataModelConfigServiceRequestValidator();
  }

  @Test
  public void testValidateUpsertEntityTypeRequest() {
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            target.validateOrThrow(
                requestContext, UpsertEntityTypeRequest.newBuilder().getDefaultInstanceForType()));
    var entityType = FraudDataModelTestUtils.entityType("test");
    Assertions.assertDoesNotThrow(
        () ->
            target.validateOrThrow(
                requestContext,
                UpsertEntityTypeRequest.newBuilder()
                    .setId(entityType.getId())
                    .setIdSet(entityType.getIdSet())
                    .setLifecycle(entityType.getLifecycle())
                    .putAllFieldsMeta(entityType.getFieldsMetaMap())
                    .build()));

    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            target.validateOrThrow(
                requestContext,
                UpsertEntityTypeRequest.newBuilder()
                    .setId(entityType.getId())
                    .setLifecycle(entityType.getLifecycle())
                    .putAllFieldsMeta(entityType.getFieldsMetaMap())
                    .build()));
  }

  @Test
  public void testValidateUpsertRelationshipTypeRequest() {
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            target.validateOrThrow(
                requestContext,
                UpsertRelationshipTypeRequest.newBuilder().getDefaultInstanceForType()));
    var relationshipType = FraudDataModelTestUtils.relationshipType("left", "right");
    Assertions.assertDoesNotThrow(
        () ->
            target.validateOrThrow(
                requestContext,
                UpsertRelationshipTypeRequest.newBuilder()
                    .setId(relationshipType.getId())
                    .setLeftEntityTypeId(relationshipType.getLeftEntityTypeId())
                    .setRightEntityTypeId(relationshipType.getRightEntityTypeId())
                    .setLeftCardinality(relationshipType.getLeftCardinality())
                    .setRightCardinality(relationshipType.getRightCardinality())
                    .setLeftToRightNavName(relationshipType.getLeftToRightNavName())
                    .setRightToLeftNavName(relationshipType.getRightToLeftNavName())
                    .build()));
  }

  @Test
  public void testValidateUpsertEventTypeRequest() {
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            target.validateOrThrow(
                requestContext, UpsertEventTypeRequest.newBuilder().getDefaultInstanceForType()));
    var eventType = FraudDataModelTestUtils.eventType("test_events");
    Assertions.assertDoesNotThrow(
        () ->
            target.validateOrThrow(
                requestContext,
                UpsertEventTypeRequest.newBuilder()
                    .setId(eventType.getId())
                    .setTimestampField(eventType.getTimestampField())
                    .putAllFieldsMeta(eventType.getFieldsMetaMap())
                    .build()));
  }

  @Test
  public void testValidateUpsertMetricTypeRequest() {
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            target.validateOrThrow(
                requestContext, UpsertMetricTypeRequest.newBuilder().getDefaultInstanceForType()));
    MetricType metricType = FraudDataModelTestUtils.metricType("test_metric");
    Assertions.assertDoesNotThrow(
        () ->
            target.validateOrThrow(
                requestContext,
                UpsertMetricTypeRequest.newBuilder()
                    .setId(metricType.getId())
                    .setMetricDataType(metricType.getMetricDataType())
                    .setTimestampField(metricType.getTimestampField())
                    .putAllFieldsMeta(metricType.getFieldsMetaMap())
                    .build()));
  }

  @Test
  public void testInvalidRequestContext() {
    Assertions.assertThrows(
        RuntimeException.class,
        () -> target.validateRequestContext(RequestContext.forTenantId(null)));
  }

  @Test
  public void testIsValidFieldName() {
    Assertions.assertTrue(FraudDataModelConfigServiceRequestValidator.isValidFieldName("userId"));
    Assertions.assertTrue(
        FraudDataModelConfigServiceRequestValidator.isValidFieldName("account_type"));
    Assertions.assertTrue(
        FraudDataModelConfigServiceRequestValidator.isValidFieldName("ip_address"));
    Assertions.assertTrue(
        FraudDataModelConfigServiceRequestValidator.isValidFieldName("start_time_millis_ts"));
    Assertions.assertTrue(FraudDataModelConfigServiceRequestValidator.isValidFieldName("_private"));
    Assertions.assertTrue(FraudDataModelConfigServiceRequestValidator.isValidFieldName("A"));

    Assertions.assertFalse(
        FraudDataModelConfigServiceRequestValidator.isValidFieldName("Account Owner"));
    Assertions.assertFalse(
        FraudDataModelConfigServiceRequestValidator.isValidFieldName("field-name"));
    Assertions.assertFalse(
        FraudDataModelConfigServiceRequestValidator.isValidFieldName("field.name"));
    Assertions.assertFalse(
        FraudDataModelConfigServiceRequestValidator.isValidFieldName("123starts_with_number"));
    Assertions.assertFalse(FraudDataModelConfigServiceRequestValidator.isValidFieldName(""));
    Assertions.assertFalse(FraudDataModelConfigServiceRequestValidator.isValidFieldName(null));
    Assertions.assertFalse(
        FraudDataModelConfigServiceRequestValidator.isValidFieldName("name with spaces"));
    Assertions.assertFalse(
        FraudDataModelConfigServiceRequestValidator.isValidFieldName("special!char"));
  }

  @Test
  public void testUpsertEventTypeRejectsFieldNameWithSpaces() {
    var eventType = FraudDataModelTestUtils.eventType("test_events");
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            target.validateOrThrow(
                requestContext,
                UpsertEventTypeRequest.newBuilder()
                    .setId(eventType.getId())
                    .setTimestampField(eventType.getTimestampField())
                    .putAllFieldsMeta(eventType.getFieldsMetaMap())
                    .putFieldsMeta(
                        "Account Owner",
                        FieldMetadata.newBuilder().setFieldType(FieldType.FIELD_TYPE_STR).build())
                    .build()));
  }

  @Test
  public void testUpsertEntityTypeRejectsFieldNameWithSpaces() {
    var entityType = FraudDataModelTestUtils.entityType("test");
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            target.validateOrThrow(
                requestContext,
                UpsertEntityTypeRequest.newBuilder()
                    .setId(entityType.getId())
                    .setIdSet(entityType.getIdSet())
                    .setLifecycle(entityType.getLifecycle())
                    .putAllFieldsMeta(entityType.getFieldsMetaMap())
                    .putFieldsMeta(
                        "Bad Field",
                        EntityFieldMetadata.newBuilder()
                            .setFieldType(FieldType.FIELD_TYPE_STR)
                            .build())
                    .build()));
  }
}
