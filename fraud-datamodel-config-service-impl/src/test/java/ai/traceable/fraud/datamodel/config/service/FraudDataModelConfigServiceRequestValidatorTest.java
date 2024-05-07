package ai.traceable.fraud.datamodel.config.service;

import ai.traceable.fraud.datamodel.config.service.v1.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FraudDataModelConfigServiceRequestValidatorTest {
  private FraudDataModelConfigServiceRequestValidator target;

  @BeforeEach
  public void setup() {
    target = new FraudDataModelConfigServiceRequestValidator();
  }

  @Test
  public void testValidateUpsertEntityTypeRequest() {
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            target.validateOrThrow(
                UpsertEntityTypeRequest.newBuilder().getDefaultInstanceForType()));
    var entityType = FraudDataModelTestUtils.entityType("test");
    Assertions.assertDoesNotThrow(
        () ->
            target.validateOrThrow(
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
                UpsertRelationshipTypeRequest.newBuilder().getDefaultInstanceForType()));
    var relationshipType = FraudDataModelTestUtils.relationshipType("left", "right");
    Assertions.assertDoesNotThrow(
        () ->
            target.validateOrThrow(
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
                UpsertEventTypeRequest.newBuilder().getDefaultInstanceForType()));
    var eventType = FraudDataModelTestUtils.eventType("test_events");
    Assertions.assertDoesNotThrow(
        () ->
            target.validateOrThrow(
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
                UpsertMetricTypeRequest.newBuilder().getDefaultInstanceForType()));
    var metricType = FraudDataModelTestUtils.metricType("test_metric");
    Assertions.assertDoesNotThrow(
        () ->
            target.validateOrThrow(
                UpsertMetricTypeRequest.newBuilder()
                    .setId(metricType.getId())
                    .setTimestampField(metricType.getTimestampField())
                    .putAllFieldsMeta(metricType.getFieldsMetaMap())
                    .build()));
  }
}
