package ai.traceable.fraud.datamodel.config.service;

import ai.traceable.fraud.datamodel.config.service.v1.EntityType;
import ai.traceable.fraud.datamodel.config.service.v1.EventType;
import ai.traceable.fraud.datamodel.config.service.v1.MetricType;
import ai.traceable.fraud.datamodel.config.service.v1.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.ObjectType;
import ai.traceable.fraud.datamodel.config.service.v1.ObjectTypeReference;
import ai.traceable.fraud.datamodel.config.service.v1.RelationshipType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FraudDataModelUtilsTest {
  @Test
  public void testGetObjectTypeReference() {
    String typeId = "entity_type1";
    ObjectType objectType =
        ObjectType.newBuilder().setEntityType(EntityType.newBuilder().setId(typeId)).build();
    ObjectTypeReference ref = FraudDataModelUtils.getObjectTypeReference(objectType);
    Assertions.assertEquals(ObjectKind.OBJECT_KIND_ENTITY, ref.getObjectKind());
    Assertions.assertEquals(typeId, ref.getId());

    typeId = "rel_type1";
    objectType =
        ObjectType.newBuilder()
            .setRelationshipType(RelationshipType.newBuilder().setId(typeId))
            .build();
    ref = FraudDataModelUtils.getObjectTypeReference(objectType);
    Assertions.assertEquals(ObjectKind.OBJECT_KIND_RELATIONSHIP, ref.getObjectKind());
    Assertions.assertEquals(typeId, ref.getId());

    typeId = "event_type1";
    objectType = ObjectType.newBuilder().setEventType(EventType.newBuilder().setId(typeId)).build();
    ref = FraudDataModelUtils.getObjectTypeReference(objectType);
    Assertions.assertEquals(ObjectKind.OBJECT_KIND_EVENT, ref.getObjectKind());
    Assertions.assertEquals(typeId, ref.getId());

    typeId = "metric_type1";
    objectType =
        ObjectType.newBuilder().setMetricType(MetricType.newBuilder().setId(typeId)).build();
    ref = FraudDataModelUtils.getObjectTypeReference(objectType);
    Assertions.assertEquals(ObjectKind.OBJECT_KIND_METRIC, ref.getObjectKind());
    Assertions.assertEquals(typeId, ref.getId());
  }
}
