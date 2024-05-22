package ai.traceable.fraud.datamodel.config.service;

import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectType;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectTypeReference;
import io.grpc.Status;
import java.util.Collections;
import java.util.List;

public abstract class FraudDataModelUtils {
  private static final List<String> DEFAULT_RELATIONSHIP_TYPE_TAGS =
      Collections.singletonList("api_id");

  public static ObjectTypeReference getObjectTypeReference(ObjectType objectType) {
    switch (objectType.getObjectCase()) {
      case ENTITY_TYPE:
        return ObjectTypeReference.newBuilder()
            .setObjectKind(ObjectKind.OBJECT_KIND_ENTITY)
            .setId(objectType.getEntityType().getId())
            .build();
      case RELATIONSHIP_TYPE:
        return ObjectTypeReference.newBuilder()
            .setObjectKind(ObjectKind.OBJECT_KIND_RELATIONSHIP)
            .setId(objectType.getRelationshipType().getId())
            .build();
      case EVENT_TYPE:
        return ObjectTypeReference.newBuilder()
            .setObjectKind(ObjectKind.OBJECT_KIND_EVENT)
            .setId(objectType.getEventType().getId())
            .build();
      case METRIC_TYPE:
        return ObjectTypeReference.newBuilder()
            .setObjectKind(ObjectKind.OBJECT_KIND_METRIC)
            .setId(objectType.getMetricType().getId())
            .build();
    }
    throw Status.INVALID_ARGUMENT.asRuntimeException();
  }

  public static ObjectTypeReference getObjectTypeReference(ObjectKind objectKind, String typeId) {
    return ObjectTypeReference.newBuilder().setObjectKind(objectKind).setId(typeId).build();
  }

  public static List<String> getDefaultTagsForRelationshipType() {
    return DEFAULT_RELATIONSHIP_TYPE_TAGS;
  }
}
