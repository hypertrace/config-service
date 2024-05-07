package ai.traceable.fraud.datamodel.config.service.store;

import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectType;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectTypeReference;
import io.grpc.Status;

abstract class Utils {
  static ObjectTypeReference getObjectTypeReference(ObjectType objectType) {
    switch (objectType.getObjectCase()) {
      case ENTITY_TYPE:
        return ObjectTypeReference.newBuilder()
            .setObjectKind(ObjectKind.OBJECT_KIND_ENTITY)
            .setId(objectType.getEntityType().getId())
            .build();
      case RELATIONSHIP_TYPE:
        return ObjectTypeReference.newBuilder()
            .setObjectKind(ObjectKind.OBJECT_KIND_RELATIONSHIP)
            .setId(objectType.getEntityType().getId())
            .build();
      case EVENT_TYPE:
        return ObjectTypeReference.newBuilder()
            .setObjectKind(ObjectKind.OBJECT_KIND_EVENT)
            .setId(objectType.getEntityType().getId())
            .build();
      case METRIC_TYPE:
        return ObjectTypeReference.newBuilder()
            .setObjectKind(ObjectKind.OBJECT_KIND_METRIC)
            .setId(objectType.getEntityType().getId())
            .build();
    }
    throw Status.INVALID_ARGUMENT.asRuntimeException();
  }
}
