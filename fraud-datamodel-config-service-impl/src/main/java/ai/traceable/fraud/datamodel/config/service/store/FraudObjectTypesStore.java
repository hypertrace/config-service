package ai.traceable.fraud.datamodel.config.service.store;

import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectType;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectTypeReference;
import java.util.List;
import java.util.Optional;

public interface FraudObjectTypesStore {
  void putObjectTypes(String tenantId, List<ObjectType> objectTypes) throws Exception;

  Optional<ObjectType> getObjectType(String tenantId, ObjectTypeReference objectTypeReference)
      throws Exception;

  List<ObjectType> getAllObjectTypes(String tenantId, ObjectKind objectKind) throws Exception;

  void deleteObjectTypes(String tenantId, List<ObjectTypeReference> objectTypeReferences)
      throws Exception;
}
