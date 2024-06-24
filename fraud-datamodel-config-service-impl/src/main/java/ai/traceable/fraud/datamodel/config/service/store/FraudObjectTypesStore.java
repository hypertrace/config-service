package ai.traceable.fraud.datamodel.config.service.store;

import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectType;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectTypeReference;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface FraudObjectTypesStore {
  void upsertObjectTypes(RequestContext requestContext, List<ObjectType> objectTypes)
      throws Exception;

  Optional<ObjectType> getObjectType(
      RequestContext requestContext, ObjectTypeReference objectTypeReference) throws Exception;

  List<ObjectType> getAllObjectTypes(RequestContext requestContext, ObjectKind objectKind)
      throws Exception;

  void deleteObjectTypes(
      RequestContext requestContext, List<ObjectTypeReference> objectTypeReferences)
      throws Exception;
}
