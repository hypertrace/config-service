package ai.traceable.data.classification.config.service;

import ai.traceable.data.classification.config.service.impl.v1.DeletedSystemDatatypeOuterClass.DeletedSystemDatatype;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

public class DeletedSystemDatatypeStore extends IdentifiedObjectStore<DeletedSystemDatatype> {
  private static final String DATA_CLASSIFICATION_DELETED_DATATYPE_CONFIG_RESOURCE_NAME =
      "deleted-system-datatype-config";
  private static final String DATA_CLASSIFICATION_DELETED_DATATYPE_CONFIG_RESOURCE_NAMESPACE =
      "data-classification";

  @Inject
  DeletedSystemDatatypeStore(ConfigServiceBlockingStub configServiceBlockingStub) {
    super(
        configServiceBlockingStub,
        DATA_CLASSIFICATION_DELETED_DATATYPE_CONFIG_RESOURCE_NAMESPACE,
        DATA_CLASSIFICATION_DELETED_DATATYPE_CONFIG_RESOURCE_NAME);
  }

  @Override
  protected Optional<DeletedSystemDatatype> buildDataFromValue(Value value) {
    try {
      DeletedSystemDatatype.Builder builder = DeletedSystemDatatype.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(DeletedSystemDatatype object) {
    return ConfigProtoConverter.convertToValue(object);
  }

  @Override
  protected String getContextFromData(DeletedSystemDatatype object) {
    return object.getId();
  }
}
