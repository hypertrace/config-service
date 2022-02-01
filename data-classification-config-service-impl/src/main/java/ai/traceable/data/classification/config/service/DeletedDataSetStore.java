package ai.traceable.data.classification.config.service;

import ai.traceable.data.classification.config.service.impl.v1.DeletedSystemDataset.DeletedSystemDataSet;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

public class DeletedDataSetStore extends IdentifiedObjectStore<DeletedSystemDataSet> {
  private static final String DATA_CLASSIFICATION_DELETED_DATA_SET_CONFIG_RESOURCE_NAME =
      "deleted-system-data-set-config";
  private static final String DATA_CLASSIFICATION_DELETED_DATA_SET_CONFIG_RESOURCE_NAMESPACE =
      "data-classification";

  @Inject
  DeletedDataSetStore(ConfigServiceBlockingStub configServiceBlockingStub) {
    super(
        configServiceBlockingStub,
        DATA_CLASSIFICATION_DELETED_DATA_SET_CONFIG_RESOURCE_NAMESPACE,
        DATA_CLASSIFICATION_DELETED_DATA_SET_CONFIG_RESOURCE_NAME);
  }

  @Override
  protected Optional<DeletedSystemDataSet> buildDataFromValue(Value value) {
    try {
      DeletedSystemDataSet.Builder builder = DeletedSystemDataSet.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(DeletedSystemDataSet object) {
    return ConfigProtoConverter.convertToValue(object);
  }

  @Override
  protected String getContextFromData(DeletedSystemDataSet object) {
    return object.getId();
  }
}
