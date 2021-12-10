package ai.traceable.data.classification.config.service;

import ai.traceable.data.classification.config.service.v1.DataType;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

public class DataTypeStore extends IdentifiedObjectStore<DataType> {
  private static final String DATA_CLASSIFICATION_DATA_TYPE_CONFIG_RESOURCE_NAME =
      "data-type-config";
  private static final String DATA_CLASSIFICATION_DATA_TYPE_CONFIG_RESOURCE_NAMESPACE =
      "data-classification";

  @Inject
  DataTypeStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        DATA_CLASSIFICATION_DATA_TYPE_CONFIG_RESOURCE_NAMESPACE,
        DATA_CLASSIFICATION_DATA_TYPE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<DataType> buildDataFromValue(Value value) {
    try {
      DataType.Builder builder = DataType.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(DataType object) {
    return ConfigProtoConverter.convertToValue(object);
  }

  @Override
  protected String getContextFromData(DataType object) {
    return object.getId();
  }
}
