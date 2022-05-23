package ai.traceable.data.classification.config.service;

import ai.traceable.data.classification.config.service.v1.DataSet;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

public class LegacyDataSetStore extends IdentifiedObjectStore<DataSet> {
  private static final String DATA_CLASSIFICATION_LEGACY_DATA_SET_CONFIG_RESOURCE_NAME =
      "legacy-data-set-config";
  private static final String DATA_CLASSIFICATION_DATA_SET_CONFIG_RESOURCE_NAMESPACE =
      "data-classification";

  @Inject
  LegacyDataSetStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        DATA_CLASSIFICATION_DATA_SET_CONFIG_RESOURCE_NAMESPACE,
        DATA_CLASSIFICATION_LEGACY_DATA_SET_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<DataSet> buildDataFromValue(Value value) {
    try {
      DataSet.Builder builder = DataSet.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(DataSet object) {
    return ConfigProtoConverter.convertToValue(object);
  }

  @Override
  protected String getContextFromData(DataSet object) {
    return object.getId();
  }
}
