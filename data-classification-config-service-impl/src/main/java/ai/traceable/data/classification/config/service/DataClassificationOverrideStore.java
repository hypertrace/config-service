package ai.traceable.data.classification.config.service;

import ai.traceable.data.classification.config.service.v1.DataClassificationOverride;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class DataClassificationOverrideStore
    extends IdentifiedObjectStore<DataClassificationOverride> {
  private static final String
      DATA_CLASSIFICATION_DATA_CLASSIFICATION_OVERRIDE_CONFIG_RESOURCE_NAME =
          "data-classification-override-config";
  private static final String
      DATA_CLASSIFICATION_DATA_CLASSIFICATION_OVERRIDE_CONFIG_RESOURCE_NAMESPACE =
          "data-classification";

  @Inject
  DataClassificationOverrideStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        DATA_CLASSIFICATION_DATA_CLASSIFICATION_OVERRIDE_CONFIG_RESOURCE_NAMESPACE,
        DATA_CLASSIFICATION_DATA_CLASSIFICATION_OVERRIDE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<DataClassificationOverride> buildDataFromValue(Value value) {
    try {
      DataClassificationOverride.Builder builder = DataClassificationOverride.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(DataClassificationOverride object) {
    return ConfigProtoConverter.convertToValue(object);
  }

  @Override
  protected String getContextFromData(DataClassificationOverride object) {
    return object.getId();
  }
}
