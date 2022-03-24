package ai.traceable.data.classification.config.service;

import static ai.traceable.data.classification.config.service.v1.DataSetInfo.Sensitivity.SENSITIVITY_MEDIUM;
import static ai.traceable.data.classification.config.service.v1.DataSetInfo.Sensitivity.SENSITIVITY_UNSPECIFIED;

import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

public class DataSetStore extends IdentifiedObjectStore<DataSet> {
  private static final String DATA_CLASSIFICATION_DATA_SET_CONFIG_RESOURCE_NAME = "data-set-config";
  private static final String DATA_CLASSIFICATION_DATA_SET_CONFIG_RESOURCE_NAMESPACE =
      "data-classification";
  private static final DataSetInfo.Sensitivity DEFAULT_SENSITIVITY = SENSITIVITY_MEDIUM;

  @Inject
  DataSetStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        DATA_CLASSIFICATION_DATA_SET_CONFIG_RESOURCE_NAMESPACE,
        DATA_CLASSIFICATION_DATA_SET_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<DataSet> buildDataFromValue(Value value) {
    try {
      DataSet.Builder builder = DataSet.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      DataSetInfo dataSetInfo = builder.getInfo();
      if (dataSetInfo.getSensitivity().equals(SENSITIVITY_UNSPECIFIED)) {
        DataSetInfo updatedDataSetInfo =
            dataSetInfo.toBuilder().setSensitivity(DEFAULT_SENSITIVITY).build();
        builder.setInfo(updatedDataSetInfo);
      }
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
