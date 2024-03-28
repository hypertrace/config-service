package ai.traceable.data.classification.config.service;

import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.SystemDataSetVersion;
import com.google.common.collect.Maps;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;

class DataClassificationConfig {

  private static final String DATA_CLASSIFICATION_CONFIG_SERVICE =
      "data.classification.config.service";
  private static final String SYSTEM_DATASETS_RP1 = "system.datasets.rp1";
  private static final String SYSTEM_DATATYPES_RP1 = "system.datatypes.rp1";
  private static final String SYSTEM_DATASETS_RP2 = "system.datasets.rp2";
  private static final String SYSTEM_DATATYPES_RP2 = "system.datatypes.rp2";
  private final Map<String, DataSet> systemDataSetsRp1;
  private final Map<String, DataType> systemDataTypesRp1;
  private final Map<String, DataSet> systemDataSetsRp2;
  private final Map<String, DataType> systemDataTypesRp2;

  DataClassificationConfig(Config config) {
    Config dataClassificationConfig = config.getConfig(DATA_CLASSIFICATION_CONFIG_SERVICE);
    this.systemDataSetsRp1 =
        Maps.uniqueIndex(
            this.buildDataSetsList(dataClassificationConfig.getObjectList(SYSTEM_DATASETS_RP1)),
            DataSet::getId);
    this.systemDataTypesRp1 =
        Maps.uniqueIndex(
            this.buildDataTypesList(dataClassificationConfig.getObjectList(SYSTEM_DATATYPES_RP1)),
            DataType::getId);
    this.systemDataSetsRp2 =
        Maps.uniqueIndex(
            this.buildDataSetsList(dataClassificationConfig.getObjectList(SYSTEM_DATASETS_RP2)),
            DataSet::getId);
    this.systemDataTypesRp2 =
        Maps.uniqueIndex(
            this.buildDataTypesList(dataClassificationConfig.getObjectList(SYSTEM_DATATYPES_RP2)),
            DataType::getId);
  }

  boolean isSystemDataSet(String dataSetId) {
    return getSystemDataSet(dataSetId).isPresent();
  }

  List<DataSet> getSystemDataSets(SystemDataSetVersion systemDataSetVersion) {
    switch (systemDataSetVersion) {
      case SYSTEM_DATA_SET_VERSION_RP1:
        return List.copyOf(systemDataSetsRp1.values());
      case SYSTEM_DATA_SET_VERSION_UNSPECIFIED:
      default:
        return List.copyOf(systemDataSetsRp2.values());
    }
  }

  Optional<DataSet> getSystemDataSet(String dataSetId) {
    return Optional.ofNullable(systemDataSetsRp2.get(dataSetId))
        .or(() -> Optional.ofNullable(systemDataSetsRp1.get(dataSetId)));
  }

  List<DataType> getSystemDataTypes(SystemDataSetVersion systemDataSetVersion) {
    switch (systemDataSetVersion) {
      case SYSTEM_DATA_SET_VERSION_RP1:
        return List.copyOf(systemDataTypesRp1.values());
      case SYSTEM_DATA_SET_VERSION_UNSPECIFIED:
      default:
        return List.copyOf(systemDataTypesRp2.values());
    }
  }

  Optional<DataType> getSystemDatatype(String dataTypeId) {
    return Optional.ofNullable(systemDataTypesRp2.get(dataTypeId))
        .or(() -> Optional.ofNullable(systemDataTypesRp1.get(dataTypeId)));
  }

  private List<DataSet> buildDataSetsList(
      List<? extends com.typesafe.config.ConfigObject> configObjectList) {
    return configObjectList.stream()
        .map(this::buildDataSetFromConfig)
        .collect(Collectors.toUnmodifiableList());
  }

  private List<DataType> buildDataTypesList(
      List<? extends com.typesafe.config.ConfigObject> configObjectList) {
    return configObjectList.stream()
        .map(this::buildDataTypeFromConfig)
        .collect(Collectors.toUnmodifiableList());
  }

  @SneakyThrows
  private DataType buildDataTypeFromConfig(com.typesafe.config.ConfigObject configObject) {
    String jsonString = configObject.render();
    DataType.Builder builder = DataType.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
  }

  @SneakyThrows
  private DataSet buildDataSetFromConfig(com.typesafe.config.ConfigObject configObject) {
    String jsonString = configObject.render();
    DataSet.Builder builder = DataSet.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
  }
}
