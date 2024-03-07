package ai.traceable.data.classification.cache.info;

import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataType;
import java.util.Map;
import lombok.Value;

@Value
public class DataClassificationInfo {
  private final Map<String, DataType> dataTypeIdToDataTypeMap;
  private final Map<String, DataSet> dataSetIdToDataSetMap;
}
