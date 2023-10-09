package ai.traceable.external.data.classification.config.service;

import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataType;
import lombok.Value;

@Value
class ResolvedPlatformDataType {
  DataType dataType;
  DataSuppression suppression;
}
