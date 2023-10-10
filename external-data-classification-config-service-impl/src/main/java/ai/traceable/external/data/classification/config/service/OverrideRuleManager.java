package ai.traceable.external.data.classification.config.service;

import ai.traceable.data.classification.config.service.v1.DataClassificationOverride;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideRule.DataSuppressionOverride;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.EnvironmentFilter;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
class OverrideRuleManager {
  private final DataClassificationRulesDao dataClassificationRulesDao;
  private final DataClassificationRulesTranslator dataClassificationRulesTranslator;

  List<DataClassificationOverride> getOverrides(
      RequestContext requestContext, EnvironmentFilter environmentFilter) {
    if (environmentFilter.getEnvironmentName().isBlank()) {
      return this.dataClassificationRulesDao.getUnscopedDataSuppressionOverrideRules(
          requestContext);
    }
    return this.dataClassificationRulesDao.getDataSuppressionOverrideRulesForEnvironment(
        requestContext, environmentFilter.getEnvironmentName());
  }

  List<DataType> applyOverrides(
      List<DataType> dataTypes, List<DataClassificationOverride> overrides) {
    return dataTypes.stream()
        .map(dataType -> this.applyOverrides(dataType, overrides))
        .collect(Collectors.toUnmodifiableList());
  }

  List<DataType> applyOverridesAndFilterRawRules(
      List<DataType> dataTypes, List<DataClassificationOverride> overrides) {
    return this.filterRawRules(this.applyOverrides(dataTypes, overrides));
  }

  private List<DataType> filterRawRules(List<DataType> dataTypes) {
    return dataTypes.stream()
        .filter(type -> type.getSessionIdentifier() || type.hasTransformation())
        .collect(Collectors.toUnmodifiableList());
  }

  private DataType applyOverrides(
      DataType originalDataType, List<DataClassificationOverride> overrides) {
    DataType overriddenDataType = originalDataType;
    for (DataClassificationOverride override : overrides) {
      overriddenDataType = this.applyOverride(overriddenDataType, override);
    }
    return overriddenDataType;
  }

  private DataType applyOverride(DataType dataType, DataClassificationOverride override) {
    switch (override.getDataClassificationOverrideRule().getOverrideCase()) {
      case DATA_SUPPRESSION_OVERRIDE:
        return this.applySuppressionOverride(
            dataType, override.getDataClassificationOverrideRule().getDataSuppressionOverride());
      case OVERRIDE_NOT_SET:
        log.warn("Unexpected unset Data Classification override: {}", override);
        return dataType;
      default:
        log.warn("Ignoring unknown Data Classification override: {}", override);
        return dataType;
    }
  }

  private DataType applySuppressionOverride(
      DataType dataType, DataSuppressionOverride suppressionOverride) {
    if (suppressionOverride.getDataSuppression() == DataSuppression.DATA_SUPPRESSION_RAW) {
      return dataType.toBuilder().clearTransformation().build();
    }
    return this.dataClassificationRulesTranslator
        .translateDataSuppression(suppressionOverride.getDataSuppression())
        .map(transformation -> dataType.toBuilder().setTransformation(transformation))
        .map(DataType.Builder::build)
        .orElse(dataType);
  }
}
