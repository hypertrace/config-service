package ai.traceable.data.classification.config.service;

import static ai.traceable.data.classification.config.service.DataTypeResolutionContextComparator.DATA_SENSITIVITY_COMPARATOR;
import static ai.traceable.data.classification.config.service.DataTypeResolutionContextComparator.DATA_SUPPRESSION_COMPARATOR;

import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.Sensitivity;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import java.util.Comparator;
import java.util.List;

class DataTypeResolver {
  static final Comparator<DataSet> DATA_SET_RESOLUTION_PRECEDENCE_COMPARATOR =
      Comparator.comparing((DataSet dataSet) -> dataSet.getInfo().getEnabled())
          .reversed() // enabled, then disabled
          .thenComparing(
              dataSet -> dataSet.getInfo().getDataSuppression(),
              DATA_SUPPRESSION_COMPARATOR.reversed());

  private static final DataTypeRule DEFAULT_RULE_VALUES =
      DataTypeRule.newBuilder()
          .setDataSuppression(DataSuppression.DATA_SUPPRESSION_RAW)
          .setSensitivity(Sensitivity.SENSITIVITY_LOW)
          .setEnabled(false)
          .build();

  @SuppressWarnings("deprecation")
  DataType resolve(DataType dataType, List<DataSet> dataSets) {
    if (dataSets.isEmpty()) {
      // If no data set relationship, we have nothing to resolve. However, if the data type was not
      // defined with all the necessary fields to operate on its own (an orphan created before data
      // type owned these fields), we'll set defaults (including disabling it).
      return this.isStandAloneDataType(dataType)
          ? dataType
          : this.assignDefaultsForOrphanDataType(dataType);
    }
    DataTypeRule.Builder ruleBuilder = dataType.getRule().toBuilder();
    // clear data set refs first to use the resolved result (which took these values as input)
    ruleBuilder.clearDataSetId();
    // Then add all references back and use the highest precedence (first) one to resolve any
    // missing fields
    dataSets.stream().map(DataSet::getId).forEach(ruleBuilder::addDataSetId);
    DataSet dataSetToInherit =
        dataSets.stream()
            .min(DATA_SET_RESOLUTION_PRECEDENCE_COMPARATOR) // first value
            .orElseThrow(); // already checked data sets isn't empty.
    if (!dataType.getRule().hasEnabled()) {
      ruleBuilder.setEnabled(dataSetToInherit.getInfo().getEnabled());
    }
    if (dataType.getRule().getDataSuppression() == DataSuppression.DATA_SUPPRESSION_UNSPECIFIED) {
      ruleBuilder.setDataSuppression(dataSetToInherit.getInfo().getDataSuppression());
    }
    if (dataType.getRule().getSensitivity() == Sensitivity.SENSITIVITY_UNSPECIFIED) {
      // For sensitivity, we take the highest even if this is not the value we'd typically inherit
      Sensitivity maxSensitivity =
          dataSets.stream()
              .map(dataset -> dataset.getInfo().getSensitivity())
              .max(DATA_SENSITIVITY_COMPARATOR)
              .orElseThrow();
      ruleBuilder.setSensitivity(maxSensitivity);
    }
    // Finally, assign defaults for any fields that could not be inherited
    return dataType.toBuilder()
        .setRule(DEFAULT_RULE_VALUES.toBuilder().mergeFrom(ruleBuilder.build()))
        .build();
  }

  boolean isStandAloneDataType(DataType dataType) {
    return dataType.getRule().getSensitivity() != Sensitivity.SENSITIVITY_UNSPECIFIED
        && dataType.getRule().getDataSuppression() != DataSuppression.DATA_SUPPRESSION_UNSPECIFIED
        && dataType.getRule().hasEnabled();
  }

  boolean isLegacyDataType(DataType dataType) {
    // Legacy data types are informational only, they don't contain any patterns
    return dataType.getRule().getScopedPatternsList().isEmpty();
  }

  private DataType assignDefaultsForOrphanDataType(DataType dataType) {
    return dataType.toBuilder()
        .setRule(DEFAULT_RULE_VALUES.toBuilder().mergeFrom(dataType.getRule()))
        .build();
  }
}
