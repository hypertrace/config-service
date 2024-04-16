package ai.traceable.data.classification.config.service;

import static ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression.DATA_SUPPRESSION_OBFUSCATE;
import static ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression.DATA_SUPPRESSION_RAW;
import static ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression.DATA_SUPPRESSION_REDACT;
import static ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression.DATA_SUPPRESSION_UNSPECIFIED;
import static ai.traceable.data.classification.config.service.v1.DataSetInfo.Sensitivity.SENSITIVITY_CRITICAL;
import static ai.traceable.data.classification.config.service.v1.DataSetInfo.Sensitivity.SENSITIVITY_HIGH;
import static ai.traceable.data.classification.config.service.v1.DataSetInfo.Sensitivity.SENSITIVITY_LOW;
import static ai.traceable.data.classification.config.service.v1.DataSetInfo.Sensitivity.SENSITIVITY_MEDIUM;
import static ai.traceable.data.classification.config.service.v1.DataSetInfo.Sensitivity.SENSITIVITY_UNSPECIFIED;

import ai.traceable.data.classification.config.service.DataClassificationResolutionCache.DataTypeResolutionContext;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.Sensitivity;
import com.google.common.collect.Ordering;
import java.util.Comparator;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DataTypeResolutionContextComparator implements Comparator<DataTypeResolutionContext> {
  // Evaluation priority sorts by:
  // 1. requested suppression behavior. Redacting rules always have precedence, then obfuscating
  // 2. the "provenance" of the data type - that is, standalone data types that do not rely on
  // any other config to define their behavior go first. Next are our data types that
  // inherit their behavior from data sets. Finally, any data types representing legacy redaction
  // rules go lost.
  // 3. We finally sort these based on their encounter order
  //   - For standalone data types: the order they're defined
  //   - For data set-derived types: the order the sorted data sets define the data type references
  //   - For legacy redaction types: the order the legacy redaction rules are defined.
  //
  // In summary, this means this algorithm exactly matches the prior behavior while new stand alone
  // data types are given precedence within their transformation category.

  static final Comparator<DataSuppression> DATA_SUPPRESSION_COMPARATOR =
      Ordering.explicit(
          DATA_SUPPRESSION_UNSPECIFIED,
          DATA_SUPPRESSION_RAW,
          DataSuppression.UNRECOGNIZED,
          DATA_SUPPRESSION_OBFUSCATE,
          DATA_SUPPRESSION_REDACT);

  static final Comparator<Sensitivity> DATA_SENSITIVITY_COMPARATOR =
      Ordering.explicit(
          SENSITIVITY_UNSPECIFIED,
          Sensitivity.UNRECOGNIZED,
          SENSITIVITY_LOW,
          SENSITIVITY_MEDIUM,
          SENSITIVITY_HIGH,
          SENSITIVITY_CRITICAL);
  private static final Comparator<DataTypeResolutionContext> DELEGATE =
      Comparator.comparing(
              DataTypeResolutionContextComparator::extractSuppression,
              DATA_SUPPRESSION_COMPARATOR.reversed()) // highest to lowest
          .thenComparing(DataTypeResolutionContext::getProvenance)
          .thenComparingInt(DataTypeResolutionContext::getEncounterOrder);

  @Override
  public int compare(DataTypeResolutionContext context1, DataTypeResolutionContext context2) {
    return DELEGATE.compare(context1, context2);
  }

  private static DataSuppression extractSuppression(
      DataTypeResolutionContext dataTypeResolutionContext) {
    return dataTypeResolutionContext.getResolvedDataType().getRule().getDataSuppression();
  }
}
