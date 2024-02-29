package ai.traceable.data.classification.config.service;

import static ai.traceable.data.classification.config.service.DataClassificationResolutionCache.DataTypeProvenance.FROM_LEGACY_REDACTION_RULE;
import static ai.traceable.data.classification.config.service.DataClassificationResolutionCache.DataTypeProvenance.ORPHAN_DATA_TYPE;
import static ai.traceable.data.classification.config.service.DataClassificationResolutionCache.DataTypeProvenance.RESOLVED_FROM_DATA_SET;
import static ai.traceable.data.classification.config.service.DataClassificationResolutionCache.DataTypeProvenance.STANDALONE_DATA_TYPE;
import static ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression.DATA_SUPPRESSION_OBFUSCATE;
import static ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression.DATA_SUPPRESSION_RAW;
import static ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression.DATA_SUPPRESSION_REDACT;
import static ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression.DATA_SUPPRESSION_UNSPECIFIED;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.data.classification.config.service.DataClassificationResolutionCache.DataTypeProvenance;
import ai.traceable.data.classification.config.service.DataClassificationResolutionCache.DataTypeResolutionContext;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataTypeResolutionContextComparatorTest {

  @InjectMocks DataTypeResolutionContextComparator comparator;

  @Test
  void testOrdering() {
    DataTypeResolutionContext orphanDataType1 =
        testContext(DATA_SUPPRESSION_UNSPECIFIED, ORPHAN_DATA_TYPE, 1);
    DataTypeResolutionContext redactLegacy2 =
        testContext(DATA_SUPPRESSION_REDACT, FROM_LEGACY_REDACTION_RULE, 2);
    DataTypeResolutionContext rawLegacy3 =
        testContext(DATA_SUPPRESSION_RAW, FROM_LEGACY_REDACTION_RULE, 3);
    DataTypeResolutionContext rawStandAlone4 =
        testContext(DATA_SUPPRESSION_RAW, STANDALONE_DATA_TYPE, 4);
    DataTypeResolutionContext redactStandAlone5 =
        testContext(DATA_SUPPRESSION_REDACT, STANDALONE_DATA_TYPE, 5);
    DataTypeResolutionContext redactStandAlone6 =
        testContext(DATA_SUPPRESSION_REDACT, STANDALONE_DATA_TYPE, 6);
    DataTypeResolutionContext redactDataSet1 =
        testContext(DATA_SUPPRESSION_REDACT, RESOLVED_FROM_DATA_SET, 1);
    DataTypeResolutionContext obfuscateDataSet2 =
        testContext(DATA_SUPPRESSION_OBFUSCATE, RESOLVED_FROM_DATA_SET, 2);

    List<DataTypeResolutionContext> expectedOrder =
        List.of(
            redactStandAlone5,
            redactStandAlone6,
            redactDataSet1,
            redactLegacy2,
            obfuscateDataSet2,
            rawStandAlone4,
            rawLegacy3,
            orphanDataType1);

    List<DataTypeResolutionContext> inputList = new ArrayList<>(expectedOrder);
    Collections.shuffle(inputList);
    inputList.sort(this.comparator);
    assertEquals(expectedOrder, inputList);
  }

  DataTypeResolutionContext testContext(
      DataSuppression suppression, DataTypeProvenance provenance, int encounterOrder) {
    DataType dataType =
        DataType.newBuilder()
            .setRule(DataTypeRule.newBuilder().setDataSuppression(suppression))
            .build();
    return new DataTypeResolutionContext(
        dataType, dataType, Collections.emptyList(), provenance, encounterOrder);
  }
}
