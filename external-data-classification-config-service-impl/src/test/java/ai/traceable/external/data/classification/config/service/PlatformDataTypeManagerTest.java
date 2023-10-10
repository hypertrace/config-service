package ai.traceable.external.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.external.data.classification.config.service.legacy.LegacyRuleManager;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.PredicateSupportLevel;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PlatformDataTypeManagerTest {
  @Mock DataClassificationRulesDao dataClassificationRulesDao;
  @Mock LegacyRuleManager legacyRuleManager;
  @Mock DataClassificationRulesTranslator rulesTranslator;
  @InjectMocks PlatformDataTypeManager platformDataTypeManager;

  @Test
  void testGetEnabledDataSets() {
    RequestContext testContext = RequestContext.forTenantId("testGetEnabledDataSets");
    DataSet enabledDataSet =
        DataSet.newBuilder()
            .setId("enabled")
            .setInfo(DataSetInfo.newBuilder().setEnabled(true))
            .build();
    DataSet disabledDataSet =
        DataSet.newBuilder()
            .setId("disabled")
            .setInfo(DataSetInfo.newBuilder().setEnabled(false))
            .build();

    when(this.dataClassificationRulesDao.getAllDataSets(testContext))
        .thenReturn(List.of(enabledDataSet, disabledDataSet));
    assertEquals(
        List.of(enabledDataSet), this.platformDataTypeManager.getEnabledDataSets(testContext));
  }

  @Test
  void testGetDataTypes() {
    RequestContext testContext = RequestContext.forTenantId("testGetDataTypes");
    DataSet dataSet1 =
        DataSet.newBuilder()
            .setId("dataset-1")
            .setInfo(
                DataSetInfo.newBuilder()
                    .setName("datasetname-1")
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_OBFUSCATE)
                    .addDataTypeIds("datatype-1")
                    .addDataTypeIds("datatype-2"))
            .build();
    DataSet dataSet2 =
        DataSet.newBuilder()
            .setId("dataset-2")
            .setInfo(
                DataSetInfo.newBuilder()
                    .setName("datasetname-2")
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_REDACT)
                    .addDataTypeIds("datatype-2"))
            .build();
    DataSet legacyDataSet =
        DataSet.newBuilder()
            .setId("legacy-dataset")
            .setInfo(
                DataSetInfo.newBuilder()
                    .setName("legacy-dataset-1")
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_RAW)
                    .addDataTypeIds("datatype-3"))
            .build();
    DataType dataType1 =
        DataType.newBuilder()
            .setId("datatype-1")
            .setRule(
                DataTypeRule.newBuilder()
                    .setName("datatypename-1")
                    .addScopedPatterns(
                        DataTypeRule.ScopedPattern.newBuilder()
                            .setGlobalScope(DataTypeRule.GlobalScope.newBuilder())))
            .build();

    DataType dataType2 =
        DataType.newBuilder()
            .setId("datatype-2")
            .setRule(
                DataTypeRule.newBuilder()
                    .setName("datatypename-2")
                    .addScopedPatterns(
                        DataTypeRule.ScopedPattern.newBuilder()
                            .setEnvironmentScope(
                                DataTypeRule.EnvironmentScope.newBuilder()
                                    .addEnvironmentIds("env-1"))))
            .build();

    DataType dataType3 = DataType.newBuilder().setId("datatype-3").build();

    when(this.dataClassificationRulesDao.getAllDataTypes(testContext))
        .thenReturn(List.of(dataType1, dataType2, dataType3));
    when(this.legacyRuleManager.isLegacyDataSet(legacyDataSet)).thenReturn(true);
    when(this.legacyRuleManager.isLegacyDataSet(not(eq(legacyDataSet)))).thenReturn(false);

    List<ai.traceable.external.data.classification.config.service.v1.DataType> expectedResult =
        List.of(
            ai.traceable.external.data.classification.config.service.v1.DataType.newBuilder()
                .setDataTypeId(dataType1.getId())
                .build(),
            ai.traceable.external.data.classification.config.service.v1.DataType.newBuilder()
                .setDataTypeId(dataType2.getId())
                .build());

    when(this.rulesTranslator.translateDataTypes(
            List.of(
                new ResolvedPlatformDataType(dataType2, DataSuppression.DATA_SUPPRESSION_REDACT),
                new ResolvedPlatformDataType(
                    dataType1, DataSuppression.DATA_SUPPRESSION_OBFUSCATE)),
            Optional.of("env-1"),
            PredicateSupportLevel.PREDICATE_SUPPORT_LEVEL_LEAF_PATH_SEGMENT))
        .thenReturn(expectedResult);
    assertEquals(
        expectedResult,
        this.platformDataTypeManager.getDataTypes(
            testContext,
            List.of(dataSet1, dataSet2, legacyDataSet),
            Optional.of("env-1"),
            PredicateSupportLevel.PREDICATE_SUPPORT_LEVEL_LEAF_PATH_SEGMENT));
  }
}
