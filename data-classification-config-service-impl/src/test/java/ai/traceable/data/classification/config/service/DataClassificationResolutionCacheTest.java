package ai.traceable.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.data.classification.config.service.DataClassificationResolutionCache.DataTypeProvenance;
import ai.traceable.data.classification.config.service.DataClassificationResolutionCache.DataTypeResolutionContext;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.GetDataSetsRequest;
import ai.traceable.data.classification.config.service.v1.GetDataSetsResponse;
import java.util.List;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataClassificationResolutionCacheTest {

  @Mock(answer = Answers.RETURNS_SELF)
  DataClassificationConfigServiceBlockingStub mockStub;

  @Mock DataTypeResolver mockResolver;
  @Mock ClientConfig clientConfig;
  RequestContext testRequestContext =
      RequestContext.forTenantId("DataClassificationResolutionCacheTest");
  @InjectMocks DataClassificationResolutionCache cache;

  @Test
  void testBuildingContexts() {
    DataSet dataSet1 =
        DataSet.newBuilder()
            .setId("ds1")
            .setInfo(
                DataSetInfo.newBuilder()
                    .addDataTypeIds("dt2-fromdataset")
                    .addDataTypeIds("dt3-legacy"))
            .build();
    DataSet dataSet2 =
        DataSet.newBuilder()
            .setId("ds2")
            .setInfo(
                DataSetInfo.newBuilder()
                    .addDataTypeIds("dt1-standalone")
                    .addDataTypeIds("dt2-fromdataset"))
            .build();
    when(this.mockStub.getDataSets(GetDataSetsRequest.getDefaultInstance()))
        .thenReturn(
            GetDataSetsResponse.newBuilder().addDataSets(dataSet1).addDataSets(dataSet2).build());
    DataType dataType1 =
        DataType.newBuilder()
            .setId("dt1-standalone")
            .setRule(DataTypeRule.newBuilder().addDataSetId(dataSet1.getId()))
            .build();
    when(this.mockResolver.resolve(dataType1, List.of(dataSet1, dataSet2))).thenReturn(dataType1);
    when(this.mockResolver.isLegacyDataType(dataType1)).thenReturn(false);
    when(this.mockResolver.isStandAloneDataType(dataType1)).thenReturn(true);
    DataType dataType2 = DataType.newBuilder().setId("dt2-fromdataset").build();
    when(this.mockResolver.resolve(dataType2, List.of(dataSet1, dataSet2))).thenReturn(dataType2);
    when(this.mockResolver.isLegacyDataType(dataType2)).thenReturn(false);
    when(this.mockResolver.isStandAloneDataType(dataType2)).thenReturn(false);
    DataType dataType3 = DataType.newBuilder().setId("dt3-legacy").build();
    when(this.mockResolver.resolve(dataType3, List.of(dataSet1))).thenReturn(dataType3);
    when(this.mockResolver.isLegacyDataType(dataType3)).thenReturn(true);
    DataType dataType4 = DataType.newBuilder().setId("dt4-orphan").build();
    when(this.mockResolver.resolve(dataType4, List.of())).thenReturn(dataType4);
    when(this.mockResolver.isLegacyDataType(dataType4)).thenReturn(false);
    when(this.mockResolver.isStandAloneDataType(dataType4)).thenReturn(false);

    List<DataTypeResolutionContext> expectedResult =
        List.of(
            new DataTypeResolutionContext(
                dataType1,
                dataType1,
                List.of(dataSet1, dataSet2),
                DataTypeProvenance.STANDALONE_DATA_TYPE,
                0),
            new DataTypeResolutionContext(
                dataType2,
                dataType2,
                List.of(dataSet1, dataSet2),
                DataTypeProvenance.RESOLVED_FROM_DATA_SET,
                0),
            new DataTypeResolutionContext(
                dataType3,
                dataType3,
                List.of(dataSet1),
                DataTypeProvenance.FROM_LEGACY_REDACTION_RULE,
                2),
            new DataTypeResolutionContext(
                dataType4, dataType4, List.of(), DataTypeProvenance.ORPHAN_DATA_TYPE, 3));
    assertEquals(
        expectedResult,
        cache.getDataTypeResolutions(
            testRequestContext, List.of(dataType1, dataType2, dataType3, dataType4)));
  }

  @Test
  void correctlyMergesDataTypesFromOutOfOrderDataSets() {
    // the encounter order for data sets should be based on their precedence
    DataSet dataSet1 =
        DataSet.newBuilder()
            .setId("ds1")
            .setInfo(
                DataSetInfo.newBuilder()
                    .addDataTypeIds("dt2")
                    .addDataTypeIds("dt1")
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_OBFUSCATE))
            .build();
    DataSet dataSet2 =
        DataSet.newBuilder()
            .setId("ds2")
            .setInfo(
                DataSetInfo.newBuilder()
                    .addDataTypeIds("dt1")
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_REDACT))
            .build();
    when(this.mockStub.getDataSets(GetDataSetsRequest.getDefaultInstance()))
        .thenReturn(
            GetDataSetsResponse.newBuilder().addDataSets(dataSet1).addDataSets(dataSet2).build());
    DataType dataType1 = DataType.newBuilder().setId("dt1").build();
    when(this.mockResolver.resolve(dataType1, List.of(dataSet2, dataSet1))).thenReturn(dataType1);
    when(this.mockResolver.isLegacyDataType(dataType1)).thenReturn(false);
    when(this.mockResolver.isStandAloneDataType(dataType1)).thenReturn(false);
    DataType dataType2 = DataType.newBuilder().setId("dt2").build();
    when(this.mockResolver.resolve(dataType2, List.of(dataSet1))).thenReturn(dataType2);
    when(this.mockResolver.isLegacyDataType(dataType2)).thenReturn(false);
    when(this.mockResolver.isStandAloneDataType(dataType2)).thenReturn(false);

    // They're returned in the provided order, but the encounter order is reversed when determining
    // precedence
    List<DataTypeResolutionContext> expectedResult =
        List.of(
            new DataTypeResolutionContext(
                dataType2,
                dataType2,
                List.of(dataSet1),
                DataTypeProvenance.RESOLVED_FROM_DATA_SET,
                1),
            new DataTypeResolutionContext(
                dataType1,
                dataType1,
                List.of(dataSet2, dataSet1),
                DataTypeProvenance.RESOLVED_FROM_DATA_SET,
                0));
    assertEquals(
        expectedResult,
        cache.getDataTypeResolutions(testRequestContext, List.of(dataType2, dataType1)));
  }

  @Test
  void reevaluatesResolutionsOnDatasetChange() {
    DataType datatype = DataType.newBuilder().setId("dt1").build();

    // At first, return empty for datasets
    when(this.mockStub.getDataSets(GetDataSetsRequest.getDefaultInstance()))
        .thenReturn(GetDataSetsResponse.getDefaultInstance());

    when(this.mockResolver.isLegacyDataType(datatype)).thenReturn(false);
    when(this.mockResolver.isStandAloneDataType(datatype)).thenReturn(false);
    when(this.mockResolver.resolve(datatype, List.of())).thenReturn(datatype);

    // They're returned in the provided order, but the encounter order is reversed when determining
    // precedence
    List<DataTypeResolutionContext> expectedResult =
        List.of(
            new DataTypeResolutionContext(
                datatype, datatype, List.of(), DataTypeProvenance.ORPHAN_DATA_TYPE, 0));

    assertEquals(
        expectedResult, cache.getDataTypeResolutions(testRequestContext, List.of(datatype)));

    DataSet newDataset =
        DataSet.newBuilder()
            .setId("ds1")
            .setInfo(
                DataSetInfo.newBuilder()
                    .addDataTypeIds(datatype.getId())
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_OBFUSCATE))
            .build();
    DataType resolutionResult = DataType.newBuilder().setId("resolved-dt1").build();

    // Now we update the mock to simulate a new data set being created
    when(this.mockStub.getDataSets(GetDataSetsRequest.getDefaultInstance()))
        .thenReturn(GetDataSetsResponse.newBuilder().addDataSets(newDataset).build());
    when(this.mockResolver.resolve(datatype, List.of(newDataset))).thenReturn(resolutionResult);

    expectedResult =
        List.of(
            new DataTypeResolutionContext(
                datatype,
                resolutionResult,
                List.of(newDataset),
                DataTypeProvenance.RESOLVED_FROM_DATA_SET,
                0));

    assertEquals(
        expectedResult, cache.getDataTypeResolutions(testRequestContext, List.of(datatype)));
  }
}
