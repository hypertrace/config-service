package ai.traceable.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.data.classification.config.service.DataClassificationResolutionCache.DataTypeProvenance;
import ai.traceable.data.classification.config.service.DataClassificationResolutionCache.DataTypeResolutionContext;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest.DataTypeFilter;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest.DataTypeOrdering;
import ai.traceable.data.classification.config.service.v1.GetDataTypesResponse;
import ai.traceable.data.classification.config.service.v1.SystemDataSetVersion;
import java.util.List;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataTypeManagerTest {

  private static final DataSet TEST_DATA_SET = DataSet.newBuilder().setId("dataset").build();
  private static final DataType TEST_LEGACY_DATA_TYPE =
      DataType.newBuilder()
          .setId("legacy")
          .setRule(DataTypeRule.newBuilder().setEnabled(true))
          .build();
  private static final DataTypeResolutionContext TEST_LEGACY_RESOLUTION_CONTEXT =
      new DataTypeResolutionContext(
          TEST_LEGACY_DATA_TYPE,
          TEST_LEGACY_DATA_TYPE.toBuilder().setId("legacy-resolved").build(),
          List.of(),
          DataTypeProvenance.FROM_LEGACY_REDACTION_RULE,
          1);
  private static final DataType TEST_SYSTEM_DATA_TYPE =
      DataType.newBuilder()
          .setId("system")
          .setRule(DataTypeRule.newBuilder().setEnabled(false))
          .build();
  private static final DataTypeResolutionContext TEST_SYSTEM_RESOLUTION_CONTEXT =
      new DataTypeResolutionContext(
          TEST_SYSTEM_DATA_TYPE,
          TEST_SYSTEM_DATA_TYPE.toBuilder().setId("system-resolved").build(),
          List.of(TEST_DATA_SET),
          DataTypeProvenance.RESOLVED_FROM_DATA_SET,
          1);

  private static final DataType TEST_ORPHAN_DATA_TYPE =
      DataType.newBuilder().setId("orphan").build();
  private static final DataTypeResolutionContext TEST_ORPHAN_RESOLUTION_CONTEXT =
      new DataTypeResolutionContext(
          TEST_ORPHAN_DATA_TYPE,
          TEST_ORPHAN_DATA_TYPE.toBuilder().setId("orphan-resolved").build(),
          List.of(),
          DataTypeProvenance.ORPHAN_DATA_TYPE,
          2);

  private static final DataType TEST_REGULAR_DATA_TYPE =
      DataType.newBuilder()
          .setId("regular")
          .setRule(DataTypeRule.newBuilder().setEnabled(true).addDataSetId(TEST_DATA_SET.getId()))
          .build();

  private static final DataTypeResolutionContext TEST_REGULAR_RESOLUTION_CONTEXT =
      new DataTypeResolutionContext(
          TEST_REGULAR_DATA_TYPE,
          TEST_REGULAR_DATA_TYPE.toBuilder().setId("regular-resolved").build(),
          List.of(TEST_DATA_SET),
          DataTypeProvenance.STANDALONE_DATA_TYPE,
          3);

  @Mock DataTypeStore mockDataTypeStore;
  @Mock RedactionRulesDao mockRulesDao;
  @Mock DataClassificationConfig mockConfig;
  @Mock DataClassificationResolutionCache mockCache;

  @Mock RequestContext mockRequestContext;
  @Mock ContextualConfigObject<DataType> mockConfigObject;
  @Mock ContextualConfigObject<DataType> mockConfigObject2;

  @Mock(answer = Answers.CALLS_REAL_METHODS)
  DataTypeResolutionContextComparator dataTypeResolutionContextComparator;

  @InjectMocks DataTypeManager dataTypeManager;

  @BeforeEach
  void beforeEachSetupData() {
    when(this.mockRulesDao.getAllDataTypesFromRedactionRules(mockRequestContext))
        .thenReturn(List.of(TEST_LEGACY_DATA_TYPE));
    when(this.mockConfigObject.getData()).thenReturn(TEST_ORPHAN_DATA_TYPE);
    when(this.mockConfigObject2.getData()).thenReturn(TEST_REGULAR_DATA_TYPE);
    when(this.mockDataTypeStore.getAllObjects(mockRequestContext))
        .thenReturn(List.of(this.mockConfigObject, this.mockConfigObject2));
    when(this.mockConfig.getSystemDataTypes(
            SystemDataSetVersion.SYSTEM_DATA_SET_VERSION_UNSPECIFIED))
        .thenReturn(List.of(TEST_SYSTEM_DATA_TYPE));
    when(this.mockCache.getDataTypeResolutions(
            mockRequestContext,
            List.of(
                TEST_ORPHAN_DATA_TYPE,
                TEST_REGULAR_DATA_TYPE,
                TEST_SYSTEM_DATA_TYPE,
                TEST_LEGACY_DATA_TYPE)))
        .thenReturn(
            List.of(
                TEST_ORPHAN_RESOLUTION_CONTEXT,
                TEST_REGULAR_RESOLUTION_CONTEXT,
                TEST_SYSTEM_RESOLUTION_CONTEXT,
                TEST_LEGACY_RESOLUTION_CONTEXT));
  }

  @Test
  void doesNotApplyFilterSortAndResolutionIfNotRequested() {
    assertEquals(
        GetDataTypesResponse.newBuilder()
            .addDataTypes(TEST_ORPHAN_DATA_TYPE)
            .addDataTypes(TEST_REGULAR_DATA_TYPE)
            .addDataTypes(TEST_SYSTEM_DATA_TYPE)
            .addDataTypes(TEST_LEGACY_DATA_TYPE)
            .putReferencedDataSetsById(TEST_DATA_SET.getId(), TEST_DATA_SET)
            .build(),
        this.dataTypeManager.getDataTypesMatchingRequest(
            mockRequestContext, GetDataTypesRequest.getDefaultInstance()));
  }

  @Test
  void appliesFilterSortAndResolutionIfRequested() {
    assertEquals(
        GetDataTypesResponse.newBuilder()
            .addDataTypes(TEST_ORPHAN_RESOLUTION_CONTEXT.getResolvedDataType())
            .addDataTypes(TEST_REGULAR_RESOLUTION_CONTEXT.getResolvedDataType())
            .addDataTypes(TEST_SYSTEM_RESOLUTION_CONTEXT.getResolvedDataType())
            .putReferencedDataSetsById(TEST_DATA_SET.getId(), TEST_DATA_SET)
            .build(),
        this.dataTypeManager.getDataTypesMatchingRequest(
            mockRequestContext,
            GetDataTypesRequest.newBuilder()
                .setFilter(DataTypeFilter.newBuilder().setLegacyTypes(false))
                .setResolveInheritedDetails(true)
                .build()));

    assertEquals(
        GetDataTypesResponse.newBuilder()
            .addDataTypes(TEST_ORPHAN_RESOLUTION_CONTEXT.getResolvedDataType())
            .addDataTypes(TEST_SYSTEM_RESOLUTION_CONTEXT.getResolvedDataType())
            .putReferencedDataSetsById(TEST_DATA_SET.getId(), TEST_DATA_SET)
            .build(),
        this.dataTypeManager.getDataTypesMatchingRequest(
            mockRequestContext,
            GetDataTypesRequest.newBuilder()
                .setFilter(DataTypeFilter.newBuilder().setEnabled(false))
                .setResolveInheritedDetails(true)
                .build()));

    assertEquals(
        GetDataTypesResponse.newBuilder()
            .addDataTypes(TEST_REGULAR_DATA_TYPE)
            .putReferencedDataSetsById(TEST_DATA_SET.getId(), TEST_DATA_SET)
            .build(),
        this.dataTypeManager.getDataTypesMatchingRequest(
            mockRequestContext,
            GetDataTypesRequest.newBuilder()
                .setFilter(DataTypeFilter.newBuilder().setLegacyTypes(false).setEnabled(true))
                .build()));

    assertEquals(
        GetDataTypesResponse.newBuilder()
            .addDataTypes(TEST_REGULAR_DATA_TYPE)
            .addDataTypes(TEST_SYSTEM_DATA_TYPE)
            .addDataTypes(TEST_LEGACY_DATA_TYPE)
            .addDataTypes(TEST_ORPHAN_DATA_TYPE)
            .putReferencedDataSetsById(TEST_DATA_SET.getId(), TEST_DATA_SET)
            .build(),
        this.dataTypeManager.getDataTypesMatchingRequest(
            mockRequestContext,
            GetDataTypesRequest.newBuilder()
                .setOrdering(DataTypeOrdering.DATA_TYPE_ORDERING_EVALUATION_PRIORITY)
                .build()));
  }
}
