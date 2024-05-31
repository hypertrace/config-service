package ai.traceable.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import ai.traceable.data.classification.config.service.DataClassificationResolutionCache.DataTypeProvenance;
import ai.traceable.data.classification.config.service.DataClassificationResolutionCache.DataTypeResolutionContext;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.DeleteDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.DeleteDataTypeResponse;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest.DataTypeFilter;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest.DataTypeOrdering;
import ai.traceable.data.classification.config.service.v1.GetDataTypesResponse;
import ai.traceable.data.classification.config.service.v1.SystemDataSetVersion;
import ai.traceable.data.classification.config.service.v1.UpdateDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.UpdateDataTypeResponse;
import com.google.protobuf.util.Structs;
import com.google.protobuf.util.Values;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.DeletedContextualConfigObject;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.context.RequestContext;
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
  @Mock DeletedSystemDatatypeStore mockDeletedSystemDataTypeStore;
  @Mock RedactionRulesDao mockRulesDao;
  @Mock DataClassificationConfig mockConfig;
  @Mock DataClassificationResolutionCache mockCache;
  @Mock ConfigChangeEventGenerator mockConfigChangeEventGenerator;

  @Mock RequestContext mockRequestContext;
  @Mock ContextualConfigObject<DataType> mockConfigObject;
  @Mock ContextualConfigObject<DataType> mockConfigObject2;
  @Mock DeletedContextualConfigObject<DataType> mockDeletedConfigObject;

  @Mock(answer = Answers.CALLS_REAL_METHODS)
  DataTypeResolutionContextComparator dataTypeResolutionContextComparator;

  @InjectMocks DataTypeManager dataTypeManager;

  @Test
  void doesNotApplyFilterSortAndResolutionIfNotRequested() {
    this.setUpMockData();
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
    this.setUpMockData();
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

  @Test
  void appliesOrphanFilter() {
    this.setUpMockData();
    assertEquals(
        GetDataTypesResponse.newBuilder().addDataTypes(TEST_ORPHAN_DATA_TYPE).build(),
        this.dataTypeManager.getDataTypesMatchingRequest(
            mockRequestContext,
            GetDataTypesRequest.newBuilder()
                .setFilter(DataTypeFilter.newBuilder().setOrphanTypes(true))
                .build()));

    assertEquals(
        GetDataTypesResponse.newBuilder()
            .addDataTypes(TEST_REGULAR_DATA_TYPE)
            .addDataTypes(TEST_SYSTEM_DATA_TYPE)
            .addDataTypes(TEST_LEGACY_DATA_TYPE)
            .putReferencedDataSetsById(TEST_DATA_SET.getId(), TEST_DATA_SET)
            .build(),
        this.dataTypeManager.getDataTypesMatchingRequest(
            mockRequestContext,
            GetDataTypesRequest.newBuilder()
                .setFilter(DataTypeFilter.newBuilder().setOrphanTypes(false))
                .build()));
  }

  @Test
  void deletesDatatype() {
    String id = "id-to-delete";
    when(this.mockConfig.getSystemDatatype(id)).thenReturn(Optional.empty());
    when(this.mockDataTypeStore.deleteObject(mockRequestContext, id))
        .thenReturn(Optional.of(mockDeletedConfigObject));
    assertEquals(
        DeleteDataTypeResponse.getDefaultInstance(),
        this.dataTypeManager.deleteDatatype(
            mockRequestContext, DeleteDataTypeRequest.newBuilder().setId(id).build()));
  }

  @Test
  void deletesSystemDatatype() {
    DataType datatypeToDelete = DataType.newBuilder().setId("id-to-delete").build();
    when(this.mockConfig.getSystemDatatype(datatypeToDelete.getId()))
        .thenReturn(Optional.of(datatypeToDelete));
    when(this.mockDataTypeStore.deleteObject(mockRequestContext, datatypeToDelete.getId()))
        .thenReturn(Optional.empty());
    when(this.mockDeletedSystemDataTypeStore.getData(mockRequestContext, datatypeToDelete.getId()))
        .thenReturn(Optional.empty());
    assertEquals(
        DeleteDataTypeResponse.getDefaultInstance(),
        this.dataTypeManager.deleteDatatype(
            mockRequestContext,
            DeleteDataTypeRequest.newBuilder().setId(datatypeToDelete.getId()).build()));
    verify(this.mockConfigChangeEventGenerator)
        .sendDeleteNotification(
            mockRequestContext,
            DataType.class.getName(),
            datatypeToDelete.getId(),
            Values.of(Structs.of("id", Values.of("id-to-delete"))));
  }

  @Test
  void throwsIfDatatypeMissing() {
    String id = "id-to-delete";
    when(this.mockConfig.getSystemDatatype(id)).thenReturn(Optional.empty());
    when(this.mockDataTypeStore.deleteObject(mockRequestContext, id)).thenReturn(Optional.empty());
    StatusRuntimeException thrownException =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                this.dataTypeManager.deleteDatatype(
                    mockRequestContext, DeleteDataTypeRequest.newBuilder().setId(id).build()));
    assertEquals(Code.NOT_FOUND, thrownException.getStatus().getCode());
  }

  @Test
  void updatesDatatype() {
    UpdateDataTypeRequest inputRequest =
        UpdateDataTypeRequest.newBuilder()
            .setId("id-to-update")
            .setRule(DataTypeRule.newBuilder().setName("new name"))
            .build();
    DataType updatedDatatype =
        DataType.newBuilder().setId(inputRequest.getId()).setRule(inputRequest.getRule()).build();
    when(this.mockDataTypeStore.getData(mockRequestContext, inputRequest.getId()))
        .thenReturn(Optional.of(DataType.getDefaultInstance()));
    when(this.mockDataTypeStore.upsertObject(mockRequestContext, updatedDatatype))
        .thenReturn(mockConfigObject);
    when(mockConfigObject.getData()).thenReturn(updatedDatatype);

    assertEquals(
        UpdateDataTypeResponse.newBuilder().setDataType(updatedDatatype).build(),
        this.dataTypeManager.updateDatatype(mockRequestContext, inputRequest));
  }

  @Test
  void throwsIfUpdateDatatypeMissing() {
    String id = "id-to-update";
    when(this.mockConfig.getSystemDatatype(id)).thenReturn(Optional.empty());
    when(this.mockDataTypeStore.getData(mockRequestContext, id)).thenReturn(Optional.empty());

    StatusRuntimeException thrownException =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                this.dataTypeManager.updateDatatype(
                    mockRequestContext, UpdateDataTypeRequest.newBuilder().setId(id).build()));
    assertEquals(Code.NOT_FOUND, thrownException.getStatus().getCode());
  }

  @Test
  void supportsUpdatingImpactedDatatypesOnDatasetDelete() {
    when(this.mockConfigObject.getData()).thenReturn(TEST_ORPHAN_DATA_TYPE);
    when(this.mockConfigObject2.getData()).thenReturn(TEST_REGULAR_DATA_TYPE);
    when(this.mockDataTypeStore.getAllObjects(mockRequestContext))
        .thenReturn(List.of(this.mockConfigObject, this.mockConfigObject2));
    when(this.mockDataTypeStore.getData(mockRequestContext, TEST_REGULAR_DATA_TYPE.getId()))
        .thenReturn(Optional.of(TEST_REGULAR_DATA_TYPE));
    this.dataTypeManager.tryRemoveDatasetFromAllDatatypes(
        mockRequestContext, TEST_DATA_SET.getId());

    verify(this.mockDataTypeStore, times(1))
        .upsertObject(
            mockRequestContext,
            TEST_REGULAR_DATA_TYPE.toBuilder()
                .setRule(TEST_REGULAR_DATA_TYPE.getRule().toBuilder().clearDataSetId())
                .build());
    verifyNoMoreInteractions(this.mockDataTypeStore);
  }

  private void setUpMockData() {
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
}
