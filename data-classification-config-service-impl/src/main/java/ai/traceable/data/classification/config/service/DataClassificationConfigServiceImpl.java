package ai.traceable.data.classification.config.service;

import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_DATA_SET_IDS;
import static java.util.function.Function.identity;
import static org.hypertrace.config.proto.converter.ConfigProtoConverter.convertToValue;

import ai.traceable.data.classification.config.service.impl.v1.DeletedSystemDataset.DeletedSystemDataSet;
import ai.traceable.data.classification.config.service.v1.CreateDataClassificationOverrideRequest;
import ai.traceable.data.classification.config.service.v1.CreateDataClassificationOverrideResponse;
import ai.traceable.data.classification.config.service.v1.CreateDataSetRequest;
import ai.traceable.data.classification.config.service.v1.CreateDataSetResponse;
import ai.traceable.data.classification.config.service.v1.CreateDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.CreateDataTypeResponse;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceImplBase;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverride;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideFilter;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideRule.DataClassificationOverrideScope;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DeleteDataClassificationOverridesRequest;
import ai.traceable.data.classification.config.service.v1.DeleteDataClassificationOverridesResponse;
import ai.traceable.data.classification.config.service.v1.DeleteDataSetRequest;
import ai.traceable.data.classification.config.service.v1.DeleteDataSetResponse;
import ai.traceable.data.classification.config.service.v1.DeleteDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.DeleteDataTypeResponse;
import ai.traceable.data.classification.config.service.v1.GetDataClassificationOverridesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataClassificationOverridesResponse;
import ai.traceable.data.classification.config.service.v1.GetDataSetRequest;
import ai.traceable.data.classification.config.service.v1.GetDataSetResponse;
import ai.traceable.data.classification.config.service.v1.GetDataSetsRequest;
import ai.traceable.data.classification.config.service.v1.GetDataSetsResponse;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesResponse;
import ai.traceable.data.classification.config.service.v1.UpdateDataClassificationOverrideRequest;
import ai.traceable.data.classification.config.service.v1.UpdateDataClassificationOverrideResponse;
import ai.traceable.data.classification.config.service.v1.UpdateDataSetRequest;
import ai.traceable.data.classification.config.service.v1.UpdateDataSetResponse;
import ai.traceable.data.classification.config.service.v1.UpdateDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.UpdateDataTypeResponse;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.DeletedContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class DataClassificationConfigServiceImpl extends DataClassificationConfigServiceImplBase {
  private final IdentifiedObjectStore<DataSet> dataSetStore;
  private final IdentifiedObjectStore<DeletedSystemDataSet> deletedSystemDatasetStore;
  private final IdentifiedObjectStore<DataClassificationOverride> dataClassificationOverrideStore;
  private final DataSetConfigRequestValidator dataSetConfigRequestValidator;
  private final DataTypeConfigRequestValidator dataTypeConfigRequestValidator;
  private final DataClassificationOverrideConfigRequestValidator
      dataClassificationOverrideConfigRequestValidator;
  private final Optional<ConfigChangeEventGenerator> configChangeEventGenerator;
  private final RedactionRulesDao redactionRulesDao;
  private final DataTypeManager dataTypeManager;
  private final DataClassificationConfig config;

  @Inject
  public DataClassificationConfigServiceImpl(
      DataSetStore dataSetStore,
      DeletedDataSetStore deletedSystemDatasetStore,
      DataClassificationOverrideStore dataClassificationOverrideStore,
      DataSetConfigRequestValidator dataSetConfigRequestValidator,
      DataTypeConfigRequestValidator dataTypeConfigRequestValidator,
      DataClassificationOverrideConfigRequestValidator
          dataClassificationOverrideConfigRequestValidator,
      ConfigChangeEventGenerator configChangeEventGenerator,
      RedactionRulesDao redactionRulesDao,
      DataTypeManager dataTypeManager,
      DataClassificationConfig config) {
    this.dataSetStore = dataSetStore;
    this.deletedSystemDatasetStore = deletedSystemDatasetStore;
    this.dataClassificationOverrideStore = dataClassificationOverrideStore;
    this.dataSetConfigRequestValidator = dataSetConfigRequestValidator;
    this.dataTypeConfigRequestValidator = dataTypeConfigRequestValidator;
    this.dataClassificationOverrideConfigRequestValidator =
        dataClassificationOverrideConfigRequestValidator;
    this.configChangeEventGenerator = Optional.ofNullable(configChangeEventGenerator);
    this.redactionRulesDao = redactionRulesDao;
    this.dataTypeManager = dataTypeManager;
    this.config = config;
  }

  @Override
  public void createDataType(
      CreateDataTypeRequest request, StreamObserver<CreateDataTypeResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.dataTypeConfigRequestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(this.dataTypeManager.createDatatype(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to create data type - {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getDataTypes(
      GetDataTypesRequest request, StreamObserver<GetDataTypesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.dataTypeConfigRequestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          this.dataTypeManager.getDataTypesMatchingRequest(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get data types - {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateDataType(
      UpdateDataTypeRequest request, StreamObserver<UpdateDataTypeResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.dataTypeConfigRequestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(this.dataTypeManager.updateDatatype(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to update data type - {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteDataType(
      DeleteDataTypeRequest request, StreamObserver<DeleteDataTypeResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.dataTypeConfigRequestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(this.dataTypeManager.deleteDatatype(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to delete data type - {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createDataSet(
      CreateDataSetRequest request, StreamObserver<CreateDataSetResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.dataSetConfigRequestValidator.validateOrThrow(requestContext, request);
      DataSet dataSet =
          DataSet.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setInfo(request.getInfo())
              .build();
      DataSet createdDataSet = this.dataSetStore.upsertObject(requestContext, dataSet).getData();
      responseObserver.onNext(
          CreateDataSetResponse.newBuilder().setDataSet(createdDataSet).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to create data set - {}", request, e);
      responseObserver.onError(e);
    }
  }

  /**
   * @deprecated This has been deprecated to standardize our API, as a data set can be fetched
   *     through the plural version. Further, the plural version allows overriding the system data
   *     set version which helps the caller get the appropriate system data sets for a given tenant
   *     based on their current state (which version TPAs are in use).
   */
  @Override
  @Deprecated
  public void getDataSet(
      GetDataSetRequest request, StreamObserver<GetDataSetResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.dataSetConfigRequestValidator.validateOrThrow(requestContext, request);
      Optional<DataSet> dataSetOptional =
          this.dataSetStore
              .getData(requestContext, request.getId())
              .or(() -> this.getSystemDataSet(requestContext, request.getId()))
              .or(
                  () ->
                      this.redactionRulesDao.getDataSetWithIdFromRedactionRules(
                          requestContext, request.getId()));
      DataSet dataSet =
          dataSetOptional.orElseThrow(
              () -> Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers()));
      responseObserver.onNext(GetDataSetResponse.newBuilder().setDataSet(dataSet).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get data set - {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getDataSets(
      GetDataSetsRequest request, StreamObserver<GetDataSetsResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.dataSetConfigRequestValidator.validateOrThrow(requestContext, request);
      List<String> deletedSystemDataSetsIds = getDeletedSystemDataSets(requestContext);
      List<DataSet> tenantDataSets = this.dataSetStore.getAllConfigData(requestContext);
      // filter out system data sets as we need to maintain order of system data sets
      List<DataSet> filteredTenantDataSets =
          tenantDataSets.stream()
              .filter(dataSet -> !this.config.isSystemDataSet(dataSet.getId()))
              .collect(Collectors.toUnmodifiableList());
      Map<String, DataSet> tenantDataSetsToIdMap =
          tenantDataSets.stream().collect(Collectors.toUnmodifiableMap(DataSet::getId, identity()));
      // filter out deleted system data sets
      // also if overridden by tenant, then pick the overridden one
      List<DataSet> filteredSystemDataSets =
          this.config.getSystemDataSets(request.getSystemDataSetVersion()).stream()
              .filter(dataSet -> !deletedSystemDataSetsIds.contains(dataSet.getId()))
              .map(
                  systemDataSet ->
                      tenantDataSetsToIdMap.getOrDefault(systemDataSet.getId(), systemDataSet))
              .collect(Collectors.toUnmodifiableList());
      List<DataSet> redactionRulesToDataSets =
          this.redactionRulesDao.getDataSetsFromRedactionRules(requestContext);
      responseObserver.onNext(
          GetDataSetsResponse.newBuilder()
              .addAllDataSets(filteredTenantDataSets)
              .addAllDataSets(filteredSystemDataSets)
              .addAllDataSets(redactionRulesToDataSets)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get data sets - {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateDataSet(
      UpdateDataSetRequest request, StreamObserver<UpdateDataSetResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.dataSetConfigRequestValidator.validateOrThrow(requestContext, request);
      String dataSetId = request.getId();
      DataSet upsertedDataSet;
      if (LEGACY_DATA_SET_IDS.contains(dataSetId)) {
        upsertedDataSet =
            this.redactionRulesDao.updateDataSet(requestContext, dataSetId, request.getInfo());
      } else {
        // Check that it exists
        this.dataSetStore
            .getData(requestContext, dataSetId)
            .or(() -> this.getSystemDataSet(requestContext, dataSetId))
            .orElseThrow(() -> Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers()));
        DataSet updatedDataSet =
            DataSet.newBuilder().setId(dataSetId).setInfo(request.getInfo()).build();
        upsertedDataSet = this.dataSetStore.upsertObject(requestContext, updatedDataSet).getData();
      }
      responseObserver.onNext(
          UpdateDataSetResponse.newBuilder().setDataSet(upsertedDataSet).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to update data set - {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteDataSet(
      DeleteDataSetRequest request, StreamObserver<DeleteDataSetResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.dataSetConfigRequestValidator.validateOrThrow(requestContext, request);
      String dataSetId = request.getId();
      if (LEGACY_DATA_SET_IDS.contains(dataSetId)) {
        this.redactionRulesDao.deleteDataSet(requestContext, dataSetId);
      } else {
        Optional<DeletedContextualConfigObject<DataSet>> optionalDeletedContextualConfigObject =
            this.dataSetStore.deleteObject(requestContext, dataSetId);
        Optional<DataSet> systemDataSetOptional = getSystemDataSet(requestContext, dataSetId);
        if (systemDataSetOptional.isPresent()) {
          DeletedSystemDataSet deletedSystemDataSet =
              DeletedSystemDataSet.newBuilder().setId(dataSetId).build();
          this.deletedSystemDatasetStore.upsertObject(requestContext, deletedSystemDataSet);
          sendSystemDataSetDeletionEvent(
              requestContext,
              optionalDeletedContextualConfigObject.isEmpty(),
              systemDataSetOptional.get());
        } else if (optionalDeletedContextualConfigObject.isEmpty()) {
          throw Status.NOT_FOUND.asRuntimeException();
        }
      }
      this.dataTypeManager.tryRemoveDatasetFromAllDatatypes(requestContext, dataSetId);
      responseObserver.onNext(DeleteDataSetResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to delete data set - {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createDataClassificationOverride(
      CreateDataClassificationOverrideRequest request,
      StreamObserver<CreateDataClassificationOverrideResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.dataClassificationOverrideConfigRequestValidator.validateOrThrow(
          requestContext, request);
      DataClassificationOverride dataClassificationOverride =
          DataClassificationOverride.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setDataClassificationOverrideRule(request.getDataClassificationOverrideRule())
              .build();
      DataClassificationOverride createdDataClassificationOverride =
          this.dataClassificationOverrideStore
              .upsertObject(requestContext, dataClassificationOverride)
              .getData();
      responseObserver.onNext(
          CreateDataClassificationOverrideResponse.newBuilder()
              .setCreatedDataClassificationOverride(createdDataClassificationOverride)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to create data classification override - {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getDataClassificationOverrides(
      GetDataClassificationOverridesRequest request,
      StreamObserver<GetDataClassificationOverridesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.dataClassificationOverrideConfigRequestValidator.validateOrThrow(
          requestContext, request);
      List<DataClassificationOverride> tenantDataClassificationOverrides =
          this.dataClassificationOverrideStore.getAllConfigData(requestContext);
      List<DataClassificationOverride> dataClassificationOverrides =
          getDataClassificationOverridesListByFilter(
              tenantDataClassificationOverrides, request.getFilter());
      responseObserver.onNext(
          GetDataClassificationOverridesResponse.newBuilder()
              .addAllDataClassificationOverrides(dataClassificationOverrides)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get data classification overrides - {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateDataClassificationOverride(
      UpdateDataClassificationOverrideRequest request,
      StreamObserver<UpdateDataClassificationOverrideResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.dataClassificationOverrideConfigRequestValidator.validateOrThrow(
          requestContext, request);
      DataClassificationOverride existingDataClassificationOverride =
          this.dataClassificationOverrideStore
              .getData(requestContext, request.getId())
              .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      DataClassificationOverride updatedDataClassificationOverride =
          existingDataClassificationOverride.toBuilder()
              .setDataClassificationOverrideRule(request.getDataClassificationOverrideRule())
              .build();
      DataClassificationOverride upsertedDataClassificationOverride =
          this.dataClassificationOverrideStore
              .upsertObject(requestContext, updatedDataClassificationOverride)
              .getData();
      responseObserver.onNext(
          UpdateDataClassificationOverrideResponse.newBuilder()
              .setUpdatedDataClassificationOverride(upsertedDataClassificationOverride)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to update data classification override - {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteDataClassificationOverrides(
      DeleteDataClassificationOverridesRequest request,
      StreamObserver<DeleteDataClassificationOverridesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.dataClassificationOverrideConfigRequestValidator.validateOrThrow(
          requestContext, request);
      List<DataClassificationOverride> tenantDataClassificationOverrides =
          this.dataClassificationOverrideStore.getAllConfigData(requestContext);
      List<String> dataClassificationOverrideIds =
          getDataClassificationOverridesListByFilter(
                  tenantDataClassificationOverrides, request.getFilter())
              .stream()
              .map(DataClassificationOverride::getId)
              .collect(Collectors.toUnmodifiableList());
      List<DataClassificationOverride> deletedDataClassificationOverrides =
          this.dataClassificationOverrideStore
              .deleteObjects(requestContext, dataClassificationOverrideIds)
              .stream()
              .map(DeletedContextualConfigObject::getDeletedData)
              .flatMap(Optional::stream)
              .collect(Collectors.toUnmodifiableList());
      responseObserver.onNext(
          DeleteDataClassificationOverridesResponse.newBuilder()
              .addAllDeletedDataClassificationOverrides(deletedDataClassificationOverrides)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to delete data classification overrides - {}", request, e);
      responseObserver.onError(e);
    }
  }

  @SneakyThrows
  private void sendSystemDataSetDeletionEvent(
      RequestContext requestContext, boolean isObjectEmpty, DataSet systemDataSet) {
    // send event only in case contextual object is empty
    // contextual object being empty implies, override doesn't exist,
    // therefore no datas set deletion event is sent so far
    if (isObjectEmpty && configChangeEventGenerator.isPresent()) {
      configChangeEventGenerator
          .get()
          .sendDeleteNotification(
              requestContext,
              DataSet.class.getName(),
              systemDataSet.getId(),
              convertToValue(systemDataSet));
    }
  }

  private Optional<DataSet> getSystemDataSet(RequestContext requestContext, String id) {
    if (this.config.isSystemDataSet(id)) {
      Optional<DeletedSystemDataSet> isSystemDataSetDeleted =
          this.deletedSystemDatasetStore.getData(requestContext, id);
      if (isSystemDataSetDeleted.isEmpty()) {
        return this.config.getSystemDataSet(id);
      }
    }
    return Optional.empty();
  }

  private List<String> getDeletedSystemDataSets(RequestContext requestContext) {
    return this.deletedSystemDatasetStore.getAllConfigData(requestContext).stream()
        .map(DeletedSystemDataSet::getId)
        .collect(Collectors.toList());
  }

  private List<DataClassificationOverride> getDataClassificationOverridesListByFilter(
      List<DataClassificationOverride> tenantDataClassificationOverrides,
      DataClassificationOverrideFilter filter) {
    switch (filter.getFilterCase()) {
      case ID_FILTER:
        {
          Set<String> filterSet = Set.copyOf(filter.getIdFilter().getIdsList());
          return tenantDataClassificationOverrides.stream()
              .filter(
                  tenantDataClassificationOverride ->
                      filterSet.contains(tenantDataClassificationOverride.getId()))
              .collect(Collectors.toUnmodifiableList());
        }
      case SCOPE_FILTER:
        {
          List<DataClassificationOverrideScope> scopesList =
              filter.getScopeFilter().getScopesList();
          Set<String> environmentFilterSet =
              scopesList.stream()
                  .filter(DataClassificationOverrideScope::hasEnvironmentScope)
                  .map(scope -> scope.getEnvironmentScope().getEnvironmentId())
                  .collect(Collectors.toUnmodifiableSet());
          return tenantDataClassificationOverrides.stream()
              .filter(
                  tenantDataClassificationOverride ->
                      environmentFilterSet.contains(
                          tenantDataClassificationOverride
                              .getDataClassificationOverrideRule()
                              .getScope()
                              .getEnvironmentScope()
                              .getEnvironmentId()))
              .collect(Collectors.toUnmodifiableList());
        }
      case LOGICAL_AND_FILTER:
        List<DataClassificationOverride> filteredList =
            new ArrayList<>(tenantDataClassificationOverrides);
        for (DataClassificationOverrideFilter innerFilter :
            filter.getLogicalAndFilter().getFiltersList()) {
          filteredList = getDataClassificationOverridesListByFilter(filteredList, innerFilter);
        }
        return Collections.unmodifiableList(filteredList);
      default:
        return tenantDataClassificationOverrides;
    }
  }
}
