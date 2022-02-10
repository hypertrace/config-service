package ai.traceable.data.classification.config.service;

import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_DATA_SET_IDS;
import static java.util.function.Function.identity;
import static org.hypertrace.config.proto.converter.ConfigProtoConverter.convertToValue;

import ai.traceable.data.classification.config.service.impl.v1.DeletedSystemDataset.DeletedSystemDataSet;
import ai.traceable.data.classification.config.service.v1.CreateDataSetRequest;
import ai.traceable.data.classification.config.service.v1.CreateDataSetResponse;
import ai.traceable.data.classification.config.service.v1.CreateDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.CreateDataTypeResponse;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceImplBase;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DeleteDataSetRequest;
import ai.traceable.data.classification.config.service.v1.DeleteDataSetResponse;
import ai.traceable.data.classification.config.service.v1.DeleteDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.DeleteDataTypeResponse;
import ai.traceable.data.classification.config.service.v1.GetDataSetRequest;
import ai.traceable.data.classification.config.service.v1.GetDataSetResponse;
import ai.traceable.data.classification.config.service.v1.GetDataSetsRequest;
import ai.traceable.data.classification.config.service.v1.GetDataSetsResponse;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesResponse;
import ai.traceable.data.classification.config.service.v1.UpdateDataSetRequest;
import ai.traceable.data.classification.config.service.v1.UpdateDataSetResponse;
import ai.traceable.data.classification.config.service.v1.UpdateDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.UpdateDataTypeResponse;
import com.google.inject.Inject;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DataClassificationConfigServiceImpl extends DataClassificationConfigServiceImplBase {
  private final IdentifiedObjectStore<DataSet> dataSetStore;
  private final IdentifiedObjectStore<DataType> dataTypeStore;
  private final IdentifiedObjectStore<DeletedSystemDataSet> deletedDataSetStore;
  private final DataSetConfigRequestValidator dataSetConfigRequestValidator;
  private final DataTypeConfigRequestValidator dataTypeConfigRequestValidator;
  private static final String DATA_CLASSIFICATION_CONFIG_SERVICE =
      "data.classification.config.service";
  private static final String SYSTEM_DATASETS = "system.datasets";
  private static final String SYSTEM_DATATYPES = "system.datatypes";
  private final List<DataSet> systemDataSets;
  private final List<DataType> systemDataTypes;
  private final Map<String, DataSet> systemDataSetsToIdMap;
  private final Optional<ConfigChangeEventGenerator> configChangeEventGenerator;
  private final RedactionRulesDao redactionRulesDao;

  @Inject
  public DataClassificationConfigServiceImpl(
      DataSetStore dataSetStore,
      DataTypeStore dataTypeStore,
      DeletedDataSetStore deletedDataSetStore,
      DataSetConfigRequestValidator dataSetConfigRequestValidator,
      DataTypeConfigRequestValidator dataTypeConfigRequestValidator,
      Config config,
      ConfigChangeEventGenerator configChangeEventGenerator,
      RedactionRulesDao redactionRulesDao) {
    this.dataSetStore = dataSetStore;
    this.dataTypeStore = dataTypeStore;
    this.deletedDataSetStore = deletedDataSetStore;
    this.dataSetConfigRequestValidator = dataSetConfigRequestValidator;
    this.dataTypeConfigRequestValidator = dataTypeConfigRequestValidator;
    this.configChangeEventGenerator = Optional.ofNullable(configChangeEventGenerator);
    this.redactionRulesDao = redactionRulesDao;
    List<? extends com.typesafe.config.ConfigObject> systemDataSetsObjectList = null;
    List<? extends com.typesafe.config.ConfigObject> systemDataTypesObjectList = null;
    if (config.hasPath(DATA_CLASSIFICATION_CONFIG_SERVICE)) {
      Config dataClassificationConfig = config.getConfig(DATA_CLASSIFICATION_CONFIG_SERVICE);
      if (dataClassificationConfig.hasPath(SYSTEM_DATASETS)) {
        systemDataSetsObjectList = dataClassificationConfig.getObjectList(SYSTEM_DATASETS);
      }
      if (dataClassificationConfig.hasPath(SYSTEM_DATATYPES)) {
        systemDataTypesObjectList = dataClassificationConfig.getObjectList(SYSTEM_DATATYPES);
      }
    }
    if (systemDataSetsObjectList != null) {
      systemDataSets = buildSystemDataSetsList(systemDataSetsObjectList);
      systemDataSetsToIdMap =
          systemDataSets.stream().collect(Collectors.toUnmodifiableMap(DataSet::getId, identity()));
    } else {
      systemDataSets = Collections.emptyList();
      systemDataSetsToIdMap = Collections.emptyMap();
    }

    if (systemDataTypesObjectList != null) {
      systemDataTypes = buildSystemDataTypesList(systemDataTypesObjectList);
    } else {
      systemDataTypes = Collections.emptyList();
    }
  }

  @Override
  public void createDataType(
      CreateDataTypeRequest request, StreamObserver<CreateDataTypeResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.dataTypeConfigRequestValidator.validateOrThrow(requestContext, request);
      DataType dataType =
          DataType.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setRule(request.getRule())
              .build();
      DataType createdDataType =
          this.dataTypeStore.upsertObject(requestContext, dataType).getData();
      responseObserver.onNext(
          CreateDataTypeResponse.newBuilder().setDataType(createdDataType).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getDataTypes(
      GetDataTypesRequest request, StreamObserver<GetDataTypesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.dataTypeConfigRequestValidator.validateOrThrow(requestContext, request);
      List<DataType> tenantDataTypes =
          this.dataTypeStore.getAllObjects(requestContext).stream()
              .map(ConfigObject::getData)
              .collect(Collectors.toUnmodifiableList());
      Map<String, DataType> tenantDataTypesToIdMap =
          tenantDataTypes.stream()
              .collect(Collectors.toUnmodifiableMap(DataType::getId, identity()));
      List<DataType> filteredSystemDataTypes =
          systemDataTypes.stream()
              .filter(dataType -> !tenantDataTypesToIdMap.containsKey(dataType.getId()))
              .collect(Collectors.toUnmodifiableList());
      List<DataType> convertedRedactionRules =
          redactionRulesDao.getAllDataTypesFromRedactionRules(requestContext);
      responseObserver.onNext(
          GetDataTypesResponse.newBuilder()
              .addAllDataTypes(tenantDataTypes)
              .addAllDataTypes(filteredSystemDataTypes)
              .addAllDataTypes(convertedRedactionRules)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateDataType(
      UpdateDataTypeRequest request, StreamObserver<UpdateDataTypeResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.dataTypeConfigRequestValidator.validateOrThrow(requestContext, request);
      DataType existingDataType =
          this.dataTypeStore
              .getData(requestContext, request.getId())
              .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      DataType updatedDataType = existingDataType.toBuilder().setRule(request.getRule()).build();
      DataType upsertedDataType =
          this.dataTypeStore.upsertObject(requestContext, updatedDataType).getData();
      responseObserver.onNext(
          UpdateDataTypeResponse.newBuilder().setDataType(upsertedDataType).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteDataType(
      DeleteDataTypeRequest request, StreamObserver<DeleteDataTypeResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.dataTypeConfigRequestValidator.validateOrThrow(requestContext, request);
      this.dataTypeStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      responseObserver.onNext(DeleteDataTypeResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
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
      responseObserver.onError(e);
    }
  }

  @Override
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
      DataSet dataSet = dataSetOptional.orElseThrow(Status.NOT_FOUND::asRuntimeException);
      responseObserver.onNext(GetDataSetResponse.newBuilder().setDataSet(dataSet).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
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
      List<DataSet> tenantDataSets =
          this.dataSetStore.getAllObjects(requestContext).stream()
              .map(ConfigObject::getData)
              .collect(Collectors.toUnmodifiableList());
      Map<String, DataSet> tenantDataSetsToIdMap =
          tenantDataSets.stream().collect(Collectors.toUnmodifiableMap(DataSet::getId, identity()));
      List<DataSet> filteredSystemDataSets =
          systemDataSets.stream()
              .filter(
                  dataSet ->
                      !tenantDataSetsToIdMap.containsKey(dataSet.getId())
                          && !deletedSystemDataSetsIds.contains(dataSet.getId()))
              .collect(Collectors.toUnmodifiableList());
      List<DataSet> redactionRulesToDataSets =
          this.redactionRulesDao.getDataSetsFromRedactionRules(requestContext);
      responseObserver.onNext(
          GetDataSetsResponse.newBuilder()
              .addAllDataSets(tenantDataSets)
              .addAllDataSets(filteredSystemDataSets)
              .addAllDataSets(redactionRulesToDataSets)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
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
        Optional<DataSet> existingDataSet =
            this.dataSetStore
                .getData(requestContext, dataSetId)
                .or(() -> this.getSystemDataSet(requestContext, dataSetId));
        DataSet dataSet = existingDataSet.orElseThrow(Status.NOT_FOUND::asRuntimeException);
        DataSet updatedDataSet = dataSet.toBuilder().setInfo(request.getInfo()).build();
        upsertedDataSet = this.dataSetStore.upsertObject(requestContext, updatedDataSet).getData();
      }
      responseObserver.onNext(
          UpdateDataSetResponse.newBuilder().setDataSet(upsertedDataSet).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
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
        Optional<ContextualConfigObject<DataSet>> optionalContextualConfigObject =
            this.dataSetStore.deleteObject(requestContext, dataSetId);
        Optional<DataSet> systemDataSetOptional = getSystemDataSet(requestContext, dataSetId);
        if (systemDataSetOptional.isPresent()) {
          DeletedSystemDataSet deletedSystemDataSet =
              DeletedSystemDataSet.newBuilder().setId(dataSetId).build();
          this.deletedDataSetStore.upsertObject(requestContext, deletedSystemDataSet);
          sendSystemDataSetDeletionEvent(
              requestContext,
              optionalContextualConfigObject.isEmpty(),
              systemDataSetOptional.get());
        } else if (optionalContextualConfigObject.isEmpty()) {
          throw Status.NOT_FOUND.asRuntimeException();
        }
      }
      responseObserver.onNext(DeleteDataSetResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
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
    if (systemDataSetsToIdMap.containsKey(id)) {
      Optional<DeletedSystemDataSet> isSystemDataSetDeleted =
          this.deletedDataSetStore.getData(requestContext, id);
      if (isSystemDataSetDeleted.isEmpty()) {
        return Optional.of(systemDataSetsToIdMap.get(id));
      }
    }
    return Optional.empty();
  }

  private List<String> getDeletedSystemDataSets(RequestContext requestContext) {
    return this.deletedDataSetStore.getAllObjects(requestContext).stream()
        .map(ConfigObject::getData)
        .map(DeletedSystemDataSet::getId)
        .collect(Collectors.toList());
  }

  private List<DataSet> buildSystemDataSetsList(
      List<? extends com.typesafe.config.ConfigObject> configObjectList) {
    return configObjectList.stream()
        .map(DataClassificationConfigServiceImpl::buildDataSetFromConfig)
        .collect(Collectors.toUnmodifiableList());
  }

  private List<DataType> buildSystemDataTypesList(
      List<? extends com.typesafe.config.ConfigObject> configObjectList) {
    return configObjectList.stream()
        .map(DataClassificationConfigServiceImpl::buildDataTypeFromConfig)
        .collect(Collectors.toUnmodifiableList());
  }

  @SneakyThrows
  private static DataType buildDataTypeFromConfig(com.typesafe.config.ConfigObject configObject) {
    String jsonString = configObject.render();
    DataType.Builder builder = DataType.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
  }

  @SneakyThrows
  private static DataSet buildDataSetFromConfig(com.typesafe.config.ConfigObject configObject) {
    String jsonString = configObject.render();
    DataSet.Builder builder = DataSet.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
  }
}
