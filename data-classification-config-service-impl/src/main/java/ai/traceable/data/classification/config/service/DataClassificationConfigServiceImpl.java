package ai.traceable.data.classification.config.service;

import static java.util.function.Function.identity;

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
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DataClassificationConfigServiceImpl extends DataClassificationConfigServiceImplBase {
  private final IdentifiedObjectStore<DataSet> dataSetStore;
  private final IdentifiedObjectStore<DataType> dataTypeStore;
  private final DataSetConfigRequestValidator dataSetConfigRequestValidator;
  private final DataTypeConfigRequestValidator dataTypeConfigRequestValidator;
  private static final String DATA_CLASSIFICATION_CONFIG_SERVICE =
      "data.classification.config.service";
  private static final String SYSTEM_DATASETS = "system.datasets";
  private static final String SYSTEM_DATATYPES = "system.datatypes";
  private final List<DataSet> systemDataSets;
  private final List<DataType> systemDataTypes;
  private final Map<String, DataSet> systemDataSetsToIdMap;

  @Inject
  public DataClassificationConfigServiceImpl(
      DataSetStore dataSetStore,
      DataTypeStore dataTypeStore,
      DataSetConfigRequestValidator dataSetConfigRequestValidator,
      DataTypeConfigRequestValidator dataTypeConfigRequestValidator,
      Config config) {
    this.dataSetStore = dataSetStore;
    this.dataTypeStore = dataTypeStore;
    this.dataSetConfigRequestValidator = dataSetConfigRequestValidator;
    this.dataTypeConfigRequestValidator = dataTypeConfigRequestValidator;
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
      responseObserver.onNext(
          GetDataTypesResponse.newBuilder()
              .addAllDataTypes(tenantDataTypes)
              .addAllDataTypes(filteredSystemDataTypes)
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
          this.dataSetStore.getData(requestContext, request.getId());
      if (dataSetOptional.isEmpty()) {
        dataSetOptional = Optional.ofNullable(systemDataSetsToIdMap.get(request.getId()));
      }
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
      List<DataSet> tenantDataSets =
          this.dataSetStore.getAllObjects(requestContext).stream()
              .map(ConfigObject::getData)
              .collect(Collectors.toUnmodifiableList());
      Map<String, DataSet> tenantDataSetsToIdMap =
          tenantDataSets.stream().collect(Collectors.toUnmodifiableMap(DataSet::getId, identity()));
      List<DataSet> filteredSystemDataSets =
          systemDataSets.stream()
              .filter(dataSet -> !tenantDataSetsToIdMap.containsKey(dataSet.getId()))
              .collect(Collectors.toUnmodifiableList());
      responseObserver.onNext(
          GetDataSetsResponse.newBuilder()
              .addAllDataSets(tenantDataSets)
              .addAllDataSets(filteredSystemDataSets)
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
      DataSet existingDataSet =
          this.dataSetStore
              .getData(requestContext, request.getId())
              .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      DataSet updatedDataSet = existingDataSet.toBuilder().setInfo(request.getInfo()).build();
      DataSet upsertedDataSet =
          this.dataSetStore.upsertObject(requestContext, updatedDataSet).getData();
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
      this.dataSetStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      responseObserver.onNext(DeleteDataSetResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
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
