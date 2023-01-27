package ai.traceable.data.classification.config.service;

import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_DATA_SET_IDS;
import static java.util.function.Function.identity;
import static org.hypertrace.config.proto.converter.ConfigProtoConverter.convertToValue;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
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
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideRule;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataType;
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
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
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
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.DeletedContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class DataClassificationConfigServiceImpl extends DataClassificationConfigServiceImplBase {
  private final IdentifiedObjectStore<DataSet> dataSetStore;
  private final IdentifiedObjectStore<DataType> dataTypeStore;
  private final IdentifiedObjectStore<DeletedSystemDataSet> deletedDataSetStore;
  private final IdentifiedObjectStore<DataClassificationOverride> dataClassificationOverrideStore;
  private final DataSetConfigRequestValidator dataSetConfigRequestValidator;
  private final DataTypeConfigRequestValidator dataTypeConfigRequestValidator;
  private final DataClassificationOverrideConfigRequestValidator
      dataClassificationOverrideConfigRequestValidator;
  private static final String DATA_CLASSIFICATION_CONFIG_SERVICE =
      "data.classification.config.service";
  private static final String SYSTEM_DATASETS_RP1 = "system.datasets.rp1";
  private static final String SYSTEM_DATATYPES_RP1 = "system.datatypes.rp1";
  private static final String SYSTEM_DATASETS_RP2 = "system.datasets.rp2";
  private static final String SYSTEM_DATATYPES_RP2 = "system.datatypes.rp2";
  private final List<DataSet> systemDataSetsRp1;
  private final List<DataType> systemDataTypesRp1;
  private final Map<String, DataType> systemDataTypesRp1ToIdMap;
  private final Map<String, DataType> systemDataTypesRp2ToIdMap;
  private final List<DataSet> systemDataSetsRp2;
  private final List<DataType> systemDataTypesRp2;
  private final Map<String, DataSet> systemDataSetsRp1ToIdMap;
  private final Map<String, DataSet> systemDataSetsRp2ToIdMap;
  private final Optional<ConfigChangeEventGenerator> configChangeEventGenerator;
  private final RedactionRulesDao redactionRulesDao;
  private final FeatureCachingClient featureCachingClient;

  @Inject
  public DataClassificationConfigServiceImpl(
      DataSetStore dataSetStore,
      DataTypeStore dataTypeStore,
      DeletedDataSetStore deletedDataSetStore,
      DataClassificationOverrideStore dataClassificationOverrideStore,
      DataSetConfigRequestValidator dataSetConfigRequestValidator,
      DataTypeConfigRequestValidator dataTypeConfigRequestValidator,
      DataClassificationOverrideConfigRequestValidator
          dataClassificationOverrideConfigRequestValidator,
      Config config,
      ConfigChangeEventGenerator configChangeEventGenerator,
      RedactionRulesDao redactionRulesDao,
      FeatureCachingClient featureCachingClient) {
    this.dataSetStore = dataSetStore;
    this.dataTypeStore = dataTypeStore;
    this.deletedDataSetStore = deletedDataSetStore;
    this.dataClassificationOverrideStore = dataClassificationOverrideStore;
    this.dataSetConfigRequestValidator = dataSetConfigRequestValidator;
    this.dataTypeConfigRequestValidator = dataTypeConfigRequestValidator;
    this.dataClassificationOverrideConfigRequestValidator =
        dataClassificationOverrideConfigRequestValidator;
    this.configChangeEventGenerator = Optional.ofNullable(configChangeEventGenerator);
    this.redactionRulesDao = redactionRulesDao;
    List<? extends com.typesafe.config.ConfigObject> systemDataSetsObjectList = null;
    List<? extends com.typesafe.config.ConfigObject> systemDataTypesObjectList = null;
    List<? extends com.typesafe.config.ConfigObject> systemDataSetsRp2ObjectList = null;
    List<? extends com.typesafe.config.ConfigObject> systemDataTypesRp2ObjectList = null;
    if (config.hasPath(DATA_CLASSIFICATION_CONFIG_SERVICE)) {
      Config dataClassificationConfig = config.getConfig(DATA_CLASSIFICATION_CONFIG_SERVICE);
      if (dataClassificationConfig.hasPath(SYSTEM_DATASETS_RP1)) {
        systemDataSetsObjectList = dataClassificationConfig.getObjectList(SYSTEM_DATASETS_RP1);
      }
      if (dataClassificationConfig.hasPath(SYSTEM_DATATYPES_RP1)) {
        systemDataTypesObjectList = dataClassificationConfig.getObjectList(SYSTEM_DATATYPES_RP1);
      }
      if (dataClassificationConfig.hasPath(SYSTEM_DATASETS_RP2)) {
        systemDataSetsRp2ObjectList = dataClassificationConfig.getObjectList(SYSTEM_DATASETS_RP2);
      }
      if (dataClassificationConfig.hasPath(SYSTEM_DATATYPES_RP2)) {
        systemDataTypesRp2ObjectList = dataClassificationConfig.getObjectList(SYSTEM_DATATYPES_RP2);
      }
    }
    if (systemDataSetsObjectList != null) {
      systemDataSetsRp1 = buildSystemDataSetsList(systemDataSetsObjectList);
      systemDataSetsRp1ToIdMap =
          systemDataSetsRp1.stream()
              .collect(Collectors.toUnmodifiableMap(DataSet::getId, identity()));
    } else {
      systemDataSetsRp1 = Collections.emptyList();
      systemDataSetsRp1ToIdMap = Collections.emptyMap();
    }

    if (systemDataTypesObjectList != null) {
      systemDataTypesRp1 = buildSystemDataTypesList(systemDataTypesObjectList);
      systemDataTypesRp1ToIdMap =
          systemDataTypesRp1.stream()
              .collect(Collectors.toUnmodifiableMap(DataType::getId, identity()));
    } else {
      systemDataTypesRp1 = Collections.emptyList();
      systemDataTypesRp1ToIdMap = Collections.emptyMap();
    }

    if (systemDataSetsRp2ObjectList != null) {
      systemDataSetsRp2 = buildSystemDataSetsList(systemDataSetsRp2ObjectList);
      systemDataSetsRp2ToIdMap =
          systemDataSetsRp2.stream()
              .collect(Collectors.toUnmodifiableMap(DataSet::getId, identity()));
    } else {
      systemDataSetsRp2 = Collections.emptyList();
      systemDataSetsRp2ToIdMap = Collections.emptyMap();
    }

    if (systemDataTypesRp2ObjectList != null) {
      systemDataTypesRp2 = buildSystemDataTypesList(systemDataTypesRp2ObjectList);
      systemDataTypesRp2ToIdMap =
          systemDataTypesRp2.stream()
              .collect(Collectors.toUnmodifiableMap(DataType::getId, identity()));
    } else {
      systemDataTypesRp2 = Collections.emptyList();
      systemDataTypesRp2ToIdMap = Collections.emptyMap();
    }

    this.featureCachingClient = featureCachingClient;
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
      List<DataType> tenantDataTypes =
          this.dataTypeStore.getAllObjects(requestContext).stream()
              .map(ConfigObject::getData)
              .collect(Collectors.toUnmodifiableList());
      Map<String, DataType> tenantDataTypesToIdMap =
          tenantDataTypes.stream()
              .collect(Collectors.toUnmodifiableMap(DataType::getId, identity()));
      List<DataType> filteredSystemDataTypes =
          getSystemDataTypes(requestContext).stream()
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
      DataType existingDataType =
          this.dataTypeStore
              .getData(requestContext, request.getId())
              .or(() -> getSystemDataType(requestContext, request.getId()))
              .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      DataType updatedDataType = existingDataType.toBuilder().setRule(request.getRule()).build();
      DataType upsertedDataType =
          this.dataTypeStore.upsertObject(requestContext, updatedDataType).getData();
      responseObserver.onNext(
          UpdateDataTypeResponse.newBuilder().setDataType(upsertedDataType).build());
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
      // Need to check deletion of system datatype. Currently we are not checking system datatypes
      // for deletion of datatype
      this.dataTypeStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      responseObserver.onNext(DeleteDataTypeResponse.getDefaultInstance());
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
      List<DataSet> tenantDataSets =
          this.dataSetStore.getAllObjects(requestContext).stream()
              .map(ConfigObject::getData)
              .collect(Collectors.toUnmodifiableList());
      Map<String, DataSet> systemDataSetsToIdMap = getSystemDataSetsToIdMap(requestContext);
      // filter out system data sets as we need to maintain order of system data sets
      List<DataSet> filteredTenantDataSets =
          tenantDataSets.stream()
              .filter(dataSet -> !systemDataSetsToIdMap.containsKey(dataSet.getId()))
              .collect(Collectors.toUnmodifiableList());
      Map<String, DataSet> tenantDataSetsToIdMap =
          tenantDataSets.stream().collect(Collectors.toUnmodifiableMap(DataSet::getId, identity()));
      // filter out deleted system data sets
      // also if overridden by tenant, then pick the overridden one
      List<DataSet> filteredSystemDataSets =
          getSystemDataSets(requestContext).stream()
              .filter(dataSet -> !deletedSystemDataSetsIds.contains(dataSet.getId()))
              .map(
                  systemDataSet ->
                      tenantDataSetsToIdMap.containsKey(systemDataSet.getId())
                          ? tenantDataSetsToIdMap.get(systemDataSet.getId())
                          : systemDataSet)
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
          this.deletedDataSetStore.upsertObject(requestContext, deletedSystemDataSet);
          sendSystemDataSetDeletionEvent(
              requestContext,
              optionalDeletedContextualConfigObject.isEmpty(),
              systemDataSetOptional.get());
        } else if (optionalDeletedContextualConfigObject.isEmpty()) {
          throw Status.NOT_FOUND.asRuntimeException();
        }
      }
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
      List<DataClassificationOverride> dataClassificationOverrides =
          getDataClassificationOverridesListByFilter(requestContext, request.getFilter());
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
      List<String> dataClassificationOverrideIds =
          getDataClassificationOverridesListByFilter(requestContext, request.getFilter()).stream()
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
    Map<String, DataSet> dataSetMap = getSystemDataSetsToIdMap(requestContext);
    if (dataSetMap.containsKey(id)) {
      Optional<DeletedSystemDataSet> isSystemDataSetDeleted =
          this.deletedDataSetStore.getData(requestContext, id);
      if (isSystemDataSetDeleted.isEmpty()) {
        return Optional.of(dataSetMap.get(id));
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

  private List<DataType> getSystemDataTypes(RequestContext requestContext) {
    if (featureCachingClient.isDataClassificationRp2Enabled(requestContext)) {
      return systemDataTypesRp2;
    }
    return systemDataTypesRp1;
  }

  private Optional<DataType> getSystemDataType(RequestContext requestContext, String dataTypeId) {
    Map<String, DataType> dataTypesMap;
    if (featureCachingClient.isDataClassificationRp2Enabled(requestContext)) {
      dataTypesMap = systemDataTypesRp2ToIdMap;
    } else {
      dataTypesMap = systemDataTypesRp1ToIdMap;
    }
    return Optional.ofNullable(dataTypesMap.get(dataTypeId));
  }

  private List<DataSet> getSystemDataSets(RequestContext requestContext) {
    if (featureCachingClient.isDataClassificationRp2Enabled(requestContext)) {
      return systemDataSetsRp2;
    }
    return systemDataSetsRp1;
  }

  private Map<String, DataSet> getSystemDataSetsToIdMap(RequestContext requestContext) {
    if (featureCachingClient.isDataClassificationRp2Enabled(requestContext)) {
      return systemDataSetsRp2ToIdMap;
    }
    return systemDataSetsRp1ToIdMap;
  }

  private List<DataClassificationOverride> getDataClassificationOverridesListByFilter(
      RequestContext requestContext, DataClassificationOverrideFilter filter) {
    List<DataClassificationOverride> dataClassificationOverrides = new ArrayList<>();
    List<DataClassificationOverride> tenantDataClassificationOverrides =
        this.dataClassificationOverrideStore.getAllObjects(requestContext).stream()
            .map(ConfigObject::getData)
            .collect(Collectors.toUnmodifiableList());
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
          List<DataClassificationOverrideRule.DataClassificationOverrideScope> scopesList =
              filter.getScopeFilter().getScopesList();
          Set<String> environmentFilterSet =
              scopesList.stream()
                  .filter(scope -> scope.hasEnvironmentScope())
                  .map(scope -> scope.getEnvironmentScope().getEnvironmentId())
                  .collect(Collectors.toUnmodifiableSet());
          return tenantDataClassificationOverrides.stream()
              .filter(
                  tenantDataClassificationOverride -> {
                    return environmentFilterSet.contains(
                        tenantDataClassificationOverride
                            .getDataClassificationOverrideRule()
                            .getScope()
                            .getEnvironmentScope()
                            .getEnvironmentId());
                  })
              .collect(Collectors.toUnmodifiableList());
        }
      default:
        return tenantDataClassificationOverrides;
    }
  }
}
