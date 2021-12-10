package ai.traceable.data.classification.config.service;

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
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DataClassificationConfigServiceImpl extends DataClassificationConfigServiceImplBase {
  private final IdentifiedObjectStore<DataSet> dataSetStore;
  private final IdentifiedObjectStore<DataType> dataTypeStore;
  private final DataSetConfigRequestValidator dataSetConfigRequestValidator;
  private final DataTypeConfigRequestValidator dataTypeConfigRequestValidator;

  @Inject
  public DataClassificationConfigServiceImpl(
      DataSetStore dataSetStore,
      DataTypeStore dataTypeStore,
      DataSetConfigRequestValidator dataSetConfigRequestValidator,
      DataTypeConfigRequestValidator dataTypeConfigRequestValidator) {
    this.dataSetStore = dataSetStore;
    this.dataTypeStore = dataTypeStore;
    this.dataSetConfigRequestValidator = dataSetConfigRequestValidator;
    this.dataTypeConfigRequestValidator = dataTypeConfigRequestValidator;
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
      List<DataType> dataTypes =
          this.dataTypeStore.getAllObjects(requestContext).stream()
              .map(ConfigObject::getData)
              .collect(Collectors.toUnmodifiableList());
      responseObserver.onNext(GetDataTypesResponse.newBuilder().addAllDataTypes(dataTypes).build());
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
      DataSet existingDataSet =
          this.dataSetStore
              .getData(requestContext, request.getId())
              .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      responseObserver.onNext(GetDataSetResponse.newBuilder().setDataSet(existingDataSet).build());
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
      List<DataSet> dataSets =
          this.dataSetStore.getAllObjects(requestContext).stream()
              .map(ConfigObject::getData)
              .collect(Collectors.toUnmodifiableList());
      responseObserver.onNext(GetDataSetsResponse.newBuilder().addAllDataSets(dataSets).build());
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
}
