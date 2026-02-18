package ai.traceable.fraud.datamodel.event.kind.transformationfunction;

import ai.traceable.fraud.datamodel.event.kind.v1.GetOperatorsRequest;
import ai.traceable.fraud.datamodel.event.kind.v1.GetOperatorsResponse;
import ai.traceable.fraud.datamodel.event.kind.v1.GetTransformationFunctionsRequest;
import ai.traceable.fraud.datamodel.event.kind.v1.GetTransformationFunctionsResponse;
import ai.traceable.fraud.datamodel.event.kind.v1.OperatorFilter;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunctionFilter;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunctionServiceGrpc.TransformationFunctionServiceImplBase;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

/** gRPC service implementation for TransformationFunctionService. */
@Slf4j
public class TransformationFunctionServiceImpl extends TransformationFunctionServiceImplBase {

  private final TransformationFunctionProvider provider;

  @Inject
  public TransformationFunctionServiceImpl(TransformationFunctionProvider provider) {
    this.provider = provider;
  }

  @Override
  public void getTransformationFunctions(
      GetTransformationFunctionsRequest request,
      StreamObserver<GetTransformationFunctionsResponse> responseObserver) {
    try {
      TransformationFunctionFilter filter =
          request.hasFilter()
              ? request.getFilter()
              : TransformationFunctionFilter.getDefaultInstance();

      if (filter.getCompatibleWithKindsCount() == 0) {
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription("At least 1 compatible_with_kinds must be provided")
                .asRuntimeException());
        return;
      }

      GetTransformationFunctionsResponse response =
          GetTransformationFunctionsResponse.newBuilder()
              .addAllFunctionMappings(
                  provider.getFunctionsByKinds(filter.getCompatibleWithKindsList()))
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Failed to get transformation functions for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getOperators(
      GetOperatorsRequest request, StreamObserver<GetOperatorsResponse> responseObserver) {
    try {
      OperatorFilter filter =
          request.hasFilter() ? request.getFilter() : OperatorFilter.getDefaultInstance();

      if (filter.getCompatibleWithKindsCount() == 0) {
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription("At least 1 compatible_with_kinds must be provided")
                .asRuntimeException());
        return;
      }

      GetOperatorsResponse response =
          GetOperatorsResponse.newBuilder()
              .addAllOperatorMappings(
                  provider.getOperatorsByKinds(filter.getCompatibleWithKindsList()))
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Failed to get operators for request: {}", request, e);
      responseObserver.onError(e);
    }
  }
}
