package ai.traceable.fraud.datamodel.event.kind.operator;

import ai.traceable.fraud.datamodel.event.kind.v1.GetOperatorsRequest;
import ai.traceable.fraud.datamodel.event.kind.v1.GetOperatorsResponse;
import ai.traceable.fraud.datamodel.event.kind.v1.OperatorFilter;
import ai.traceable.fraud.datamodel.event.kind.v1.OperatorServiceGrpc.OperatorServiceImplBase;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

/** gRPC service implementation for OperatorService. */
@Slf4j
public class OperatorServiceImpl extends OperatorServiceImplBase {

  private final OperatorProvider provider;

  @Inject
  public OperatorServiceImpl(OperatorProvider provider) {
    this.provider = provider;
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
