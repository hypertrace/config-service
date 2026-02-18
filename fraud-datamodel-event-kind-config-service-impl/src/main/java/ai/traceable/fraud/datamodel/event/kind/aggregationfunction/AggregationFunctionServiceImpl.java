package ai.traceable.fraud.datamodel.event.kind.aggregationfunction;

import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunctionFilter;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunctionServiceGrpc.AggregationFunctionServiceImplBase;
import ai.traceable.fraud.datamodel.event.kind.v1.GetAggregationFunctionsRequest;
import ai.traceable.fraud.datamodel.event.kind.v1.GetAggregationFunctionsResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

/** gRPC service implementation for AggregationFunctionService. */
@Slf4j
public class AggregationFunctionServiceImpl extends AggregationFunctionServiceImplBase {

  private final AggregationFunctionProvider provider;

  @Inject
  public AggregationFunctionServiceImpl(AggregationFunctionProvider provider) {
    this.provider = provider;
  }

  @Override
  public void getAggregationFunctions(
      GetAggregationFunctionsRequest request,
      StreamObserver<GetAggregationFunctionsResponse> responseObserver) {
    try {
      GetAggregationFunctionsResponse.Builder responseBuilder =
          GetAggregationFunctionsResponse.newBuilder();

      AggregationFunctionFilter filter =
          request.hasFilter()
              ? request.getFilter()
              : AggregationFunctionFilter.getDefaultInstance();

      // Validation: at least one kind must be provided
      if (filter.getCompatibleWithKindsCount() == 0) {
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription("At least 1 compatible_with_kinds must be provided")
                .asRuntimeException());
        return;
      }

      responseBuilder.addAllFunctionMappings(
          provider.getFunctionsByKinds(filter.getCompatibleWithKindsList()));

      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Failed to get aggregation functions for request: {}", request, e);
      responseObserver.onError(e);
    }
  }
}
