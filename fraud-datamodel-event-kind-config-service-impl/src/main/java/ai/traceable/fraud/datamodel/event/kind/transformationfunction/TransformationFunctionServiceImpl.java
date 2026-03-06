package ai.traceable.fraud.datamodel.event.kind.transformationfunction;

import ai.traceable.fraud.datamodel.event.kind.v1.GetTransformationFunctionsRequest;
import ai.traceable.fraud.datamodel.event.kind.v1.GetTransformationFunctionsResponse;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunctionFilter;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunctionServiceGrpc.TransformationFunctionServiceImplBase;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunctionsByKind;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import java.util.List;
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

      List<TransformationFunctionsByKind> functionMappings;
      if (filter.getCompatibleWithKindsCount() == 0) {
        functionMappings = provider.getAllFunctions();
      } else {
        functionMappings = provider.getFunctionsByKinds(filter.getCompatibleWithKindsList());
      }

      responseObserver.onNext(
          GetTransformationFunctionsResponse.newBuilder()
              .addAllFunctionMappings(functionMappings)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Failed to get transformation functions for request: {}", request, e);
      responseObserver.onError(e);
    }
  }
}
