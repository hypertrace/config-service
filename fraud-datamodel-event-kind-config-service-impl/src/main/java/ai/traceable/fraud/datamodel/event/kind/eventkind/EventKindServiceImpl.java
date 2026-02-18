package ai.traceable.fraud.datamodel.event.kind.eventkind;

import ai.traceable.fraud.datamodel.event.kind.v1.EventKindFilter;
import ai.traceable.fraud.datamodel.event.kind.v1.EventKindServiceGrpc.EventKindServiceImplBase;
import ai.traceable.fraud.datamodel.event.kind.v1.GetEventKindsRequest;
import ai.traceable.fraud.datamodel.event.kind.v1.GetEventKindsResponse;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

/** gRPC service implementation for EventKindService. */
@Slf4j
public class EventKindServiceImpl extends EventKindServiceImplBase {

  private final EventKindProvider eventKindProvider;

  @Inject
  public EventKindServiceImpl(EventKindProvider eventKindProvider) {
    this.eventKindProvider = eventKindProvider;
  }

  @Override
  public void getEventKinds(
      GetEventKindsRequest request, StreamObserver<GetEventKindsResponse> responseObserver) {
    try {
      EventKindFilter filter =
          request.hasFilter() ? request.getFilter() : EventKindFilter.getDefaultInstance();

      GetEventKindsResponse response =
          GetEventKindsResponse.newBuilder()
              .addAllEventKinds(eventKindProvider.getEventKinds(filter))
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Failed to get event kinds for request: {}", request, e);
      responseObserver.onError(e);
    }
  }
}
