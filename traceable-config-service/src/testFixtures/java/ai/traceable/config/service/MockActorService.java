package ai.traceable.config.service;

import static ai.traceable.platform.actor.v1.StatusChangeSource.STATUS_CHANGE_SOURCE_SYSTEM;

import ai.traceable.platform.actor.v1.Actor;
import ai.traceable.platform.actor.v1.ActorServiceGrpc;
import ai.traceable.platform.actor.v1.GetActorsByStatusRequest;
import ai.traceable.platform.actor.v1.GetActorsByStatusResponse;
import io.grpc.stub.StreamObserver;

public class MockActorService extends ActorServiceGrpc.ActorServiceImplBase {
  @Override
  public void getActorsByStatus(
      GetActorsByStatusRequest request,
      StreamObserver<GetActorsByStatusResponse> responseObserver) {
    responseObserver.onNext(
        GetActorsByStatusResponse.newBuilder()
            .addActors(buildActor("Actor-1"))
            .addActors(buildActor("Actor-2"))
            .build());
    responseObserver.onCompleted();
  }

  private Actor buildActor(String actorId) {
    return Actor.newBuilder()
        .setActorId(actorId)
        .setEntityId("entity-" + actorId)
        .setStatusChangeSource(STATUS_CHANGE_SOURCE_SYSTEM)
        .setStatusExpiryTimestamp(System.currentTimeMillis() + 10000L)
        .build();
  }
}
