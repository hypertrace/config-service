package ai.traceable.config.service;

import static ai.traceable.platform.actor.v1.StatusChangeSource.STATUS_CHANGE_SOURCE_RATE_LIMIT;
import static ai.traceable.platform.actor.v1.StatusChangeSource.STATUS_CHANGE_SOURCE_SYSTEM;

import ai.traceable.platform.actor.v1.Actor;
import ai.traceable.platform.actor.v1.ActorServiceGrpc;
import ai.traceable.platform.actor.v1.GetActorsByStatusRequest;
import ai.traceable.platform.actor.v1.GetActorsByStatusResponse;
import ai.traceable.platform.actor.v1.RateLimitDetails;
import ai.traceable.platform.actor.v1.Status;
import ai.traceable.platform.actor.v1.StatusChangeDetails;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MockActorService extends ActorServiceGrpc.ActorServiceImplBase {
  private List<Actor> actorList;

  public MockActorService() {
    this.actorList = new ArrayList<>();
  }

  @Override
  public void getActorsByStatus(
      GetActorsByStatusRequest request,
      StreamObserver<GetActorsByStatusResponse> responseObserver) {
    responseObserver.onNext(GetActorsByStatusResponse.newBuilder().addAllActors(actorList).build());
    responseObserver.onCompleted();
  }

  public void addThreatActor(
      String actorId, String ip, Status status, Optional<String> environmentId) {
    actorList.add(
        Actor.newBuilder()
            .setActorId(actorId)
            .setEntityId("entity-" + actorId)
            .addIpAddresses(ip)
            .setStatusChangeSource(STATUS_CHANGE_SOURCE_SYSTEM)
            .setStatus(status)
            .setStatusExpiryTimestamp(System.currentTimeMillis() + 10000L)
            .setEnvironment(environmentId.orElse(""))
            .build());
  }

  public void addRateLimitingActor(String id, Optional<String> environmentId) {
    actorList.add(
        Actor.newBuilder()
            .setActorId(id)
            .setEntityId("entity-" + id)
            .addIpAddresses("3.3.3.3")
            .setStatusChangeSource(STATUS_CHANGE_SOURCE_RATE_LIMIT)
            .setStatusChangeDetails(
                StatusChangeDetails.newBuilder()
                    .setRateLimitDetails(RateLimitDetails.newBuilder().setRuleId(id)))
            .setStatusExpiryTimestamp(System.currentTimeMillis() + 10000L)
            .setEnvironment(environmentId.orElse(""))
            .build());
  }
}
