package org.hypertrace.alerting.config.service;

import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.alerting.config.service.v1.CreateEventConditionRequest;
import org.hypertrace.alerting.config.service.v1.CreateEventConditionResponse;
import org.hypertrace.alerting.config.service.v1.DeleteEventConditionRequest;
import org.hypertrace.alerting.config.service.v1.DeleteEventConditionResponse;
import org.hypertrace.alerting.config.service.v1.EventConditionsConfigServiceGrpc;
import org.hypertrace.alerting.config.service.v1.GetAllEventConditionsRequest;
import org.hypertrace.alerting.config.service.v1.GetAllEventConditionsResponse;
import org.hypertrace.alerting.config.service.v1.UpdateEventConditionRequest;
import org.hypertrace.alerting.config.service.v1.UpdateEventConditionResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class EventConditionConfigServiceImpl
    extends EventConditionsConfigServiceGrpc.EventConditionsConfigServiceImplBase {

  private final EventConditionStore eventConditionStore;

  public EventConditionConfigServiceImpl(Channel configChannel) {
    this.eventConditionStore = new EventConditionStore(configChannel);
  }

  @Override
  public void createEventCondition(
      CreateEventConditionRequest request,
      StreamObserver<CreateEventConditionResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      responseObserver.onNext(
          CreateEventConditionResponse.newBuilder()
              .setEventConditions(
                  eventConditionStore.createEventCondition(
                      requestContext, request.getNewEventConditions()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Create EventCondition RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateEventCondition(
      UpdateEventConditionRequest request,
      StreamObserver<UpdateEventConditionResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      responseObserver.onNext(
          UpdateEventConditionResponse.newBuilder()
              .setEventCondition(
                  eventConditionStore.updateEventCondition(
                      requestContext, request.getEventCondition()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Update Notification Rule RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAllEventConditions(
      GetAllEventConditionsRequest request,
      StreamObserver<GetAllEventConditionsResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      responseObserver.onNext(
          GetAllEventConditionsResponse.newBuilder()
              .addAllEventCondition(eventConditionStore.getAllEventConditions(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get All Notification Rules RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteEventCondition(
      DeleteEventConditionRequest request,
      StreamObserver<DeleteEventConditionResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      eventConditionStore.deleteEventCondition(requestContext, request.getEventConditionId());
      responseObserver.onNext(DeleteEventConditionResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Delete Notification Rule RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
