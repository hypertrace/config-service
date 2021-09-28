package ai.traceable.alerting.config.service;

import ai.traceable.alerting.config.service.v2.CreateEventConditionRequest;
import ai.traceable.alerting.config.service.v2.CreateEventConditionResponse;
import ai.traceable.alerting.config.service.v2.DeleteEventConditionRequest;
import ai.traceable.alerting.config.service.v2.DeleteEventConditionResponse;
import ai.traceable.alerting.config.service.v2.EventCondition;
import ai.traceable.alerting.config.service.v2.EventConditionConfigServiceGrpc;
import ai.traceable.alerting.config.service.v2.GetAllEventConditionsRequest;
import ai.traceable.alerting.config.service.v2.GetAllEventConditionsResponse;
import ai.traceable.alerting.config.service.v2.UpdateEventConditionRequest;
import ai.traceable.alerting.config.service.v2.UpdateEventConditionResponse;
import ai.traceable.config.utils.UuidGenerator;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class EventConditionConfigServiceImpl
    extends EventConditionConfigServiceGrpc.EventConditionConfigServiceImplBase {

  private final EventConditionStore eventConditionStore;
  private final EventConditionConfigServiceRequestValidator validator;
  private final UuidGenerator uuidGenerator;

  public EventConditionConfigServiceImpl(Channel configChannel) {
    this.eventConditionStore = new EventConditionStore(configChannel);
    this.validator = new EventConditionConfigServiceRequestValidator();
    this.uuidGenerator = new UuidGenerator();
  }

  @Override
  public void createEventCondition(
      CreateEventConditionRequest request,
      StreamObserver<CreateEventConditionResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateCreateEventConditionRequest(requestContext, request);
      responseObserver.onNext(
          CreateEventConditionResponse.newBuilder()
              .setEventCondition(
                  eventConditionStore.upsertObject(
                      requestContext,
                      EventCondition.newBuilder()
                          .setId(uuidGenerator.generateRandomId())
                          .setEventConditionMutableData(request.getEventConditionMutableData())
                          .build()))
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
      validator.validateUpdateEventConditionRequest(requestContext, request);
      responseObserver.onNext(
          UpdateEventConditionResponse.newBuilder()
              .setEventCondition(
                  eventConditionStore.upsertObject(
                      requestContext,
                      EventCondition.newBuilder()
                          .setId(request.getId())
                          .setEventConditionMutableData(request.getEventConditionMutableData())
                          .build()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Update EventConditions RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAllEventConditions(
      GetAllEventConditionsRequest request,
      StreamObserver<GetAllEventConditionsResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateGetAllEventConditionsRequest(requestContext, request);
      responseObserver.onNext(
          GetAllEventConditionsResponse.newBuilder()
              .addAllEventConditions(eventConditionStore.getAllObjects(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get All EventConditions RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteEventCondition(
      DeleteEventConditionRequest request,
      StreamObserver<DeleteEventConditionResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateDeleteEventConditionRequest(requestContext, request);
      eventConditionStore.deleteObject(requestContext, request.getEventConditionId());
      responseObserver.onNext(DeleteEventConditionResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Delete EventConditions RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
