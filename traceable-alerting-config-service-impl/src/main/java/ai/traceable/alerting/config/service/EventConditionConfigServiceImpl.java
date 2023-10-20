package ai.traceable.alerting.config.service;

import ai.traceable.alerting.config.service.v2.AgentAlertCondition;
import ai.traceable.alerting.config.service.v2.CreateEventConditionRequest;
import ai.traceable.alerting.config.service.v2.CreateEventConditionResponse;
import ai.traceable.alerting.config.service.v2.DataCollectionChangeCondition;
import ai.traceable.alerting.config.service.v2.DeleteEventConditionRequest;
import ai.traceable.alerting.config.service.v2.DeleteEventConditionResponse;
import ai.traceable.alerting.config.service.v2.EventCondition;
import ai.traceable.alerting.config.service.v2.EventConditionConfigServiceGrpc;
import ai.traceable.alerting.config.service.v2.EventConditionMutableData;
import ai.traceable.alerting.config.service.v2.EventConditionMutableData.Builder;
import ai.traceable.alerting.config.service.v2.EventConditionScope;
import ai.traceable.alerting.config.service.v2.GetAllEventConditionsRequest;
import ai.traceable.alerting.config.service.v2.GetAllEventConditionsResponse;
import ai.traceable.alerting.config.service.v2.NewDeployment;
import ai.traceable.alerting.config.service.v2.UpdateEventConditionRequest;
import ai.traceable.alerting.config.service.v2.UpdateEventConditionResponse;
import ai.traceable.config.utils.UuidGenerator;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
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
                  eventConditionStore
                      .upsertObject(
                          requestContext,
                          EventCondition.newBuilder()
                              .setId(uuidGenerator.generateRandomId())
                              .setEventConditionMutableData(
                                  adaptForWrite(request.getEventConditionMutableData()))
                              .build())
                      .getData())
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
                  eventConditionStore
                      .upsertObject(
                          requestContext,
                          EventCondition.newBuilder()
                              .setId(request.getId())
                              .setEventConditionMutableData(
                                  adaptForWrite(request.getEventConditionMutableData()))
                              .build())
                      .getData())
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
              .addAllEventConditions(
                  eventConditionStore.getAllObjects(requestContext).stream()
                      .map(ContextualConfigObject::getData)
                      .map(this::adaptForRead)
                      .collect(Collectors.toUnmodifiableList()))
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
      eventConditionStore
          .deleteObject(requestContext, request.getEventConditionId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      responseObserver.onNext(DeleteEventConditionResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Delete EventConditions RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  /**
   * If env field is set just merge it with the envScope list
   *
   * @param input
   * @return adapted data
   */
  EventConditionMutableData adaptForWrite(EventConditionMutableData input) {
    if (!input.getEnvironment().isBlank()) {

      List<String> mergedList =
          Stream.concat(
                  input.getScope().getEnvironmentScope().getEnvironmentNamesList().stream(),
                  Stream.of(input.getEnvironment()))
              .distinct()
              .collect(Collectors.toUnmodifiableList());

      return input.toBuilder()
          .setScope(
              EventConditionScope.newBuilder()
                  .setEnvironmentScope(
                      EventConditionScope.EnvironmentScope.newBuilder()
                          .addAllEnvironmentNames(mergedList)))
          .build();
    }

    return input;
  }

  /**
   * Just merge the envs and return the existing env field as well the env scope list If env field
   * is empty and envScope list has exactly one env then add it to the env field as well and return
   * both
   *
   * @param input
   * @return adapted EventCondition
   */
  EventCondition adaptForRead(EventCondition input) {
    EventConditionMutableData.Builder builder = input.getEventConditionMutableData().toBuilder();
    Set<String> environments = new LinkedHashSet<>();
    if (input.getEventConditionMutableData().hasEnvironment()) {
      environments.add(input.getEventConditionMutableData().getEnvironment());
    } else if (input
            .getEventConditionMutableData()
            .getScope()
            .getEnvironmentScope()
            .getEnvironmentNamesCount()
        == 1) {
      builder.setEnvironment(
          input
              .getEventConditionMutableData()
              .getScope()
              .getEnvironmentScope()
              .getEnvironmentNames(0));
    }
    environments.addAll(
        input
            .getEventConditionMutableData()
            .getScope()
            .getEnvironmentScope()
            .getEnvironmentNamesList());

    builder.setScope(
        input.getEventConditionMutableData().getScope().toBuilder()
            .setEnvironmentScope(
                EventConditionScope.EnvironmentScope.newBuilder()
                    .addAllEnvironmentNames(environments)));
    if (builder.hasDataCollectionChangeCondition()) {
      return input.toBuilder()
          .setEventConditionMutableData(buildEventConditionForDataCollection(builder))
          .build();
    }
    return input.toBuilder().setEventConditionMutableData(builder).build();
  }

  private EventConditionMutableData buildEventConditionForDataCollection(Builder builder) {
    DataCollectionChangeCondition.Builder dataCollectionEventConditionBuilder =
        builder.getDataCollectionChangeCondition().toBuilder();
    // Setting to NewDeployment for backward compatibility
    if (!dataCollectionEventConditionBuilder.hasAgentAlertCondition()) {
      dataCollectionEventConditionBuilder.setAgentAlertCondition(
          buildDefaultDataCollectionChangeCondition());
    }
    return builder.setDataCollectionChangeCondition(dataCollectionEventConditionBuilder).build();
  }

  private AgentAlertCondition buildDefaultDataCollectionChangeCondition() {
    return AgentAlertCondition.newBuilder()
        .setNewDeployment(NewDeployment.getDefaultInstance())
        .build();
  }
}
