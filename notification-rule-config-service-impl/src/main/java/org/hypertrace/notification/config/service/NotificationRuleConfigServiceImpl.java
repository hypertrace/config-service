package org.hypertrace.notification.config.service;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.notification.config.service.v1.CreateNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.CreateNotificationRuleResponse;
import org.hypertrace.notification.config.service.v1.DeleteNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.DeleteNotificationRuleResponse;
import org.hypertrace.notification.config.service.v1.GetAllNotificationRulesRequest;
import org.hypertrace.notification.config.service.v1.GetAllNotificationRulesResponse;
import org.hypertrace.notification.config.service.v1.GetNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.GetNotificationRuleResponse;
import org.hypertrace.notification.config.service.v1.NotificationRule;
import org.hypertrace.notification.config.service.v1.NotificationRuleConfigServiceGrpc;
import org.hypertrace.notification.config.service.v1.SinkType;
import org.hypertrace.notification.config.service.v1.UpdateNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.UpdateNotificationRuleResponse;

@Slf4j
public class NotificationRuleConfigServiceImpl
    extends NotificationRuleConfigServiceGrpc.NotificationRuleConfigServiceImplBase {

  private final NotificationRuleStore notificationRuleStore;
  private final NotificationRuleConfigServiceRequestValidator validator;

  public NotificationRuleConfigServiceImpl(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    this.notificationRuleStore = new NotificationRuleStore(channel, configChangeEventGenerator);
    this.validator = new NotificationRuleConfigServiceRequestValidator();
  }

  @Override
  public void createNotificationRule(
      CreateNotificationRuleRequest request,
      StreamObserver<CreateNotificationRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateCreateNotificationRuleRequest(requestContext, request);
      NotificationRule.Builder builder =
          NotificationRule.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setNotificationRuleMutableData(request.getNotificationRuleMutableData());

      responseObserver.onNext(
          CreateNotificationRuleResponse.newBuilder()
              .setNotificationRule(
                  notificationRuleStore.upsertObject(requestContext, builder.build()).getData())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Create Notification Rule RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateNotificationRule(
      UpdateNotificationRuleRequest request,
      StreamObserver<UpdateNotificationRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateUpdateNotificationRuleRequest(requestContext, request);
      responseObserver.onNext(
          UpdateNotificationRuleResponse.newBuilder()
              .setNotificationRule(
                  notificationRuleStore
                      .upsertObject(
                          requestContext,
                          NotificationRule.newBuilder()
                              .setId(request.getId())
                              .setNotificationRuleMutableData(
                                  request.getNotificationRuleMutableData())
                              .build())
                      .getData())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Update Notification Rule RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAllNotificationRules(
      GetAllNotificationRulesRequest request,
      StreamObserver<GetAllNotificationRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateGetAllNotificationRulesRequest(requestContext, request);
      responseObserver.onNext(
          GetAllNotificationRulesResponse.newBuilder()
              .addAllNotificationRules(
                  notificationRuleStore.getAllObjects(requestContext).stream()
                      .map(ConfigObject::getData)
                      .collect(Collectors.toUnmodifiableList()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get All Notification Rules RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteNotificationRule(
      DeleteNotificationRuleRequest request,
      StreamObserver<DeleteNotificationRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateDeleteNotificationRuleRequest(requestContext, request);
      notificationRuleStore
          .deleteObject(requestContext, request.getNotificationRuleId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      responseObserver.onNext(DeleteNotificationRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Delete Notification Rule RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getNotificationRule(
      GetNotificationRuleRequest request,
      StreamObserver<GetNotificationRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateGetNotificationRuleRequest(requestContext, request);
      NotificationRule notificationRule =
          notificationRuleStore
              .getData(requestContext, request.getNotificationRuleId())
              .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      responseObserver.onNext(
          GetNotificationRuleResponse.newBuilder()
              .setNotificationRule(makeBackwardCompatible(notificationRule))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get Notification Rule by id RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  private NotificationRule makeBackwardCompatible(NotificationRule rule) {
    if (rule.getNotificationRuleMutableData().getSinkType().equals(SinkType.SINK_TYPE_CHANNEL)) {
      return rule.toBuilder()
          .setNotificationRuleMutableData(
              rule.getNotificationRuleMutableData().toBuilder()
                  .setChannelId(rule.getNotificationRuleMutableData().getSinkId()))
          .build();
    }
    return rule;
  }
}
