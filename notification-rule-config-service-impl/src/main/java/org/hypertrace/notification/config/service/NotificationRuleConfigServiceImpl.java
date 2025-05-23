package org.hypertrace.notification.config.service;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
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
import org.hypertrace.notification.config.service.v1.UpdateNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.UpdateNotificationRuleResponse;

@Slf4j
public class NotificationRuleConfigServiceImpl
    extends NotificationRuleConfigServiceGrpc.NotificationRuleConfigServiceImplBase {

  private final NotificationRuleFilteredStore notificationRuleStore;
  private final NotificationRuleConfigServiceRequestValidator validator;

  public NotificationRuleConfigServiceImpl(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    this.notificationRuleStore =
        new NotificationRuleFilteredStore(channel, configChangeEventGenerator);
    this.validator = new NotificationRuleConfigServiceRequestValidator();
  }

  @Override
  public void createNotificationRule(
      CreateNotificationRuleRequest request,
      StreamObserver<CreateNotificationRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      List<NotificationRule> existingNotificationRules =
          notificationRuleStore.getAllObjects(requestContext).stream()
              .map(ConfigObject::getData)
              .collect(Collectors.toList());
      validator.validateCreateNotificationRuleRequest(
          requestContext, request, existingNotificationRules);
      NotificationRule notificationRule =
          NotificationRule.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setNotificationRuleMutableData(request.getNotificationRuleMutableData())
              .build();

      responseObserver.onNext(
          CreateNotificationRuleResponse.newBuilder()
              .setNotificationRule(
                  notificationRuleStore.upsertObject(requestContext, notificationRule).getData())
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
      List<NotificationRule> existingNotificationRules =
          notificationRuleStore.getAllObjects(requestContext).stream()
              .map(ConfigObject::getData)
              .collect(Collectors.toList());
      validator.validateUpdateNotificationRuleRequest(
          requestContext, request, existingNotificationRules);
      NotificationRule notificationRule =
          NotificationRule.newBuilder()
              .setId(request.getId())
              .setNotificationRuleMutableData(request.getNotificationRuleMutableData())
              .build();
      responseObserver.onNext(
          UpdateNotificationRuleResponse.newBuilder()
              .setNotificationRule(
                  notificationRuleStore.upsertObject(requestContext, notificationRule).getData())
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
                  notificationRuleStore.getAllObjects(requestContext, request.getFilter()).stream()
                      .map(ConfigObject::getData)
                      .filter(
                          notificationRule ->
                              !notificationRule.getNotificationRuleMutableData().getIsDeleted())
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
              .filter(rule -> !rule.getNotificationRuleMutableData().getIsDeleted())
              .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      responseObserver.onNext(
          GetNotificationRuleResponse.newBuilder().setNotificationRule(notificationRule).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get Notification Rule by id RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
