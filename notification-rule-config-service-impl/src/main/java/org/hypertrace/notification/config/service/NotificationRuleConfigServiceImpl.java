package org.hypertrace.notification.config.service;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.notification.config.service.v1.CreateNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.CreateNotificationRuleResponse;
import org.hypertrace.notification.config.service.v1.DeleteNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.DeleteNotificationRuleResponse;
import org.hypertrace.notification.config.service.v1.GetAllNotificationRulesRequest;
import org.hypertrace.notification.config.service.v1.GetAllNotificationRulesResponse;
import org.hypertrace.notification.config.service.v1.GetNotificationRuleByIdRequest;
import org.hypertrace.notification.config.service.v1.GetNotificationRuleByIdResponse;
import org.hypertrace.notification.config.service.v1.NotificationRule;
import org.hypertrace.notification.config.service.v1.NotificationRuleConfigServiceGrpc;
import org.hypertrace.notification.config.service.v1.UpdateNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.UpdateNotificationRuleResponse;

@Slf4j
public class NotificationRuleConfigServiceImpl
    extends NotificationRuleConfigServiceGrpc.NotificationRuleConfigServiceImplBase {

  private final NotificationRuleStore notificationRuleStore;
  private final NotificationRuleConfigServiceRequestValidator validator;

  public NotificationRuleConfigServiceImpl(Channel channel) {
    this.notificationRuleStore = new NotificationRuleStore(channel);
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
                  notificationRuleStore.upsertObject(requestContext, builder.build()))
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
                  notificationRuleStore.upsertObject(
                      requestContext,
                      NotificationRule.newBuilder()
                          .setId(request.getId())
                          .setNotificationRuleMutableData(request.getNotificationRuleMutableData())
                          .build()))
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
              .addAllNotificationRules(notificationRuleStore.getAllObjects(requestContext))
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
      notificationRuleStore.deleteObject(requestContext, request.getNotificationRuleId());
      responseObserver.onNext(DeleteNotificationRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Delete Notification Rule RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getNotificationRule(GetNotificationRuleByIdRequest request,
      StreamObserver<GetNotificationRuleByIdResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateGetNotificationRuleByIdRequest(requestContext, request);
      NotificationRule notificationRule = notificationRuleStore
          .getObject(requestContext, request.getNotificationRuleId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      responseObserver.onNext(
          GetNotificationRuleByIdResponse.newBuilder()
              .setNotificationRule(notificationRule)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get Notification Rule by id RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
