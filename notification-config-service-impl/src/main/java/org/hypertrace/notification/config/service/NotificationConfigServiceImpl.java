package org.hypertrace.notification.config.service;

import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.notification.config.service.v1.CreateNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.CreateNotificationChannelResponse;
import org.hypertrace.notification.config.service.v1.CreateNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.CreateNotificationRuleResponse;
import org.hypertrace.notification.config.service.v1.DeleteNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.DeleteNotificationChannelResponse;
import org.hypertrace.notification.config.service.v1.DeleteNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.DeleteNotificationRuleResponse;
import org.hypertrace.notification.config.service.v1.GetAllNotificationChannelsRequest;
import org.hypertrace.notification.config.service.v1.GetAllNotificationChannelsResponse;
import org.hypertrace.notification.config.service.v1.GetAllNotificationRulesRequest;
import org.hypertrace.notification.config.service.v1.GetAllNotificationRulesResponse;
import org.hypertrace.notification.config.service.v1.NewNotificationChannel;
import org.hypertrace.notification.config.service.v1.NewNotificationRule;
import org.hypertrace.notification.config.service.v1.NotificationChannel;
import org.hypertrace.notification.config.service.v1.NotificationConfigServiceGrpc;
import org.hypertrace.notification.config.service.v1.NotificationRule;
import org.hypertrace.notification.config.service.v1.NotificationRuleMutableData;
import org.hypertrace.notification.config.service.v1.UpdateNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.UpdateNotificationChannelResponse;
import org.hypertrace.notification.config.service.v1.UpdateNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.UpdateNotificationRuleResponse;

@Slf4j
public class NotificationConfigServiceImpl
    extends NotificationConfigServiceGrpc.NotificationConfigServiceImplBase {
  private final NotificationChannelStore notificationChannelStore;
  private final NotificationRuleStore notificationRuleStore;
  private final NotificationConfigServiceRequestValidator notificationConfigServiceRequestValidator;

  public NotificationConfigServiceImpl(Channel channel) {
    this.notificationChannelStore = new NotificationChannelStore(channel);
    this.notificationRuleStore = new NotificationRuleStore(channel);
    this.notificationConfigServiceRequestValidator =
        new NotificationConfigServiceRequestValidator();
  }

  @Override
  public void createNotificationRule(
      CreateNotificationRuleRequest request,
      StreamObserver<CreateNotificationRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      notificationConfigServiceRequestValidator.validateCreateNotificationRuleRequest(
          requestContext, request);
      NewNotificationRule newNotificationRule = request.getNewNotificationRule();
      NotificationRule.Builder builder =
          NotificationRule.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setNotificationRuleData(
                  NotificationRuleMutableData.newBuilder()
                      .setRuleName(newNotificationRule.getNotificationRuleData().getRuleName())
                      .setEventConditionId(
                          newNotificationRule.getNotificationRuleData().getEventConditionId())
                      .setEventConditionType(
                          newNotificationRule.getNotificationRuleData().getEventConditionType())
                      .setDescription(
                          newNotificationRule.getNotificationRuleData().getDescription())
                      .setChannelId(newNotificationRule.getNotificationRuleData().getChannelId())
                      .setRateLimitIntervalDuration(
                          newNotificationRule
                              .getNotificationRuleData()
                              .getRateLimitIntervalDuration()));

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
      notificationConfigServiceRequestValidator.validateUpdateNotificationRuleRequest(
          requestContext, request);
      responseObserver.onNext(
          UpdateNotificationRuleResponse.newBuilder()
              .setNotificationRule(
                  notificationRuleStore.upsertObject(requestContext, request.getNotificationRule()))
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
      notificationConfigServiceRequestValidator.validateGetAllNotificationRulesRequest(
          requestContext, request);
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
      notificationConfigServiceRequestValidator.validateDeleteNotificationRuleRequest(
          requestContext, request);
      notificationRuleStore.deleteObject(requestContext, request.getNotificationRuleId());
      responseObserver.onNext(DeleteNotificationRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Delete Notification Rule RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createNotificationChannel(
      CreateNotificationChannelRequest request,
      StreamObserver<CreateNotificationChannelResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      notificationConfigServiceRequestValidator.validateCreateNotificationChannelRequest(
          requestContext, request);
      NewNotificationChannel newNotificationChannel = request.getNewNotificationChannel();
      NotificationChannel.Builder builder =
          NotificationChannel.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setChannelName(newNotificationChannel.getChannelName())
              .setNotificationChannelConfig(newNotificationChannel.getNotificationChannelConfig());
      responseObserver.onNext(
          CreateNotificationChannelResponse.newBuilder()
              .setNotificationChannel(
                  notificationChannelStore.upsertObject(requestContext, builder.build()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Create Notification Channel RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateNotificationChannel(
      UpdateNotificationChannelRequest request,
      StreamObserver<UpdateNotificationChannelResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      notificationConfigServiceRequestValidator.validateUpdateNotificationChannelRequest(
          requestContext, request);
      responseObserver.onNext(
          UpdateNotificationChannelResponse.newBuilder()
              .setNotificationChannel(
                  notificationChannelStore.upsertObject(
                      requestContext, request.getNotificationChannel()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Update Notification Channel RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAllNotificationChannels(
      GetAllNotificationChannelsRequest request,
      StreamObserver<GetAllNotificationChannelsResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      notificationConfigServiceRequestValidator.validateGetAllNotificationChannelsRequest(
          requestContext, request);
      List<NotificationChannel> notificationChannels =
          notificationChannelStore.getAllObjects(requestContext);
      GetAllNotificationChannelsResponse getAllNotificationChannelsResponse =
          GetAllNotificationChannelsResponse.newBuilder()
              .addAllNotificationChannels(notificationChannels)
              .build();
      responseObserver.onNext(getAllNotificationChannelsResponse);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get All Notification Channels RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteNotificationChannel(
      DeleteNotificationChannelRequest request,
      StreamObserver<DeleteNotificationChannelResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      notificationConfigServiceRequestValidator.validateDeleteNotificationChannelRequest(
          requestContext, request);
      notificationChannelStore.deleteObject(requestContext, request.getNotificationChannelId());
      responseObserver.onNext(DeleteNotificationChannelResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Delete Notification Channel RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
