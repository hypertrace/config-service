package org.hypertrace.alerting.config.service;

import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.alerting.config.service.v1.CreateNotificationChannelRequest;
import org.hypertrace.alerting.config.service.v1.CreateNotificationChannelResponse;
import org.hypertrace.alerting.config.service.v1.CreateNotificationRuleRequest;
import org.hypertrace.alerting.config.service.v1.CreateNotificationRuleResponse;
import org.hypertrace.alerting.config.service.v1.DeleteNotificationChannelRequest;
import org.hypertrace.alerting.config.service.v1.DeleteNotificationChannelResponse;
import org.hypertrace.alerting.config.service.v1.DeleteNotificationRuleRequest;
import org.hypertrace.alerting.config.service.v1.DeleteNotificationRuleResponse;
import org.hypertrace.alerting.config.service.v1.GetAllNotificationChannelsRequest;
import org.hypertrace.alerting.config.service.v1.GetAllNotificationChannelsResponse;
import org.hypertrace.alerting.config.service.v1.GetAllNotificationRulesRequest;
import org.hypertrace.alerting.config.service.v1.GetAllNotificationRulesResponse;
import org.hypertrace.alerting.config.service.v1.NotificationConfigServiceGrpc;
import org.hypertrace.alerting.config.service.v1.UpdateNotificationChannelRequest;
import org.hypertrace.alerting.config.service.v1.UpdateNotificationChannelResponse;
import org.hypertrace.alerting.config.service.v1.UpdateNotificationRuleRequest;
import org.hypertrace.alerting.config.service.v1.UpdateNotificationRuleResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class AlertingConfigServiceImpl
    extends NotificationConfigServiceGrpc.NotificationConfigServiceImplBase {
  private final AlertingConfigServiceCoordinator alertingConfigServiceCoordinator;
  private final AlertingConfigRequestValidator alertingConfigRequestValidator;

  public AlertingConfigServiceImpl(Channel channel) {
    this.alertingConfigServiceCoordinator = new AlertingConfigServiceCoordinatorImpl(channel);
    this.alertingConfigRequestValidator = new AlertingConfigRequestValidatorImpl();
  }

  @Override
  public void createNotificationRule(
      CreateNotificationRuleRequest request,
      StreamObserver<CreateNotificationRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      alertingConfigRequestValidator.validateCreateNotificationRuleRequest(
          requestContext, request, alertingConfigServiceCoordinator);
      responseObserver.onNext(
          CreateNotificationRuleResponse.newBuilder()
              .setNotificationRule(
                  alertingConfigServiceCoordinator.createNotificationRule(
                      requestContext, request.getNewNotificationRule()))
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
      alertingConfigRequestValidator.validateUpdateNotificationRuleRequest(
          requestContext, request, alertingConfigServiceCoordinator);
      responseObserver.onNext(
          UpdateNotificationRuleResponse.newBuilder()
              .setNotificationRule(
                  alertingConfigServiceCoordinator.updateNotificationRule(
                      requestContext, request.getNotificationRule()))
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
      alertingConfigRequestValidator.validateGetAllNotificationRulesRequest(
          requestContext, request);
      responseObserver.onNext(
          GetAllNotificationRulesResponse.newBuilder()
              .addAllNotificationRules(
                  alertingConfigServiceCoordinator.getAllNotificationRules(requestContext))
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
      alertingConfigRequestValidator.validateDeleteNotificationRuleRequest(requestContext, request);
      alertingConfigServiceCoordinator.deleteNotificationRule(
          requestContext, request.getNotificationRuleId());
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
      alertingConfigRequestValidator.validateCreateNotificationChannelRequest(
          requestContext, request);
      responseObserver.onNext(
          CreateNotificationChannelResponse.newBuilder()
              .setNotificationChannel(
                  alertingConfigServiceCoordinator.createNotificationChannel(
                      requestContext, request.getNewNotificationChannel()))
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
      alertingConfigRequestValidator.validateUpdateNotificationChannelRequest(
          requestContext, request);
      responseObserver.onNext(
          UpdateNotificationChannelResponse.newBuilder()
              .setNotificationChannel(
                  alertingConfigServiceCoordinator.updateNotificationChannel(
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
      alertingConfigRequestValidator.validateGetAllNotificationChannelsRequest(
          requestContext, request);
      responseObserver.onNext(
          GetAllNotificationChannelsResponse.newBuilder()
              .addAllNotificationChannels(
                  alertingConfigServiceCoordinator.getAllNotificationChannels(requestContext))
              .build());
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
      alertingConfigRequestValidator.validateDeleteNotificationChannelRequest(
          requestContext, request);
      alertingConfigServiceCoordinator.deleteNotificationChannel(
          requestContext, request.getNotificationChannelId());
      responseObserver.onNext(DeleteNotificationChannelResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Delete Notification Channel RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}