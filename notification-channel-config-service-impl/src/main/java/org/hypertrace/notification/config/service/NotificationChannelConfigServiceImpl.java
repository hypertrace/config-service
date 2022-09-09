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
import org.hypertrace.notification.config.service.v1.CreateNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.CreateNotificationChannelResponse;
import org.hypertrace.notification.config.service.v1.DeleteNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.DeleteNotificationChannelResponse;
import org.hypertrace.notification.config.service.v1.GetAllNotificationChannelsRequest;
import org.hypertrace.notification.config.service.v1.GetAllNotificationChannelsResponse;
import org.hypertrace.notification.config.service.v1.GetNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.GetNotificationChannelResponse;
import org.hypertrace.notification.config.service.v1.NotificationChannel;
import org.hypertrace.notification.config.service.v1.NotificationChannelConfigServiceGrpc;
import org.hypertrace.notification.config.service.v1.NotificationChannelMutableData;
import org.hypertrace.notification.config.service.v1.UpdateNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.UpdateNotificationChannelResponse;

@Slf4j
public class NotificationChannelConfigServiceImpl
    extends NotificationChannelConfigServiceGrpc.NotificationChannelConfigServiceImplBase {

  private final NotificationChannelStore notificationChannelStore;
  private final NotificationChannelConfigServiceRequestValidator validator;

  public NotificationChannelConfigServiceImpl(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    this.notificationChannelStore =
        new NotificationChannelStore(channel, configChangeEventGenerator);
    this.validator = new NotificationChannelConfigServiceRequestValidator();
  }

  @Override
  public void createNotificationChannel(
      CreateNotificationChannelRequest request,
      StreamObserver<CreateNotificationChannelResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateCreateNotificationChannelRequest(requestContext, request);
      NotificationChannel.Builder builder =
          NotificationChannel.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setNotificationChannelMutableData(
                  getIdPopulatedData(request.getNotificationChannelMutableData()));
      responseObserver.onNext(
          CreateNotificationChannelResponse.newBuilder()
              .setNotificationChannel(
                  notificationChannelStore.upsertObject(requestContext, builder.build()).getData())
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
      validator.validateUpdateNotificationChannelRequest(requestContext, request);
      responseObserver.onNext(
          UpdateNotificationChannelResponse.newBuilder()
              .setNotificationChannel(
                  notificationChannelStore
                      .upsertObject(
                          requestContext,
                          NotificationChannel.newBuilder()
                              .setId(request.getId())
                              .setNotificationChannelMutableData(
                                  getIdPopulatedData(request.getNotificationChannelMutableData()))
                              .build())
                      .getData())
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
      validator.validateGetAllNotificationChannelsRequest(requestContext, request);
      List<NotificationChannel> notificationChannels =
          notificationChannelStore.getAllObjects(requestContext).stream()
              .map(ConfigObject::getData)
              .collect(Collectors.toUnmodifiableList());
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
      validator.validateDeleteNotificationChannelRequest(requestContext, request);
      notificationChannelStore
          .deleteObject(requestContext, request.getNotificationChannelId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      responseObserver.onNext(DeleteNotificationChannelResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Delete Notification Channel RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getNotificationChannel(
      GetNotificationChannelRequest request,
      StreamObserver<GetNotificationChannelResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateGetNotificationChannelRequest(requestContext, request);
      NotificationChannel notificationChannel =
          notificationChannelStore
              .getData(requestContext, request.getNotificationChannelId())
              .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      responseObserver.onNext(
          GetNotificationChannelResponse.newBuilder()
              .setNotificationChannel(notificationChannel)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get Notification Channel by id RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  private NotificationChannelMutableData getIdPopulatedData(NotificationChannelMutableData data) {
    return NotificationChannelMutableData.newBuilder()
        .setChannelName(data.getChannelName())
        .addAllEmailChannelConfig(data.getEmailChannelConfigList())
        .addAllWebhookChannelConfig(
            data.getWebhookChannelConfigList().stream()
                .map(
                    config ->
                        config.getId().isBlank()
                            ? config.toBuilder().setId(UUID.randomUUID().toString()).build()
                            : config)
                .collect(Collectors.toList()))
        .build();
  }
}
