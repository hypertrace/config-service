package org.hypertrace.notification.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import com.google.common.base.Preconditions;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.notification.config.service.v1.CreateNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.DeleteNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.GetAllNotificationChannelsRequest;
import org.hypertrace.notification.config.service.v1.NotificationChannel;
import org.hypertrace.notification.config.service.v1.NotificationChannelMutableData;
import org.hypertrace.notification.config.service.v1.UpdateNotificationChannelRequest;

public class NotificationChannelConfigServiceRequestValidator {

  public void validateCreateNotificationChannelRequest(
      RequestContext requestContext, CreateNotificationChannelRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNotificationChannelMutableData(request.getNotificationChannelMutableData());
  }

  public void validateUpdateNotificationChannelRequest(
      RequestContext requestContext, UpdateNotificationChannelRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request.getNotificationChannel(), NotificationChannel.ID_FIELD_NUMBER);
    validateNotificationChannelMutableData(
        request.getNotificationChannel().getNotificationChannelMutableData());
  }

  private void validateNotificationChannelMutableData(NotificationChannelMutableData data) {
    validateNonDefaultPresenceOrThrow(
        data, NotificationChannelMutableData.CHANNEL_NAME_FIELD_NUMBER);
    if (data.getEmailChannelConfigCount() != 0 || data.getWebhookChannelConfigCount() != 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Either email or webhook config should be present")
          .asRuntimeException();
    }
  }

  public void validateGetAllNotificationChannelsRequest(
      RequestContext requestContext, GetAllNotificationChannelsRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateDeleteNotificationChannelRequest(
      RequestContext requestContext, DeleteNotificationChannelRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, DeleteNotificationChannelRequest.NOTIFICATION_CHANNEL_ID_FIELD_NUMBER);
  }
}
