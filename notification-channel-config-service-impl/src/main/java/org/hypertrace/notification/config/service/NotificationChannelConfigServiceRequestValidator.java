package org.hypertrace.notification.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.notification.config.service.v1.AwsS3BucketChannelConfig;
import org.hypertrace.notification.config.service.v1.AwsS3BucketChannelConfig.WebIdentityAuthenticationCredential;
import org.hypertrace.notification.config.service.v1.CreateNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.DeleteNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.EmailChannelConfig;
import org.hypertrace.notification.config.service.v1.GetAllNotificationChannelsRequest;
import org.hypertrace.notification.config.service.v1.GetNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.NotificationChannelMutableData;
import org.hypertrace.notification.config.service.v1.SplunkIntegrationChannelConfig;
import org.hypertrace.notification.config.service.v1.UpdateNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.WebhookChannelConfig;
import org.hypertrace.notification.config.service.v1.WebhookHeader;

public class NotificationChannelConfigServiceRequestValidator {

  public void validateCreateNotificationChannelRequest(
      RequestContext requestContext, CreateNotificationChannelRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNotificationChannelMutableData(request.getNotificationChannelMutableData());
  }

  public void validateUpdateNotificationChannelRequest(
      RequestContext requestContext, UpdateNotificationChannelRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateNotificationChannelRequest.ID_FIELD_NUMBER);
    validateNotificationChannelMutableData(request.getNotificationChannelMutableData());
  }

  private void validateNotificationChannelMutableData(NotificationChannelMutableData data) {
    validateNonDefaultPresenceOrThrow(
        data, NotificationChannelMutableData.CHANNEL_NAME_FIELD_NUMBER);
    if (data.getEmailChannelConfigCount() == 0
        && data.getWebhookChannelConfigCount() == 0
        && data.getS3BucketChannelConfigCount() == 0
        && data.getSplunkIntegrationChannelConfigCount() == 0) {
      throw Status.INVALID_ARGUMENT.withDescription("No config present").asRuntimeException();
    }
    data.getEmailChannelConfigList().forEach(this::validateEmailChannelConfig);
    data.getWebhookChannelConfigList().forEach(this::validateWebhookChannelConfig);
    data.getS3BucketChannelConfigList().forEach(this::validateS3BucketConfig);
    data.getSplunkIntegrationChannelConfigList()
        .forEach(this::validateSplunkIntegrationChannelConfig);
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

  public void validateGetNotificationChannelRequest(
      RequestContext requestContext, GetNotificationChannelRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, GetNotificationChannelRequest.NOTIFICATION_CHANNEL_ID_FIELD_NUMBER);
  }

  private void validateEmailChannelConfig(EmailChannelConfig emailChannelConfig) {
    validateNonDefaultPresenceOrThrow(emailChannelConfig, EmailChannelConfig.ADDRESS_FIELD_NUMBER);
  }

  private void validateWebhookChannelConfig(WebhookChannelConfig webhookChannelConfig) {
    validateNonDefaultPresenceOrThrow(webhookChannelConfig, WebhookChannelConfig.URL_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        webhookChannelConfig, WebhookChannelConfig.FORMAT_FIELD_NUMBER);
    webhookChannelConfig.getHeadersList().forEach(this::validateWebhookHeader);
  }

  private void validateS3BucketConfig(AwsS3BucketChannelConfig awsS3BucketChannelConfig) {
    validateNonDefaultPresenceOrThrow(
        awsS3BucketChannelConfig, AwsS3BucketChannelConfig.BUCKET_NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        awsS3BucketChannelConfig, AwsS3BucketChannelConfig.REGION_FIELD_NUMBER);
    switch (awsS3BucketChannelConfig.getAuthenticationCredentialCase()) {
      case WEB_IDENTITY_AUTH_CREDENTIAL:
        validateNonDefaultPresenceOrThrow(
            awsS3BucketChannelConfig.getWebIdentityAuthCredential(),
            WebIdentityAuthenticationCredential.ROLE_ARN_FIELD_NUMBER);
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("No credentials present in AWS S3 bucket config")
            .asRuntimeException();
    }
  }

  private void validateWebhookHeader(WebhookHeader webhookHeader) {
    validateNonDefaultPresenceOrThrow(webhookHeader, WebhookHeader.NAME_FIELD_NUMBER);
  }

  private void validateSplunkIntegrationChannelConfig(
      SplunkIntegrationChannelConfig splunkIntegrationChannelConfig) {
    validateNonDefaultPresenceOrThrow(
        splunkIntegrationChannelConfig,
        SplunkIntegrationChannelConfig.SPLUNK_INTEGRATION_ID_FIELD_NUMBER);
  }
}
