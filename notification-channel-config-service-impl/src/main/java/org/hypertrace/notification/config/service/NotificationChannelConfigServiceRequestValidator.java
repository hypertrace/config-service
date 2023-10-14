package org.hypertrace.notification.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import com.typesafe.config.Config;
import io.grpc.Status;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
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
import org.hypertrace.notification.config.service.v1.SyslogIntegrationChannelConfig;
import org.hypertrace.notification.config.service.v1.UpdateNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.WebhookChannelConfig;
import org.hypertrace.notification.config.service.v1.WebhookHeader;

public class NotificationChannelConfigServiceRequestValidator {

  public static final String WEBHOOK_EXCLUSION_DOMAINS = "webhook.exclusion.domains";
  public static final String WEBHOOK_HTTP_SUPPORT_ENABLED = "webhook.http.support.enabled";

  public void validateCreateNotificationChannelRequest(
      RequestContext requestContext,
      CreateNotificationChannelRequest request,
      Config notificationChannelConfig) {
    validateRequestContextOrThrow(requestContext);
    validateNotificationChannelMutableData(request.getNotificationChannelMutableData());
    validateWebhookConfigExclusionDomains(
        request.getNotificationChannelMutableData(), notificationChannelConfig);
    validateWebhookHttpSupport(
        request.getNotificationChannelMutableData(), notificationChannelConfig);
  }

  void validateWebhookHttpSupport(
      NotificationChannelMutableData notificationChannelMutableData,
      Config notificationChannelConfig) {
    if (notificationChannelConfig == null
        || notificationChannelMutableData.getWebhookChannelConfigList().isEmpty()) {
      return;
    }
    for (WebhookChannelConfig webhookChannelConfig :
        notificationChannelMutableData.getWebhookChannelConfigList()) {
      if (notificationChannelConfig.hasPath(WEBHOOK_HTTP_SUPPORT_ENABLED)
          && notificationChannelConfig.getBoolean(WEBHOOK_HTTP_SUPPORT_ENABLED)) {
        continue;
      }
      validateHttpsUrl(webhookChannelConfig.getUrl());
    }
  }

  private void validateHttpsUrl(String urlString) {
    try {
      URL url = new URL(urlString);
      String protocol = url.getProtocol();
      if (!protocol.equals("https")) {
        throw Status.INVALID_ARGUMENT
            .withDescription("URL configured in webhook is not https ")
            .asRuntimeException();
      }
    } catch (MalformedURLException e) {
      throw Status.INVALID_ARGUMENT
          .withDescription("URL configured in webhook is malformed ")
          .asRuntimeException();
    }
  }

  void validateWebhookConfigExclusionDomains(
      NotificationChannelMutableData notificationChannelMutableData,
      Config notificationChannelConfig) {
    if (notificationChannelConfig == null
        || notificationChannelMutableData.getWebhookChannelConfigList().isEmpty()) {
      return;
    }
    if (notificationChannelConfig.hasPath(WEBHOOK_EXCLUSION_DOMAINS)) {
      List<String> exclusionDomains =
          notificationChannelConfig.getStringList(WEBHOOK_EXCLUSION_DOMAINS);
      for (WebhookChannelConfig webhookChannelConfig :
          notificationChannelMutableData.getWebhookChannelConfigList()) {
        for (String exclusionDomain : exclusionDomains) {
          if (webhookChannelConfig.getUrl().contains(exclusionDomain)) {
            throw Status.INVALID_ARGUMENT
                .withDescription("URL configured in webhook contains excluded domain")
                .asRuntimeException();
          }
        }
      }
    }
  }

  public void validateUpdateNotificationChannelRequest(
      RequestContext requestContext,
      UpdateNotificationChannelRequest request,
      Config notificationChannelConfig) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateNotificationChannelRequest.ID_FIELD_NUMBER);
    validateNotificationChannelMutableData(request.getNotificationChannelMutableData());
    validateWebhookConfigExclusionDomains(
        request.getNotificationChannelMutableData(), notificationChannelConfig);
  }

  private void validateNotificationChannelMutableData(NotificationChannelMutableData data) {
    validateNonDefaultPresenceOrThrow(
        data, NotificationChannelMutableData.CHANNEL_NAME_FIELD_NUMBER);
    if (data.getEmailChannelConfigCount() == 0
        && data.getWebhookChannelConfigCount() == 0
        && data.getS3BucketChannelConfigCount() == 0
        && data.getSplunkIntegrationChannelConfigCount() == 0
        && data.getSyslogIntegrationChannelConfigCount() == 0) {
      throw Status.INVALID_ARGUMENT.withDescription("No config present").asRuntimeException();
    }
    data.getEmailChannelConfigList().forEach(this::validateEmailChannelConfig);
    data.getWebhookChannelConfigList().forEach(this::validateWebhookChannelConfig);
    data.getS3BucketChannelConfigList().forEach(this::validateS3BucketConfig);
    data.getSplunkIntegrationChannelConfigList()
        .forEach(this::validateSplunkIntegrationChannelConfig);
    data.getSyslogIntegrationChannelConfigList()
        .forEach(this::validateSyslogIntegrationChannelConfig);
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

  private void validateSyslogIntegrationChannelConfig(
      SyslogIntegrationChannelConfig syslogIntegrationChannelConfig) {
    validateNonDefaultPresenceOrThrow(
        syslogIntegrationChannelConfig,
        SyslogIntegrationChannelConfig.SYSLOG_SERVER_INTEGRATION_ID_FIELD_NUMBER);
  }
}
