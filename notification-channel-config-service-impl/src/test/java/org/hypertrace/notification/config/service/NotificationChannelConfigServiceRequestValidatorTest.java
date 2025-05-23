package org.hypertrace.notification.config.service;

import static org.hypertrace.notification.config.service.NotificationChannelConfigServiceImpl.NOTIFICATION_CHANNEL_CONFIG_SERVICE_CONFIG;
import static org.hypertrace.notification.config.service.NotificationChannelConfigServiceRequestValidator.WEBHOOK_HTTP_SUPPORT_ENABLED;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.File;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.notification.config.service.v1.NotificationChannelMutableData;
import org.hypertrace.notification.config.service.v1.UpdateNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.WebhookChannelConfig;
import org.hypertrace.notification.config.service.v1.WebhookFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NotificationChannelConfigServiceRequestValidatorTest {
  @Test
  public void testValidateWebhookExclusions() {
    NotificationChannelConfigServiceRequestValidator
        notificationChannelConfigServiceRequestValidator =
            new NotificationChannelConfigServiceRequestValidator();
    File configFile = new File(ClassLoader.getSystemResource("application.conf").getPath());
    Config config = ConfigFactory.parseFile(configFile);
    NotificationChannelMutableData notificationChannelMutableDataWithExcludedDomain =
        getNotificationChannelMutableData("http://localhost:9000/test");
    Assertions.assertThrows(
        RuntimeException.class,
        () -> {
          notificationChannelConfigServiceRequestValidator.validateWebhookConfigExclusionDomains(
              notificationChannelMutableDataWithExcludedDomain,
              config.getConfig(NOTIFICATION_CHANNEL_CONFIG_SERVICE_CONFIG));
        },
        "RuntimeException was expected");
    NotificationChannelMutableData notificationChannelMutableDataWithValidDomain =
        getNotificationChannelMutableData("http://testHost:9000/test");
    notificationChannelConfigServiceRequestValidator.validateWebhookConfigExclusionDomains(
        notificationChannelMutableDataWithValidDomain,
        config.getConfig(NOTIFICATION_CHANNEL_CONFIG_SERVICE_CONFIG));
  }

  @Test
  public void testValidateWebhookHttpsSupport() {
    NotificationChannelConfigServiceRequestValidator
        notificationChannelConfigServiceRequestValidator =
            new NotificationChannelConfigServiceRequestValidator();
    File configFile = new File(ClassLoader.getSystemResource("application.conf").getPath());
    Config config = ConfigFactory.parseFile(configFile);
    Config notificationChannelConfig = config.getConfig(NOTIFICATION_CHANNEL_CONFIG_SERVICE_CONFIG);
    NotificationChannelMutableData notificationChannelWithHttpUrl =
        getNotificationChannelMutableData("http://localhost:9000/test");
    // As http support disabled RuntimeException should be thrown.
    Assertions.assertThrows(
        RuntimeException.class,
        () -> {
          notificationChannelConfigServiceRequestValidator.validateWebhookHttpSupport(
              notificationChannelWithHttpUrl, notificationChannelConfig);
        },
        "RuntimeException was expected");

    // In valid URl not accepted
    NotificationChannelMutableData notificationChannelWithInvalidUrl =
        getNotificationChannelMutableData("localhost");
    Assertions.assertThrows(
        RuntimeException.class,
        () -> {
          notificationChannelConfigServiceRequestValidator.validateWebhookHttpSupport(
              notificationChannelWithInvalidUrl, notificationChannelConfig);
        },
        "RuntimeException was expected");

    // Valid webhook config with https url.
    NotificationChannelMutableData notificationChannelMutableDataWithHttpsUrl =
        getNotificationChannelMutableData("https://localhost:9000/test");
    notificationChannelConfigServiceRequestValidator.validateWebhookHttpSupport(
        notificationChannelMutableDataWithHttpsUrl, notificationChannelConfig);

    // Http Webhook url while updating notification channel should throw exception
    Assertions.assertThrows(
        RuntimeException.class,
        () -> {
          notificationChannelConfigServiceRequestValidator.validateUpdateNotificationChannelRequest(
              RequestContext.forTenantId("tenant1"),
              getUpdateNotificationChannelRequestWithHttpUrl(),
              notificationChannelConfig,
              List.of());
        },
        "RuntimeException was expected");

    // Update config with http support enabled and verify no exceptions for http url
    Config updatedNotificationChannelConfig =
        config.withValue(WEBHOOK_HTTP_SUPPORT_ENABLED, ConfigValueFactory.fromAnyRef("true"));

    NotificationChannelMutableData notificationChannelMutableDataWithHttpUrl =
        getNotificationChannelMutableData("http://localhost:9000/test");
    notificationChannelConfigServiceRequestValidator.validateWebhookHttpSupport(
        notificationChannelMutableDataWithHttpUrl, updatedNotificationChannelConfig);

    // Update config with http support enabled and verify no exceptions for http url
    notificationChannelConfigServiceRequestValidator.validateUpdateNotificationChannelRequest(
        RequestContext.forTenantId("tenant1"),
        UpdateNotificationChannelRequest.newBuilder()
            .setId("id1")
            .setNotificationChannelMutableData(notificationChannelMutableDataWithHttpUrl)
            .build(),
        updatedNotificationChannelConfig,
        List.of());
  }

  private static UpdateNotificationChannelRequest getUpdateNotificationChannelRequestWithHttpUrl() {
    return UpdateNotificationChannelRequest.newBuilder()
        .setNotificationChannelMutableData(
            NotificationChannelMutableData.newBuilder()
                .setChannelName("channel1")
                .addWebhookChannelConfig(
                    WebhookChannelConfig.newBuilder()
                        .setUrl("http://localhost:9000/url")
                        .setFormat(WebhookFormat.WEBHOOK_FORMAT_JSON)
                        .build())
                .build())
        .build();
  }

  private static NotificationChannelMutableData getNotificationChannelMutableData(String url) {
    return NotificationChannelMutableData.newBuilder()
        .setChannelName("testChannel")
        .addWebhookChannelConfig(
            WebhookChannelConfig.newBuilder()
                .setUrl(url)
                .setFormat(WebhookFormat.WEBHOOK_FORMAT_JSON))
        .build();
  }
}
