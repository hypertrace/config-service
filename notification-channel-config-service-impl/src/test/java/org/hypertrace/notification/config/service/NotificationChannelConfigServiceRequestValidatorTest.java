package org.hypertrace.notification.config.service;

import static org.hypertrace.notification.config.service.NotificationChannelConfigServiceImpl.NOTIFICATION_CHANNEL_CONFIG_SERVICE_CONFIG;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import org.hypertrace.notification.config.service.v1.NotificationChannelMutableData;
import org.hypertrace.notification.config.service.v1.WebhookChannelConfig;
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
        NotificationChannelMutableData.newBuilder()
            .setChannelName("testChannel")
            .addWebhookChannelConfig(
                WebhookChannelConfig.newBuilder().setUrl("http://localhost:9000/test"))
            .build();
    Assertions.assertThrows(
        RuntimeException.class,
        () -> {
          notificationChannelConfigServiceRequestValidator.validateWebhookConfigExclusionDomains(
              notificationChannelMutableDataWithExcludedDomain,
              config.getConfig(NOTIFICATION_CHANNEL_CONFIG_SERVICE_CONFIG));
        },
        "RuntimeException was expected");
    NotificationChannelMutableData notificationChannelMutableDataWithValidDomain =
        NotificationChannelMutableData.newBuilder()
            .setChannelName("testChannel")
            .addWebhookChannelConfig(
                WebhookChannelConfig.newBuilder().setUrl("http://testHost:9000/test"))
            .build();
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
    NotificationChannelMutableData notificationChannelWithHttpUrl =
        NotificationChannelMutableData.newBuilder()
            .setChannelName("testChannel")
            .addWebhookChannelConfig(
                WebhookChannelConfig.newBuilder().setUrl("http://localhost:9000/test"))
            .build();
    Assertions.assertThrows(
        RuntimeException.class,
        () -> {
          notificationChannelConfigServiceRequestValidator.validateWebhookHttpSupport(
              notificationChannelWithHttpUrl,
              config.getConfig(NOTIFICATION_CHANNEL_CONFIG_SERVICE_CONFIG));
        },
        "RuntimeException was expected");
    NotificationChannelMutableData notificationChannelWithInvalidUrl =
        NotificationChannelMutableData.newBuilder()
            .setChannelName("testChannel")
            .addWebhookChannelConfig(WebhookChannelConfig.newBuilder().setUrl("localhost"))
            .build();
    Assertions.assertThrows(
        RuntimeException.class,
        () -> {
          notificationChannelConfigServiceRequestValidator.validateWebhookHttpSupport(
              notificationChannelWithInvalidUrl,
              config.getConfig(NOTIFICATION_CHANNEL_CONFIG_SERVICE_CONFIG));
        },
        "RuntimeException was expected");

    NotificationChannelMutableData notificationChannelMutableDataWithHttpsUrl =
        NotificationChannelMutableData.newBuilder()
            .setChannelName("testChannel")
            .addWebhookChannelConfig(
                WebhookChannelConfig.newBuilder().setUrl("https://localhost:9000/test"))
            .build();
    notificationChannelConfigServiceRequestValidator.validateWebhookHttpSupport(
        notificationChannelMutableDataWithHttpsUrl,
        config.getConfig(NOTIFICATION_CHANNEL_CONFIG_SERVICE_CONFIG));
  }
}
