package org.hypertrace.notification.config.service;

import static org.hypertrace.notification.config.service.NotificationChannelConfigServiceImpl.NOTIFICATION_CHANNEL_CONFIG_SERVICE_CONFIG;
import static org.hypertrace.notification.config.service.NotificationChannelConfigServiceRequestValidator.WEBHOOK_HTTP_SUPPORT_ENABLED;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.File;
import java.util.List;
import java.util.stream.Stream;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.notification.config.service.v1.CortexIntegrationChannelConfig;
import org.hypertrace.notification.config.service.v1.CrowdStrikeIntegrationChannelConfig;
import org.hypertrace.notification.config.service.v1.HttpEventCollectorChannelConfig;
import org.hypertrace.notification.config.service.v1.NotificationChannelMutableData;
import org.hypertrace.notification.config.service.v1.SplunkIntegrationChannelConfig;
import org.hypertrace.notification.config.service.v1.UpdateNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.WebhookChannelConfig;
import org.hypertrace.notification.config.service.v1.WebhookFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class NotificationChannelConfigServiceRequestValidatorTest {
  @ParameterizedTest(name = "{0}")
  @MethodSource("validHttpEventCollectorChannelConfigs")
  void acceptsValidHttpEventCollectorChannelConfig(
      String caseName, HttpEventCollectorChannelConfig httpEventCollectorChannelConfig) {
    NotificationChannelConfigServiceRequestValidator validator =
        new NotificationChannelConfigServiceRequestValidator();
    assertDoesNotThrow(
        () ->
            validator.validateUpdateNotificationChannelRequest(
                RequestContext.forTenantId("tenant1"),
                updateRequestWithHttpEventCollector(httpEventCollectorChannelConfig),
                null,
                List.of()));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidHttpEventCollectorChannelConfigs")
  void rejectsInvalidHttpEventCollectorChannelConfig(
      String caseName, HttpEventCollectorChannelConfig httpEventCollectorChannelConfig) {
    NotificationChannelConfigServiceRequestValidator validator =
        new NotificationChannelConfigServiceRequestValidator();
    assertThrows(
        RuntimeException.class,
        () ->
            validator.validateUpdateNotificationChannelRequest(
                RequestContext.forTenantId("tenant1"),
                updateRequestWithHttpEventCollector(httpEventCollectorChannelConfig),
                null,
                List.of()));
  }

  private static Stream<Arguments> validHttpEventCollectorChannelConfigs() {
    return Stream.of(
        Arguments.of(
            "splunk integration id present",
            HttpEventCollectorChannelConfig.newBuilder()
                .setSplunkIntegrationChannelConfig(
                    SplunkIntegrationChannelConfig.newBuilder()
                        .setSplunkIntegrationId("splunk-id")
                        .build())
                .build()),
        Arguments.of(
            "crowd strike integration id present",
            HttpEventCollectorChannelConfig.newBuilder()
                .setCrowdStrikeIntegrationChannelConfig(
                    CrowdStrikeIntegrationChannelConfig.newBuilder()
                        .setCrowdStrikeIntegrationId("crowdstrike-id")
                        .build())
                .build()),
        Arguments.of(
            "cortex integration id present",
            HttpEventCollectorChannelConfig.newBuilder()
                .setCortexIntegrationChannelConfig(
                    CortexIntegrationChannelConfig.newBuilder()
                        .setCortexIntegrationId("cortex-id")
                        .build())
                .build()));
  }

  private static Stream<Arguments> invalidHttpEventCollectorChannelConfigs() {
    return Stream.of(
        Arguments.of(
            "splunk integration id missing",
            HttpEventCollectorChannelConfig.newBuilder()
                .setSplunkIntegrationChannelConfig(
                    SplunkIntegrationChannelConfig.getDefaultInstance())
                .build()),
        Arguments.of(
            "crowd strike integration id missing",
            HttpEventCollectorChannelConfig.newBuilder()
                .setCrowdStrikeIntegrationChannelConfig(
                    CrowdStrikeIntegrationChannelConfig.getDefaultInstance())
                .build()),
        Arguments.of(
            "cortex integration id missing",
            HttpEventCollectorChannelConfig.newBuilder()
                .setCortexIntegrationChannelConfig(
                    CortexIntegrationChannelConfig.getDefaultInstance())
                .build()),
        Arguments.of(
            "unknown http event collector channel type",
            HttpEventCollectorChannelConfig.getDefaultInstance()));
  }

  private static UpdateNotificationChannelRequest updateRequestWithHttpEventCollector(
      HttpEventCollectorChannelConfig httpEventCollectorChannelConfig) {
    return UpdateNotificationChannelRequest.newBuilder()
        .setId("channel-id")
        .setNotificationChannelMutableData(
            NotificationChannelMutableData.newBuilder()
                .setChannelName("hec-channel")
                .addHttpEventCollectorChannelConfig(httpEventCollectorChannelConfig)
                .build())
        .build();
  }

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
