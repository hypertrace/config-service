package org.hypertrace.notification.config.service;

import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.notification.config.service.v1.NotificationChannel;
import org.hypertrace.notification.config.service.v1.NotificationChannelMutableData;
import org.hypertrace.notification.config.service.v1.WebhookChannelConfig;
import org.hypertrace.notification.config.service.v1.WebhookHeader;

public class Utils {

  private final NotificationChannelStore notificationChannelStore;

  public Utils(NotificationChannelStore notificationChannelStore) {
    this.notificationChannelStore = notificationChannelStore;
  }

  public NotificationChannelMutableData getIdPopulatedData(NotificationChannelMutableData data) {
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

  public NotificationChannelMutableData getHeaderPopulatedData(
      NotificationChannelMutableData data, String channelId) {
    return NotificationChannelMutableData.newBuilder()
        .setChannelName(data.getChannelName())
        .addAllEmailChannelConfig(data.getEmailChannelConfigList())
        .addAllWebhookChannelConfig(
            data.getWebhookChannelConfigList().stream()
                .map(config -> transformConfig(config, channelId))
                .collect(Collectors.toList()))
        .build();
  }

  private WebhookChannelConfig transformConfig(WebhookChannelConfig config, String channelId) {
    List<WebhookHeader> updatedHeaders = config.getHeadersList();
    if (hasSecretHeaders(updatedHeaders)) {
      Optional<WebhookChannelConfig> currentConfig = getCurrentConfig(config.getId(), channelId);

      // if newly added config, return as no encrypted secret headers
      if (currentConfig.isEmpty()) {
        return config;
      }

      return config.toBuilder()
          .clearHeaders()
          .addAllHeaders(transformHeaders(updatedHeaders, currentConfig.get().getHeadersList()))
          .build();
    } else {
      return config;
    }
  }

  private boolean hasSecretHeaders(List<WebhookHeader> webhookHeaderList) {
    return webhookHeaderList.stream().anyMatch(WebhookHeader::getIsSecret);
  }

  @SneakyThrows
  private List<WebhookHeader> transformHeaders(
      List<WebhookHeader> updatedHeaderList, List<WebhookHeader> currentHeaderList) {
    return updatedHeaderList.stream()
        .map(
            header -> {
              if (header.getIsSecret() && header.getValue().isEmpty()) {
                return header.toBuilder()
                    .setValue(
                        currentHeaderList.stream()
                            .filter(
                                currentHeader -> currentHeader.getName().equals(header.getName()))
                            .findFirst()
                            .get()
                            .getValue())
                    .build();
              } else {
                return header;
              }
            })
        .collect(Collectors.toList());
  }

  private Optional<WebhookChannelConfig> getCurrentConfig(String configId, String channelId) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    NotificationChannel channel =
        notificationChannelStore
            .getData(requestContext, channelId)
            .orElseThrow(Status.NOT_FOUND::asRuntimeException);

    return channel.getNotificationChannelMutableData().getWebhookChannelConfigList().stream()
        .filter(config -> config.getId().equals(configId))
        .findFirst();
  }
}
