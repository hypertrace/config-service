package org.hypertrace.notification.config.service;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import io.grpc.Channel;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.notification.config.service.v1.NotificationChannel;

@Slf4j
public class NotificationChannelStore extends IdentifiedObjectStore<NotificationChannel> {

  private static final String NOTIFICATION_CONFIG_NAMESPACE = "notification-v1";
  private static final String NOTIFICATION_CHANNEL_CONFIG_RESOURCE_NAME =
      "notificationChannelConfig";

  public NotificationChannelStore(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        ConfigServiceGrpc.newBlockingStub(channel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get()),
        NOTIFICATION_CONFIG_NAMESPACE,
        NOTIFICATION_CHANNEL_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<NotificationChannel> buildDataFromValue(Value value) {
    NotificationChannel.Builder builder = NotificationChannel.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Conversion failed. value {}", value, e);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(NotificationChannel object) {
    return ConfigProtoConverter.convertToValue(object);
  }

  @Override
  protected String getContextFromData(NotificationChannel object) {
    return object.getId();
  }
}
