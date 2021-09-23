package org.hypertrace.notification.config.service;

import static org.hypertrace.notification.config.service.NotificationConfigServiceConstants.NOTIFICATION_CHANNEL_CONFIG_RESOURCE_NAME;
import static org.hypertrace.notification.config.service.NotificationConfigServiceConstants.NOTIFICATION_CONFIG_NAMESPACE;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import io.grpc.Channel;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.notification.config.service.v1.NotificationChannel;

@Slf4j
public class NotificationChannelStore extends IdentifiedObjectStore<NotificationChannel> {

  public NotificationChannelStore(Channel channel) {
    super(
        ConfigServiceGrpc.newBlockingStub(channel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get()),
        NOTIFICATION_CONFIG_NAMESPACE,
        NOTIFICATION_CHANNEL_CONFIG_RESOURCE_NAME);
  }

  @Override
  protected Optional<NotificationChannel> buildObjectFromValue(Value value) {
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
  protected Value buildValueFromObject(NotificationChannel object) {
    return ConfigProtoConverter.convertToValue(object);
  }

  @Override
  protected String getContextFromObject(NotificationChannel object) {
    return object.getId();
  }
}
