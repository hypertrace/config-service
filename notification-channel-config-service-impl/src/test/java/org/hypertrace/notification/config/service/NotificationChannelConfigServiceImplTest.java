package org.hypertrace.notification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.notification.config.service.v1.CreateNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.DeleteNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.EmailChannelConfig;
import org.hypertrace.notification.config.service.v1.GetAllNotificationChannelsRequest;
import org.hypertrace.notification.config.service.v1.GetNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.NotificationChannel;
import org.hypertrace.notification.config.service.v1.NotificationChannelConfigServiceGrpc;
import org.hypertrace.notification.config.service.v1.NotificationChannelMutableData;
import org.hypertrace.notification.config.service.v1.UpdateNotificationChannelRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NotificationChannelConfigServiceImplTest {

  MockGenericConfigService mockGenericConfigService;
  NotificationChannelConfigServiceGrpc.NotificationChannelConfigServiceBlockingStub channelStub;

  @BeforeEach
  void beforeEach() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    mockGenericConfigService
        .addService(
            new NotificationChannelConfigServiceImpl(
                mockGenericConfigService.channel(), mock(Config.class), configChangeEventGenerator))
        .start();

    channelStub =
        NotificationChannelConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
  }

  @Test
  void testProtoChanges() throws IOException {
    String notificationChannelFileName =
        Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("sample_notification_channel.json"))
            .getPath();
    String notificationChannelInJson =
        new String(Files.readAllBytes(new File(notificationChannelFileName).toPath()));

    Value.Builder valueBuilder = Value.newBuilder();
    JsonFormat.parser().merge(notificationChannelInJson, valueBuilder);
    NotificationChannel.Builder builder = NotificationChannel.newBuilder();
    ConfigProtoConverter.mergeFromValue(valueBuilder.build(), builder);
    NotificationChannel channel = builder.build();

    assertEquals("ca793999-1ecd-4d8d-abf6-492ecd161350", channel.getId());
  }

  @Test
  void createReadUpdateDeleteNotificationChannels() {
    NotificationChannelMutableData notificationChannelMutableData1 =
        getNotificationChannelMutableData("channel1");
    NotificationChannelMutableData notificationChannelMutableData2 =
        getNotificationChannelMutableData("channel2");
    NotificationChannel notificationChannel1 =
        channelStub
            .createNotificationChannel(
                CreateNotificationChannelRequest.newBuilder()
                    .setNotificationChannelMutableData(notificationChannelMutableData1)
                    .build())
            .getNotificationChannel();
    assertEquals(
        getNotificationChannel(notificationChannelMutableData1, notificationChannel1.getId()),
        notificationChannel1);

    NotificationChannel notificationChannel2 =
        channelStub
            .createNotificationChannel(
                CreateNotificationChannelRequest.newBuilder()
                    .setNotificationChannelMutableData(notificationChannelMutableData2)
                    .build())
            .getNotificationChannel();

    assertEquals(
        notificationChannel1,
        channelStub
            .getNotificationChannel(
                GetNotificationChannelRequest.newBuilder()
                    .setNotificationChannelId(notificationChannel1.getId())
                    .build())
            .getNotificationChannel());
    assertEquals(
        notificationChannel2,
        channelStub
            .getNotificationChannel(
                GetNotificationChannelRequest.newBuilder()
                    .setNotificationChannelId(notificationChannel2.getId())
                    .build())
            .getNotificationChannel());

    assertEquals(
        List.of(notificationChannel2, notificationChannel1),
        channelStub
            .getAllNotificationChannels(GetAllNotificationChannelsRequest.getDefaultInstance())
            .getNotificationChannelsList());

    NotificationChannel channelToUpdate =
        notificationChannel1.toBuilder()
            .setNotificationChannelMutableData(
                notificationChannel1.getNotificationChannelMutableData().toBuilder()
                    .setChannelName("Channel1a"))
            .build();
    NotificationChannel updatedChannel =
        channelStub
            .updateNotificationChannel(
                UpdateNotificationChannelRequest.newBuilder()
                    .setNotificationChannelMutableData(
                        channelToUpdate.getNotificationChannelMutableData())
                    .setId(channelToUpdate.getId())
                    .build())
            .getNotificationChannel();
    assertEquals(channelToUpdate, updatedChannel);

    assertEquals(
        List.of(notificationChannel2, updatedChannel),
        channelStub
            .getAllNotificationChannels(GetAllNotificationChannelsRequest.getDefaultInstance())
            .getNotificationChannelsList());

    channelStub.deleteNotificationChannel(
        DeleteNotificationChannelRequest.newBuilder()
            .setNotificationChannelId(notificationChannel2.getId())
            .build());
    assertEquals(
        List.of(updatedChannel),
        channelStub
            .getAllNotificationChannels(GetAllNotificationChannelsRequest.getDefaultInstance())
            .getNotificationChannelsList());
  }

  private NotificationChannelMutableData getNotificationChannelMutableData(String name) {
    return NotificationChannelMutableData.newBuilder()
        .setChannelName(name)
        .addEmailChannelConfig(EmailChannelConfig.newBuilder().setAddress("localhost"))
        .build();
  }

  private NotificationChannel getNotificationChannel(
      NotificationChannelMutableData notificationChannelMutableData, String id) {
    NotificationChannel.Builder builder =
        NotificationChannel.newBuilder()
            .setId(id)
            .setNotificationChannelMutableData(notificationChannelMutableData);
    return builder.build();
  }
}
