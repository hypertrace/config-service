package org.hypertrace.notification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

import java.util.List;
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
import org.hypertrace.notification.config.service.v1.WebhookChannelConfig;
import org.hypertrace.notification.config.service.v1.WebhookHeader;
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
                mockGenericConfigService.channel(), configChangeEventGenerator))
        .start();

    channelStub =
        NotificationChannelConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
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

  @Test
  void testIdPropagation() {
    NotificationChannelMutableData data =
        NotificationChannelMutableData.newBuilder()
            .setChannelName("test-channel")
            .addWebhookChannelConfig(WebhookChannelConfig.newBuilder())
            .build();

    NotificationChannel channel =
        channelStub
            .createNotificationChannel(
                CreateNotificationChannelRequest.newBuilder()
                    .setNotificationChannelMutableData(data)
                    .build())
            .getNotificationChannel();

    String configId =
        channel.getNotificationChannelMutableData().getWebhookChannelConfigList().get(0).getId();

    // id should get generated
    assertFalse(configId.isEmpty());

    data =
        NotificationChannelMutableData.newBuilder()
            .setChannelName("test-channel")
            .addWebhookChannelConfig(WebhookChannelConfig.newBuilder().setId(configId))
            .build();

    NotificationChannel updatedChannel =
        channelStub
            .updateNotificationChannel(
                UpdateNotificationChannelRequest.newBuilder()
                    .setNotificationChannelMutableData(data)
                    .setId(channel.getId())
                    .build())
            .getNotificationChannel();

    // id should not get generated
    assertEquals(
        configId,
        updatedChannel
            .getNotificationChannelMutableData()
            .getWebhookChannelConfigList()
            .get(0)
            .getId());
  }

  @Test
  void testHeaderPopulation() {
    NotificationChannelMutableData data =
        NotificationChannelMutableData.newBuilder()
            .setChannelName("test-channel")
            .addWebhookChannelConfig(WebhookChannelConfig.newBuilder())
            .build();

    NotificationChannel channel =
        channelStub
            .createNotificationChannel(
                CreateNotificationChannelRequest.newBuilder()
                    .setNotificationChannelMutableData(data)
                    .build())
            .getNotificationChannel();

    // no secret header
    NotificationChannel updatedChannel =
        channelStub
            .updateNotificationChannel(
                UpdateNotificationChannelRequest.newBuilder()
                    .setNotificationChannelMutableData(data)
                    .setId(channel.getId())
                    .build())
            .getNotificationChannel();

    assertEquals(
        data.getWebhookChannelConfigList().get(0).getHeadersList(),
        updatedChannel
            .getNotificationChannelMutableData()
            .getWebhookChannelConfigList()
            .get(0)
            .getHeadersList());

    // add a new config with secret headers
    data =
        NotificationChannelMutableData.newBuilder()
            .setChannelName("test-channel")
            .addWebhookChannelConfig(
                WebhookChannelConfig.newBuilder()
                    .addHeaders(
                        WebhookHeader.newBuilder()
                            .setName("test-header")
                            .setValue("test-encrypted-header")
                            .setIsSecret(true)))
            .build();

    updatedChannel =
        channelStub
            .updateNotificationChannel(
                UpdateNotificationChannelRequest.newBuilder()
                    .setNotificationChannelMutableData(data)
                    .setId(channel.getId())
                    .build())
            .getNotificationChannel();

    assertEquals(
        data.getWebhookChannelConfigList().get(0).getHeadersList(),
        updatedChannel
            .getNotificationChannelMutableData()
            .getWebhookChannelConfigList()
            .get(0)
            .getHeadersList());

    List<NotificationChannel> notificationChannels =
        channelStub
            .getAllNotificationChannels(GetAllNotificationChannelsRequest.getDefaultInstance())
            .getNotificationChannelsList();
    assertEquals(
        1,
        notificationChannels
            .get(0)
            .getNotificationChannelMutableData()
            .getWebhookChannelConfigList()
            .size());

    String configId =
        updatedChannel
            .getNotificationChannelMutableData()
            .getWebhookChannelConfigList()
            .get(0)
            .getId();

    // add secret header to existing config
    data =
        NotificationChannelMutableData.newBuilder()
            .setChannelName("test-channel")
            .addWebhookChannelConfig(
                WebhookChannelConfig.newBuilder()
                    .setId(configId)
                    .addHeaders(
                        WebhookHeader.newBuilder()
                            .setName("test-header")
                            .setValue("")
                            .setIsSecret(true))
                    .addHeaders(
                        WebhookHeader.newBuilder()
                            .setName("test-header-2")
                            .setValue("test-encrypted-header-2")
                            .setIsSecret(true)))
            .build();

    updatedChannel =
        channelStub
            .updateNotificationChannel(
                UpdateNotificationChannelRequest.newBuilder()
                    .setNotificationChannelMutableData(data)
                    .setId(channel.getId())
                    .build())
            .getNotificationChannel();

    NotificationChannelMutableData expectedData =
        NotificationChannelMutableData.newBuilder()
            .setChannelName("test-channel")
            .addWebhookChannelConfig(
                WebhookChannelConfig.newBuilder()
                    .setId(configId)
                    .addHeaders(
                        WebhookHeader.newBuilder()
                            .setName("test-header")
                            .setValue("test-encrypted-header")
                            .setIsSecret(true))
                    .addHeaders(
                        WebhookHeader.newBuilder()
                            .setName("test-header-2")
                            .setValue("test-encrypted-header-2")
                            .setIsSecret(true)))
            .build();

    assertEquals(
        expectedData.getWebhookChannelConfigList().get(0).getHeadersList(),
        updatedChannel
            .getNotificationChannelMutableData()
            .getWebhookChannelConfigList()
            .get(0)
            .getHeadersList());

    // add non-secret header to existing config
    data =
        NotificationChannelMutableData.newBuilder()
            .setChannelName("test-channel")
            .addWebhookChannelConfig(
                WebhookChannelConfig.newBuilder()
                    .setId(configId)
                    .addHeaders(
                        WebhookHeader.newBuilder()
                            .setName("test-header")
                            .setValue("")
                            .setIsSecret(true))
                    .addHeaders(
                        WebhookHeader.newBuilder()
                            .setName("test-header-2")
                            .setValue("")
                            .setIsSecret(true))
                    .addHeaders(
                        WebhookHeader.newBuilder()
                            .setName("test-header-3")
                            .setValue("test-header-3")
                            .setIsSecret(false)))
            .build();

    updatedChannel =
        channelStub
            .updateNotificationChannel(
                UpdateNotificationChannelRequest.newBuilder()
                    .setNotificationChannelMutableData(data)
                    .setId(channel.getId())
                    .build())
            .getNotificationChannel();

    expectedData =
        NotificationChannelMutableData.newBuilder()
            .setChannelName("test-channel")
            .addWebhookChannelConfig(
                WebhookChannelConfig.newBuilder()
                    .setId(configId)
                    .addHeaders(
                        WebhookHeader.newBuilder()
                            .setName("test-header")
                            .setValue("test-encrypted-header")
                            .setIsSecret(true))
                    .addHeaders(
                        WebhookHeader.newBuilder()
                            .setName("test-header-2")
                            .setValue("test-encrypted-header-2")
                            .setIsSecret(true))
                    .addHeaders(
                        WebhookHeader.newBuilder()
                            .setName("test-header-3")
                            .setValue("test-header-3")
                            .setIsSecret(false)))
            .build();

    assertEquals(
        expectedData.getWebhookChannelConfigList().get(0).getHeadersList(),
        updatedChannel
            .getNotificationChannelMutableData()
            .getWebhookChannelConfigList()
            .get(0)
            .getHeadersList());
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
