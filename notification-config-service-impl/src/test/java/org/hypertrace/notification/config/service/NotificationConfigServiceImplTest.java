package org.hypertrace.notification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.notification.config.service.v1.CreateNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.CreateNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.DeleteNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.DeleteNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.EmailChannelConfig;
import org.hypertrace.notification.config.service.v1.GetAllNotificationChannelsRequest;
import org.hypertrace.notification.config.service.v1.GetAllNotificationRulesRequest;
import org.hypertrace.notification.config.service.v1.NotificationChannel;
import org.hypertrace.notification.config.service.v1.NotificationChannelMutableData;
import org.hypertrace.notification.config.service.v1.NotificationConfigServiceGrpc;
import org.hypertrace.notification.config.service.v1.NotificationRule;
import org.hypertrace.notification.config.service.v1.NotificationRuleMutableData;
import org.hypertrace.notification.config.service.v1.UpdateNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.UpdateNotificationRuleRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class NotificationConfigServiceImplTest {

  MockGenericConfigService mockGenericConfigService;
  NotificationConfigServiceGrpc.NotificationConfigServiceBlockingStub notificationStub;

  @BeforeEach
  void beforeEach() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();

    mockGenericConfigService
        .addService(new NotificationConfigServiceImpl(mockGenericConfigService.channel()))
        .start();

    notificationStub =
        NotificationConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
  }

  @Test
  void createReadUpdateDeleteNotificationRules() {
    NotificationChannelMutableData notificationChannelMutableData1 =
        getNotificationChannelMutableData("channel1");
    NotificationChannel channel =
        notificationStub
            .createNotificationChannel(
                CreateNotificationChannelRequest.newBuilder()
                    .setNotificationChannelMutableData(notificationChannelMutableData1)
                    .build())
            .getNotificationChannel();
    NotificationRuleMutableData notificationRuleMutableData1 =
        getNotificationRuleMutableData("rule1", channel.getId());
    NotificationRuleMutableData notificationRuleMutableData2 =
        getNotificationRuleMutableData("rule2", channel.getId());
    NotificationRule notificationRule1 =
        notificationStub
            .createNotificationRule(
                CreateNotificationRuleRequest.newBuilder()
                    .setNotificationRuleMutableData(notificationRuleMutableData1)
                    .build())
            .getNotificationRule();
    assertEquals(
        getNotificationRule(notificationRuleMutableData1, notificationRule1.getId()),
        notificationRule1);

    NotificationRule notificationRule2 =
        notificationStub
            .createNotificationRule(
                CreateNotificationRuleRequest.newBuilder()
                    .setNotificationRuleMutableData(notificationRuleMutableData2)
                    .build())
            .getNotificationRule();

    assertEquals(
        List.of(notificationRule2, notificationRule1),
        notificationStub
            .getAllNotificationRules(GetAllNotificationRulesRequest.getDefaultInstance())
            .getNotificationRulesList());

    NotificationRule ruleToUpdate =
        notificationRule1.toBuilder()
            .setNotificationRuleMutableData(
                notificationRule1.getNotificationRuleMutableData().toBuilder()
                    .setRuleName("rule1a")
                    .build())
            .build();
    NotificationRule updatedRule =
        notificationStub
            .updateNotificationRule(
                UpdateNotificationRuleRequest.newBuilder()
                    .setNotificationRule(ruleToUpdate)
                    .build())
            .getNotificationRule();
    assertEquals(ruleToUpdate, updatedRule);

    assertEquals(
        List.of(notificationRule2, updatedRule),
        notificationStub
            .getAllNotificationRules(GetAllNotificationRulesRequest.getDefaultInstance())
            .getNotificationRulesList());

    notificationStub.deleteNotificationRule(
        DeleteNotificationRuleRequest.newBuilder()
            .setNotificationRuleId(notificationRule2.getId())
            .build());
    assertEquals(
        List.of(updatedRule),
        notificationStub
            .getAllNotificationRules(GetAllNotificationRulesRequest.getDefaultInstance())
            .getNotificationRulesList());
  }

  @Test
  void createReadUpdateDeleteNotificationChannels() {
    NotificationChannelMutableData notificationChannelMutableData1 =
        getNotificationChannelMutableData("channel1");
    NotificationChannelMutableData notificationChannelMutableData2 =
        getNotificationChannelMutableData("channel2");
    NotificationChannel notificationChannel1 =
        notificationStub
            .createNotificationChannel(
                CreateNotificationChannelRequest.newBuilder()
                    .setNotificationChannelMutableData(notificationChannelMutableData1)
                    .build())
            .getNotificationChannel();
    assertEquals(
        getNotificationChannel(notificationChannelMutableData1, notificationChannel1.getId()),
        notificationChannel1);

    NotificationChannel notificationChannel2 =
        notificationStub
            .createNotificationChannel(
                CreateNotificationChannelRequest.newBuilder()
                    .setNotificationChannelMutableData(notificationChannelMutableData2)
                    .build())
            .getNotificationChannel();

    assertEquals(
        List.of(notificationChannel2, notificationChannel1),
        notificationStub
            .getAllNotificationChannels(GetAllNotificationChannelsRequest.getDefaultInstance())
            .getNotificationChannelsList());

    NotificationChannel channelToUpdate =
        notificationChannel1.toBuilder()
            .setNotificationChannelMutableData(
                notificationChannel1.getNotificationChannelMutableData().toBuilder()
                    .setChannelName("Channel1a"))
            .build();
    NotificationChannel updatedChannel =
        notificationStub
            .updateNotificationChannel(
                UpdateNotificationChannelRequest.newBuilder()
                    .setNotificationChannel(channelToUpdate)
                    .build())
            .getNotificationChannel();
    assertEquals(channelToUpdate, updatedChannel);

    assertEquals(
        List.of(notificationChannel2, updatedChannel),
        notificationStub
            .getAllNotificationChannels(GetAllNotificationChannelsRequest.getDefaultInstance())
            .getNotificationChannelsList());

    notificationStub.deleteNotificationChannel(
        DeleteNotificationChannelRequest.newBuilder()
            .setNotificationChannelId(notificationChannel2.getId())
            .build());
    assertEquals(
        List.of(updatedChannel),
        notificationStub
            .getAllNotificationChannels(GetAllNotificationChannelsRequest.getDefaultInstance())
            .getNotificationChannelsList());
  }

  private NotificationRuleMutableData getNotificationRuleMutableData(
      String name, String channelId) {
    return NotificationRuleMutableData.newBuilder()
        .setRuleName(name)
        .setDescription("sample rule")
        .setChannelId(channelId)
        .setEventConditionType("metricAnomalyEventCondition")
        .setEventConditionId("rule-1")
        .build();
  }

  private NotificationRule getNotificationRule(
      NotificationRuleMutableData notificationRuleMutableData, String id) {
    NotificationRule.Builder builder =
        NotificationRule.newBuilder()
            .setId(id)
            .setNotificationRuleMutableData(notificationRuleMutableData);
    return builder.build();
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
