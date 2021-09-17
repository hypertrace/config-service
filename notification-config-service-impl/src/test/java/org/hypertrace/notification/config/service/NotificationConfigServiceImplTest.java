package org.hypertrace.notification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.notification.config.service.v1.CreateNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.CreateNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.DeleteNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.DeleteNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.GetAllNotificationChannelsRequest;
import org.hypertrace.notification.config.service.v1.GetAllNotificationRulesRequest;
import org.hypertrace.notification.config.service.v1.NewNotificationChannel;
import org.hypertrace.notification.config.service.v1.NewNotificationRule;
import org.hypertrace.notification.config.service.v1.NotificationChannel;
import org.hypertrace.notification.config.service.v1.NotificationChannelConfig;
import org.hypertrace.notification.config.service.v1.NotificationConfigServiceGrpc;
import org.hypertrace.notification.config.service.v1.NotificationRule;
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
    NewNotificationChannel newNotificationChannel1 = getNewNotificationChannel("channel1");
    NotificationChannel channel =
        notificationStub
            .createNotificationChannel(
                CreateNotificationChannelRequest.newBuilder()
                    .setNewNotificationChannel(newNotificationChannel1)
                    .build())
            .getNotificationChannel();
    NewNotificationRule newNotificationRule1 = getNewNotificationRule("rule1", channel.getId());
    NewNotificationRule newNotificationRule2 = getNewNotificationRule("rule2", channel.getId());
    NotificationRule notificationRule1 =
        notificationStub
            .createNotificationRule(
                CreateNotificationRuleRequest.newBuilder()
                    .setNewNotificationRule(newNotificationRule1)
                    .build())
            .getNotificationRule();
    assertEquals(
        getNotificationRule(newNotificationRule1, notificationRule1.getId()), notificationRule1);

    NotificationRule notificationRule2 =
        notificationStub
            .createNotificationRule(
                CreateNotificationRuleRequest.newBuilder()
                    .setNewNotificationRule(newNotificationRule2)
                    .build())
            .getNotificationRule();

    assertEquals(
        List.of(notificationRule2, notificationRule1),
        notificationStub
            .getAllNotificationRules(GetAllNotificationRulesRequest.getDefaultInstance())
            .getNotificationRulesList());

    NotificationRule ruleToUpdate = notificationRule1.toBuilder().setRuleName("rule1a").build();
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
    NewNotificationChannel newNotificationChannel1 = getNewNotificationChannel("channel1");
    NewNotificationChannel newNotificationChannel2 = getNewNotificationChannel("channel2");
    NotificationChannel notificationChannel1 =
        notificationStub
            .createNotificationChannel(
                CreateNotificationChannelRequest.newBuilder()
                    .setNewNotificationChannel(newNotificationChannel1)
                    .build())
            .getNotificationChannel();
    assertEquals(
        getNotificationChannel(newNotificationChannel1, notificationChannel1.getId()),
        notificationChannel1);

    NotificationChannel notificationChannel2 =
        notificationStub
            .createNotificationChannel(
                CreateNotificationChannelRequest.newBuilder()
                    .setNewNotificationChannel(newNotificationChannel2)
                    .build())
            .getNotificationChannel();

    assertEquals(
        List.of(notificationChannel2, notificationChannel1),
        notificationStub
            .getAllNotificationChannels(GetAllNotificationChannelsRequest.getDefaultInstance())
            .getNotificationChannelsList());

    NotificationChannel channelToUpdate =
        notificationChannel1.toBuilder().setChannelName("Channel1a").build();
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

  private NewNotificationRule getNewNotificationRule(String name, String channelId) {
    return NewNotificationRule.newBuilder()
        .setRuleName(name)
        .setDescription("sample rule")
        .setEnvironment("dev")
        .setChannelId(channelId)
        .setEventConditionType("metricAnomalyEventCondition")
        .setEventConditionId("rule-1")
        .build();
  }

  private NotificationRule getNotificationRule(NewNotificationRule newNotificationRule, String id) {
    NotificationRule.Builder builder =
        NotificationRule.newBuilder()
            .setId(id)
            .setRuleName(newNotificationRule.getRuleName())
            .setDescription(newNotificationRule.getDescription())
            .setEnvironment(newNotificationRule.getEnvironment())
            .setEventConditionId(newNotificationRule.getEventConditionId())
            .setEventConditionType(newNotificationRule.getEventConditionType())
            .setChannelId(newNotificationRule.getChannelId());
    return builder.build();
  }

  private NewNotificationChannel getNewNotificationChannel(String name) {
    return NewNotificationChannel.newBuilder()
        .setChannelName(name)
        .setNotificationChannelConfig(NotificationChannelConfig.getDefaultInstance())
        .build();
  }

  private NotificationChannel getNotificationChannel(
      NewNotificationChannel newNotificationChannel, String id) {
    NotificationChannel.Builder builder =
        NotificationChannel.newBuilder()
            .setId(id)
            .setChannelName(newNotificationChannel.getChannelName())
            .setNotificationChannelConfig(newNotificationChannel.getNotificationChannelConfig());
    return builder.build();
  }
}