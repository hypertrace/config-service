package org.hypertrace.notification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.notification.config.service.v1.CreateNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.DeleteNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.GetAllNotificationRulesRequest;
import org.hypertrace.notification.config.service.v1.NotificationRule;
import org.hypertrace.notification.config.service.v1.NotificationRuleConfigServiceGrpc;
import org.hypertrace.notification.config.service.v1.NotificationRuleMutableData;
import org.hypertrace.notification.config.service.v1.UpdateNotificationRuleRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NotificationRuleConfigServiceImplTest {

  MockGenericConfigService mockGenericConfigService;
  NotificationRuleConfigServiceGrpc.NotificationRuleConfigServiceBlockingStub notificationStub;

  @BeforeEach
  void beforeEach() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();

    mockGenericConfigService
        .addService(new NotificationRuleConfigServiceImpl(mockGenericConfigService.channel()))
        .start();

    notificationStub =
        NotificationRuleConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
  }

  @Test
  void createReadUpdateDeleteNotificationRules() {
    NotificationRuleMutableData notificationRuleMutableData1 =
        getNotificationRuleMutableData("rule1", "channel1");
    NotificationRuleMutableData notificationRuleMutableData2 =
        getNotificationRuleMutableData("rule2", "channel1");
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
                    .setNotificationRuleMutableData(ruleToUpdate.getNotificationRuleMutableData())
                    .setId(ruleToUpdate.getId())
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
}
