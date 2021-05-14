package ai.traceable.userattribution.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.userattribution.config.service.store.UserAttributionRuleGenerator;
import ai.traceable.userattribution.config.service.store.UserAttributionRuleRankCalculator;
import ai.traceable.userattribution.config.service.store.UserAttributionRuleStore;
import ai.traceable.userattribution.config.service.v1.CreateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.DeleteUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest;
import ai.traceable.userattribution.config.service.v1.RankUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.UpdateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc.UserAttributionConfigServiceBlockingStub;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.CustomUserAttributionRuleData;
import ai.traceable.userattribution.config.service.validation.UserAttributionConfigRequestValidator;
import java.util.List;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UserAttributionConfigServiceImplTest {
  private static final UserAttributionRuleData RULE_DATA =
      UserAttributionRuleData.newBuilder()
          .setCustomData(CustomUserAttributionRuleData.newBuilder().setYaml("some-yaml"))
          .build();
  UserAttributionConfigServiceBlockingStub userAttributionStub;
  MockGenericConfigService mockGenericConfigService;

  @BeforeEach
  void beforeEach() {
    this.mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();

    ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());

    this.mockGenericConfigService
        .addService(
            new UserAttributionConfigServiceImpl(
                new UserAttributionConfigRequestValidator(),
                new UserAttributionRuleStore(genericStub),
                new UserAttributionRuleGenerator(),
                new UserAttributionRuleRankCalculator(),
                new UserAttributionRuleDiffer()))
        .start();

    this.userAttributionStub =
        UserAttributionConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
  }

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  @Test
  void lotsOfCrud() {
    UserAttributionRule firstCreated =
        this.userAttributionStub
            .createUserAttributionRule(
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("first")
                    .setData(RULE_DATA)
                    .build())
            .getRule();

    UserAttributionRule firstUpdated =
        this.userAttributionStub
            .updateUserAttributionRule(
                UpdateUserAttributionRuleRequest.newBuilder()
                    .setRule(firstCreated.toBuilder().setName("first updated"))
                    .build())
            .getRule();

    UserAttributionRule secondCreated =
        this.userAttributionStub
            .createUserAttributionRule(
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("second")
                    .setData(RULE_DATA)
                    .build())
            .getRule();

    assertEquals(
        List.of(firstUpdated, secondCreated),
        this.userAttributionStub
            .getUserAttributionRules(GetUserAttributionRulesRequest.getDefaultInstance())
            .getRulesList());

    assertEquals(
        List.of(withRank(secondCreated, 1), withRank(firstUpdated, 2)),
        this.userAttributionStub
            .rankUserAttributionRule(
                RankUserAttributionRuleRequest.newBuilder()
                    .setIdToUpdate(firstUpdated.getId())
                    .setPrecedingRuleId(secondCreated.getId())
                    .build())
            .getRulesList());

    assertEquals(
        List.of(withRank(firstUpdated, 1), withRank(secondCreated, 2)),
        this.userAttributionStub
            .rankUserAttributionRule(
                RankUserAttributionRuleRequest.newBuilder()
                    .setIdToUpdate(firstUpdated.getId())
                    .build())
            .getRulesList());

    assertEquals(
        List.of(withRank(secondCreated, 1)),
        this.userAttributionStub
            .deleteUserAttributionRule(
                DeleteUserAttributionRuleRequest.newBuilder()
                    .setRuleId(firstUpdated.getId())
                    .build())
            .getRulesList());

    assertEquals(
        List.of(withRank(secondCreated, 1)),
        this.userAttributionStub
            .getUserAttributionRules(GetUserAttributionRulesRequest.getDefaultInstance())
            .getRulesList());
  }

  private UserAttributionRule withRank(UserAttributionRule rule, int rank) {
    return rule.toBuilder().setRank(rank).build();
  }
}
