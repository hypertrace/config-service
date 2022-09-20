package ai.traceable.userattribution.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import ai.traceable.config.utils.ObjectDiffer;
import ai.traceable.config.utils.RankCalculator;
import ai.traceable.config.utils.RankCalculator.RankConfig;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.userattribution.config.service.store.UserAttributionRuleGenerator;
import ai.traceable.userattribution.config.service.store.UserAttributionRuleStore;
import ai.traceable.userattribution.config.service.v1.CreateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.DeleteUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest.GetUserAttributionRulesFilter;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest.GetUserAttributionRulesFilter.ScopeFilter;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest.GetUserAttributionRulesFilter.ScopeFilter.EnvironmentScopeFilter;
import ai.traceable.userattribution.config.service.v1.RankUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.UpdateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc.UserAttributionConfigServiceBlockingStub;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.CustomUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope.CustomScope;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope.EnvironmentScope;
import ai.traceable.userattribution.config.service.validation.UserAttributionConfigRequestValidator;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UserAttributionConfigServiceImplTest {
  private static final UserAttributionRuleData RULE_DATA =
      UserAttributionRuleData.newBuilder()
          .setCustomData(CustomUserAttributionRuleData.newBuilder().setYaml("key: value"))
          .build();
  UserAttributionConfigServiceBlockingStub userAttributionStub;
  MockGenericConfigService mockGenericConfigService;

  @BeforeEach
  void beforeEach() {
    this.mockGenericConfigService =
        new MockGenericConfigService()
            .mockUpsert()
            .mockGet()
            .mockGetAll()
            .mockDelete()
            .mockUpsertAll();

    ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());

    RankCalculator<UserAttributionRule, String> rankCalculator =
        new RankCalculator<>(
            new RankConfig<>(
                UserAttributionRule::getRank,
                UserAttributionRule::getId,
                (rule, rank) -> rule.toBuilder().setRank(rank).build()));

    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    this.mockGenericConfigService
        .addService(
            new UserAttributionConfigServiceImpl(
                new UserAttributionConfigRequestValidator(),
                new UserAttributionRuleStore(
                    genericStub, configChangeEventGenerator, rankCalculator),
                new UserAttributionRuleGenerator(new UuidGenerator()),
                rankCalculator,
                new ObjectDiffer()))
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
            .getRules(0);

    UserAttributionRule firstUpdated =
        this.userAttributionStub
            .updateUserAttributionRule(
                UpdateUserAttributionRuleRequest.newBuilder()
                    .setRule(firstCreated.toBuilder().setName("first updated").setDisabled(true))
                    .build())
            .getRule();

    assertEquals(1, firstUpdated.getRank());
    assertTrue(firstUpdated.getDisabled());

    List<UserAttributionRule> afterSecondCreate =
        this.userAttributionStub
            .createUserAttributionRule(
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("second")
                    .setData(RULE_DATA)
                    .build())
            .getRulesList();
    assertEquals(firstUpdated, afterSecondCreate.get(0));
    UserAttributionRule secondCreated = afterSecondCreate.get(1);
    assertEquals(2, secondCreated.getRank());
    assertEquals("second", secondCreated.getName());

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

  @Test
  void createAndGetRulesWithFilter() {
    UserAttributionRule firstCreated =
        this.userAttributionStub
            .createUserAttributionRule(
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("first")
                    .setData(RULE_DATA)
                    .setScope(buildRuleScope(List.of("env1")))
                    .build())
            .getRules(0);

    List<UserAttributionRule> afterSecondCreate =
        this.userAttributionStub
            .createUserAttributionRule(
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("second")
                    .setData(RULE_DATA)
                    .setScope(buildRuleScope(List.of("env2")))
                    .build())
            .getRulesList();

    List<UserAttributionRule> afterThirdCreate =
        this.userAttributionStub
            .createUserAttributionRule(
                CreateUserAttributionRuleRequest.newBuilder()
                    .setName("third")
                    .setData(RULE_DATA)
                    .build())
            .getRulesList();

    // should return the rules which are scoped to atleast one of the envs in the filter, or are not
    // scoped to any env
    assertEquals(
        List.of(afterThirdCreate.get(0), afterThirdCreate.get(2)),
        this.userAttributionStub
            .getUserAttributionRules(
                GetUserAttributionRulesRequest.newBuilder()
                    .setFilter(
                        GetUserAttributionRulesFilter.newBuilder()
                            .setScopeFilter(buildEnvironmentScopeFilter(List.of("env0", "env1")))
                            .build())
                    .build())
            .getRulesList());

    // should return only the rules which are not targeted to an env
    assertEquals(
        List.of(afterThirdCreate.get(2)),
        this.userAttributionStub
            .getUserAttributionRules(
                GetUserAttributionRulesRequest.newBuilder()
                    .setFilter(
                        GetUserAttributionRulesFilter.newBuilder()
                            .setScopeFilter(buildEnvironmentScopeFilter(Collections.emptyList()))
                            .build())
                    .build())
            .getRulesList());

    UserAttributionRule firstUpdated =
        this.userAttributionStub
            .updateUserAttributionRule(
                UpdateUserAttributionRuleRequest.newBuilder()
                    .setRule(firstCreated.toBuilder().setDisabled(true))
                    .build())
            .getRule();

    // should return all the rules which are not disabled
    assertEquals(
        List.of(afterThirdCreate.get(1), afterThirdCreate.get(2)),
        this.userAttributionStub
            .getUserAttributionRules(
                GetUserAttributionRulesRequest.newBuilder()
                    .setFilter(
                        GetUserAttributionRulesFilter.newBuilder().setDisabled(false).build())
                    .build())
            .getRulesList());
  }

  private UserAttributionRuleScope buildRuleScope(List<String> environmentNames) {
    List<EnvironmentScope> environmentScopes =
        environmentNames.stream()
            .map(env -> EnvironmentScope.newBuilder().setEnvironmentName(env).build())
            .collect(Collectors.toUnmodifiableList());
    return UserAttributionRuleScope.newBuilder()
        .setCustomScope(CustomScope.newBuilder().addAllEnvironmentScopes(environmentScopes).build())
        .build();
  }

  private ScopeFilter buildEnvironmentScopeFilter(List<String> environmentNames) {
    return ScopeFilter.newBuilder()
        .setEnvironmentScopeFilter(
            EnvironmentScopeFilter.newBuilder().addAllEnvironmentNames(environmentNames).build())
        .build();
  }
}
