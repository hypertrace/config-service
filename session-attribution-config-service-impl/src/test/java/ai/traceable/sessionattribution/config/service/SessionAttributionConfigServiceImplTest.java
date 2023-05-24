package ai.traceable.sessionattribution.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import ai.traceable.config.utils.ObjectDiffer;
import ai.traceable.config.utils.RankCalculator;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.sessionattribution.config.service.store.SessionAttributionRuleGenerator;
import ai.traceable.sessionattribution.config.service.store.SessionAttributionRuleStore;
import ai.traceable.sessionattribution.config.service.v1.AttributeProjection;
import ai.traceable.sessionattribution.config.service.v1.CreateSessionAttributionRuleRequest;
import ai.traceable.sessionattribution.config.service.v1.DeleteSessionAttributionRuleRequest;
import ai.traceable.sessionattribution.config.service.v1.GetSessionAttributionRulesRequest;
import ai.traceable.sessionattribution.config.service.v1.LiteralValue;
import ai.traceable.sessionattribution.config.service.v1.MatchCondition;
import ai.traceable.sessionattribution.config.service.v1.MatchOperator;
import ai.traceable.sessionattribution.config.service.v1.RankSessionAttributionRuleRequest;
import ai.traceable.sessionattribution.config.service.v1.RequestAttributeKeyLocation;
import ai.traceable.sessionattribution.config.service.v1.RequestSessionTokenDetails;
import ai.traceable.sessionattribution.config.service.v1.SessionAttributionConfigServiceGrpc;
import ai.traceable.sessionattribution.config.service.v1.SessionAttributionRule;
import ai.traceable.sessionattribution.config.service.v1.SessionAttributionRuleStatus;
import ai.traceable.sessionattribution.config.service.v1.SessionTokenRule;
import ai.traceable.sessionattribution.config.service.v1.SessionTokenValueRule;
import ai.traceable.sessionattribution.config.service.v1.UpdateSessionAttributionRuleRequest;
import ai.traceable.sessionattribution.config.service.validation.SessionAttributionConfigRequestValidator;
import ai.traceable.sessionattribution.config.service.validation.SessionTokenRuleValidator;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SessionAttributionConfigServiceImplTest {
  private static final SessionTokenRule RULE =
      SessionTokenRule.newBuilder()
          .setRequestSessionTokenDetails(
              RequestSessionTokenDetails.newBuilder()
                  .setTokenLocation(
                      RequestAttributeKeyLocation.REQUEST_ATTRIBUTE_KEY_LOCATION_HEADER)
                  .build())
          .setTokenValueRule(
              SessionTokenValueRule.newBuilder()
                  .setTokenValueProjection(
                      AttributeProjection.newBuilder()
                          .setAttributeKeyMatchCondition(
                              MatchCondition.newBuilder()
                                  .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                  .setMatchValue(
                                      LiteralValue.newBuilder().setStringValue("authorization"))))
                  .build())
          .build();
  SessionAttributionConfigServiceGrpc.SessionAttributionConfigServiceBlockingStub
      sessionAttributionConfigServiceBlockingStub;
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

    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());

    RankCalculator<SessionAttributionRule, String> rankCalculator =
        new RankCalculator<>(
            new RankCalculator.RankConfig<>(
                SessionAttributionRule::getRank,
                SessionAttributionRule::getId,
                (rule, rank) -> rule.toBuilder().setRank(rank).build()));

    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    this.mockGenericConfigService
        .addService(
            new SessionAttributionConfigServiceImpl(
                new SessionAttributionConfigRequestValidator(new SessionTokenRuleValidator()),
                new SessionAttributionRuleStore(
                    genericStub, configChangeEventGenerator, rankCalculator),
                new SessionAttributionRuleGenerator(new UuidGenerator()),
                rankCalculator,
                new ObjectDiffer()))
        .start();

    this.sessionAttributionConfigServiceBlockingStub =
        SessionAttributionConfigServiceGrpc.newBlockingStub(
            this.mockGenericConfigService.channel());
  }

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  @Test
  void lotsOfCrud() {
    SessionAttributionRule firstCreated =
        this.sessionAttributionConfigServiceBlockingStub
            .createSessionAttributionRule(
                CreateSessionAttributionRuleRequest.newBuilder()
                    .setName("first")
                    .addTokenRules(RULE)
                    .build())
            .getRule();

    SessionAttributionRule firstUpdated =
        this.sessionAttributionConfigServiceBlockingStub
            .updateSessionAttributionRule(
                UpdateSessionAttributionRuleRequest.newBuilder()
                    .setName("first updated")
                    .setId(firstCreated.getId())
                    .setStatus(SessionAttributionRuleStatus.newBuilder().setDisabled(true))
                    .addTokenRules(RULE)
                    .build())
            .getRule();

    assertEquals(1, firstUpdated.getRank());
    assertTrue(firstUpdated.getStatus().getDisabled());

    SessionAttributionRule secondCreated =
        this.sessionAttributionConfigServiceBlockingStub
            .createSessionAttributionRule(
                CreateSessionAttributionRuleRequest.newBuilder()
                    .setName("second")
                    .addTokenRules(RULE)
                    .build())
            .getRule();
    assertEquals(2, secondCreated.getRank());
    assertEquals("second", secondCreated.getName());

    assertEquals(
        List.of(firstUpdated, secondCreated),
        this.sessionAttributionConfigServiceBlockingStub
            .getSessionAttributionRules(GetSessionAttributionRulesRequest.getDefaultInstance())
            .getRulesList());
    this.sessionAttributionConfigServiceBlockingStub.rankSessionAttributionRule(
        RankSessionAttributionRuleRequest.newBuilder()
            .setIdToUpdate(firstUpdated.getId())
            .setPrecedingRuleId(secondCreated.getId())
            .build());
    assertEquals(
        List.of(withRank(secondCreated, 1), withRank(firstUpdated, 2)),
        this.sessionAttributionConfigServiceBlockingStub
            .getSessionAttributionRules(GetSessionAttributionRulesRequest.getDefaultInstance())
            .getRulesList());
    this.sessionAttributionConfigServiceBlockingStub.deleteSessionAttributionRule(
        DeleteSessionAttributionRuleRequest.newBuilder().setId(firstUpdated.getId()).build());
    assertEquals(
        List.of(withRank(secondCreated, 1)),
        this.sessionAttributionConfigServiceBlockingStub
            .getSessionAttributionRules(GetSessionAttributionRulesRequest.getDefaultInstance())
            .getRulesList());
  }

  private SessionAttributionRule withRank(SessionAttributionRule rule, int rank) {
    return rule.toBuilder().setRank(rank).build();
  }
}
