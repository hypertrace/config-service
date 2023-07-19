package ai.traceable.sessionidentification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.sessionidentification.config.service.migration.LegacySessionIdentificationRuleTranslatingDaoImpl;
import ai.traceable.sessionidentification.config.service.store.SessionIdentificationRuleGenerator;
import ai.traceable.sessionidentification.config.service.store.SessionIdentificationRuleStore;
import ai.traceable.sessionidentification.config.service.v1.AttributeProjection;
import ai.traceable.sessionidentification.config.service.v1.CreateSessionIdentificationRuleRequest;
import ai.traceable.sessionidentification.config.service.v1.DeleteSessionIdentificationRuleRequest;
import ai.traceable.sessionidentification.config.service.v1.GetSessionIdentificationRulesRequest;
import ai.traceable.sessionidentification.config.service.v1.LiteralValue;
import ai.traceable.sessionidentification.config.service.v1.MatchCondition;
import ai.traceable.sessionidentification.config.service.v1.MatchOperator;
import ai.traceable.sessionidentification.config.service.v1.ProjectionRoot;
import ai.traceable.sessionidentification.config.service.v1.RequestAttributeKeyLocation;
import ai.traceable.sessionidentification.config.service.v1.RequestSessionTokenDetails;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationConfigServiceGrpc;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRuleStatus;
import ai.traceable.sessionidentification.config.service.v1.SessionTokenRule;
import ai.traceable.sessionidentification.config.service.v1.SessionTokenValueRule;
import ai.traceable.sessionidentification.config.service.v1.UpdateSessionIdentificationRuleRequest;
import ai.traceable.sessionidentification.config.service.validation.SessionIdentificationConfigRequestValidator;
import ai.traceable.sessionidentification.config.service.validation.SessionTokenRuleValidator;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SessionIdentificationConfigServiceImplTest {
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
                      ProjectionRoot.newBuilder()
                          .setAttributeProjection(
                              AttributeProjection.newBuilder()
                                  .setAttributeKeyMatchCondition(
                                      MatchCondition.newBuilder()
                                          .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                          .setMatchValue(
                                              LiteralValue.newBuilder()
                                                  .setStringValue("authorization")))))
                  .build())
          .build();
  SessionIdentificationConfigServiceGrpc.SessionIdentificationConfigServiceBlockingStub
      sessionIdentificationConfigServiceBlockingStub;
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

    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    this.mockGenericConfigService
        .addService(
            new SessionIdentificationConfigServiceImpl(
                new SessionIdentificationConfigRequestValidator(new SessionTokenRuleValidator()),
                new SessionIdentificationRuleStore(genericStub, configChangeEventGenerator),
                new SessionIdentificationRuleGenerator(new UuidGenerator()),
                mock(LegacySessionIdentificationRuleTranslatingDaoImpl.class)))
        .start();

    this.sessionIdentificationConfigServiceBlockingStub =
        SessionIdentificationConfigServiceGrpc.newBlockingStub(
            this.mockGenericConfigService.channel());
  }

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  @Test
  void lotsOfCrud() {
    SessionIdentificationRule firstCreated =
        this.sessionIdentificationConfigServiceBlockingStub
            .createSessionIdentificationRule(
                CreateSessionIdentificationRuleRequest.newBuilder()
                    .setName("first")
                    .addTokenRules(RULE)
                    .build())
            .getRule();

    SessionIdentificationRule firstUpdated =
        this.sessionIdentificationConfigServiceBlockingStub
            .updateSessionIdentificationRule(
                UpdateSessionIdentificationRuleRequest.newBuilder()
                    .setName("first updated")
                    .setId(firstCreated.getId())
                    .setStatus(SessionIdentificationRuleStatus.newBuilder().setDisabled(true))
                    .addTokenRules(RULE)
                    .build())
            .getRule();

    assertTrue(firstUpdated.getStatus().getDisabled());

    SessionIdentificationRule secondCreated =
        this.sessionIdentificationConfigServiceBlockingStub
            .createSessionIdentificationRule(
                CreateSessionIdentificationRuleRequest.newBuilder()
                    .setName("second")
                    .addTokenRules(RULE)
                    .build())
            .getRule();
    assertEquals("second", secondCreated.getName());

    assertEquals(
        List.of(secondCreated, firstUpdated),
        this.sessionIdentificationConfigServiceBlockingStub
            .getSessionIdentificationRules(
                GetSessionIdentificationRulesRequest.getDefaultInstance())
            .getRulesList());

    this.sessionIdentificationConfigServiceBlockingStub.deleteSessionIdentificationRule(
        DeleteSessionIdentificationRuleRequest.newBuilder().setId(firstUpdated.getId()).build());
    assertEquals(
        List.of(secondCreated),
        this.sessionIdentificationConfigServiceBlockingStub
            .getSessionIdentificationRules(
                GetSessionIdentificationRulesRequest.getDefaultInstance())
            .getRulesList());
  }
}
