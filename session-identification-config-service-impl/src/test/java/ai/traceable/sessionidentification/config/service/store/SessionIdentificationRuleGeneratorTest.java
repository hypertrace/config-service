package ai.traceable.sessionidentification.config.service.store;

import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.sessionidentification.config.service.v1.AttributeProjection;
import ai.traceable.sessionidentification.config.service.v1.CreateSessionIdentificationRuleRequest;
import ai.traceable.sessionidentification.config.service.v1.LiteralValue;
import ai.traceable.sessionidentification.config.service.v1.MatchCondition;
import ai.traceable.sessionidentification.config.service.v1.MatchOperator;
import ai.traceable.sessionidentification.config.service.v1.ProjectionRoot;
import ai.traceable.sessionidentification.config.service.v1.RequestAttributeKeyLocation;
import ai.traceable.sessionidentification.config.service.v1.RequestSessionTokenDetails;
import ai.traceable.sessionidentification.config.service.v1.RuleCreationSource;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRuleStatus;
import ai.traceable.sessionidentification.config.service.v1.SessionTokenRule;
import ai.traceable.sessionidentification.config.service.v1.SessionTokenValueRule;
import ai.traceable.sessionidentification.config.service.v1.UpdateSessionIdentificationRuleRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SessionIdentificationRuleGeneratorTest {
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
  @Mock private UuidGenerator uuidGenerator;
  private SessionIdentificationRuleGenerator sessionIdentificationRuleGenerator;

  @Test
  void test_generateNewRuleFromCreateRequest() {
    when(this.uuidGenerator.generateRandomId()).thenReturn("id");
    sessionIdentificationRuleGenerator = new SessionIdentificationRuleGenerator(uuidGenerator);
    CreateSessionIdentificationRuleRequest request =
        CreateSessionIdentificationRuleRequest.newBuilder()
            .setName("session-identification-rule")
            .setDescription("desc")
            .addTokenRules(RULE)
            .build();
    Assertions.assertEquals(
        SessionIdentificationRule.newBuilder()
            .setName("session-identification-rule")
            .setDescription("desc")
            .setId("id")
            .addTokenRules(RULE)
            .build(),
        sessionIdentificationRuleGenerator.generateNewRuleFromCreateRequest(request));
  }

  @Test
  void test_generateRuleFromUpdateRequest() {
    sessionIdentificationRuleGenerator = new SessionIdentificationRuleGenerator(uuidGenerator);
    UpdateSessionIdentificationRuleRequest request =
        UpdateSessionIdentificationRuleRequest.newBuilder()
            .setName("session-identification-rule")
            .setId("id")
            .setDescription("desc")
            .addTokenRules(RULE)
            .build();
    Assertions.assertEquals(
        SessionIdentificationRule.newBuilder()
            .setName("session-identification-rule")
            .setDescription("desc")
            .setId("id")
            .addTokenRules(RULE)
            .build(),
        sessionIdentificationRuleGenerator.generateRuleFromUpdateRequest(request));
  }

  @Test
  void test_generateRuleForOldApiConfigFromUpdateRequest() {
    sessionIdentificationRuleGenerator = new SessionIdentificationRuleGenerator(uuidGenerator);
    UpdateSessionIdentificationRuleRequest request =
        UpdateSessionIdentificationRuleRequest.newBuilder()
            .setName("session-identification-rule")
            .setId("id")
            .setDescription("desc")
            .addTokenRules(RULE)
            .build();
    Assertions.assertEquals(
        SessionIdentificationRule.newBuilder()
            .setName("session-identification-rule")
            .setDescription("desc")
            .setId("id")
            .setStatus(
                SessionIdentificationRuleStatus.newBuilder()
                    .setRuleCreationSource(RuleCreationSource.RULE_CREATION_SOURCE_OLD_API))
            .addTokenRules(RULE)
            .build(),
        sessionIdentificationRuleGenerator.generateRuleForOldApiConfigFromUpdateRequest(request));
  }
}
