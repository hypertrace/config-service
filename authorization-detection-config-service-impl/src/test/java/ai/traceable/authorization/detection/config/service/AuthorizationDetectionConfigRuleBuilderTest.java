package ai.traceable.authorization.detection.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.authorization.detection.config.service.v1.AuthorizationDetectionRule;
import ai.traceable.authorization.detection.config.service.v1.AuthorizationDetectionRuleScope;
import ai.traceable.authorization.detection.config.service.v1.CreateAuthorizationDetectionRuleRequest;
import ai.traceable.authorization.detection.config.service.v1.Predicate;
import ai.traceable.authorization.detection.config.service.v1.Predicate.KeyValuePredicate;
import ai.traceable.authorization.detection.config.service.v1.Predicate.StringPredicate;
import ai.traceable.authorization.detection.config.service.v1.Predicate.StringPredicate.RelationalOperator;
import ai.traceable.authorization.detection.config.service.v1.UpdateAuthorizationDetectionRuleRequest;
import ai.traceable.config.utils.UuidGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AuthorizationDetectionConfigRuleBuilderTest {

  @Mock UuidGenerator mockUuidGenerator;
  @InjectMocks AuthorizationDetectionConfigRuleBuilder ruleBuilder;

  @Test
  void buildsCreateRules() {
    when(mockUuidGenerator.generateRandomId()).thenReturn("created-id");
    CreateAuthorizationDetectionRuleRequest unscopedRequest =
        CreateAuthorizationDetectionRuleRequest.newBuilder()
            .setAuthorizationType("auth-type")
            .setPredicate(
                Predicate.newBuilder()
                    .setHeaderPredicate(
                        KeyValuePredicate.newBuilder()
                            .setKeyPredicate(
                                StringPredicate.newBuilder()
                                    .setOperator(
                                        RelationalOperator.RELATIONAL_OPERATOR_MATCHES_REGEX)
                                    .setValue("(?i)authorization"))))
            .build();

    AuthorizationDetectionRule expectedUnscopedRule =
        AuthorizationDetectionRule.newBuilder()
            .setId("created-id")
            .setAuthorizationType("auth-type")
            .setPredicate(unscopedRequest.getPredicate())
            .build();

    assertEquals(expectedUnscopedRule, ruleBuilder.build(unscopedRequest));

    AuthorizationDetectionRuleScope scope =
        AuthorizationDetectionRuleScope.newBuilder().addEnvironmentNames("test-env").build();

    assertEquals(
        expectedUnscopedRule.toBuilder().setScope(scope).build(),
        ruleBuilder.build(unscopedRequest.toBuilder().setScope(scope).build()));
  }

  @Test
  void buildsUpdateRule() {
    UpdateAuthorizationDetectionRuleRequest unscopedRequest =
        UpdateAuthorizationDetectionRuleRequest.newBuilder()
            .setId("update-id")
            .setAuthorizationType("auth-type")
            .setPredicate(
                Predicate.newBuilder()
                    .setHeaderPredicate(
                        KeyValuePredicate.newBuilder()
                            .setKeyPredicate(
                                StringPredicate.newBuilder()
                                    .setOperator(
                                        RelationalOperator.RELATIONAL_OPERATOR_MATCHES_REGEX)
                                    .setValue("(?i)authorization"))))
            .build();

    AuthorizationDetectionRule expectedUnscopedRule =
        AuthorizationDetectionRule.newBuilder()
            .setId("update-id")
            .setAuthorizationType("auth-type")
            .setPredicate(unscopedRequest.getPredicate())
            .build();

    assertEquals(expectedUnscopedRule, ruleBuilder.build(unscopedRequest));

    AuthorizationDetectionRuleScope scope =
        AuthorizationDetectionRuleScope.newBuilder().addEnvironmentNames("test-env").build();

    assertEquals(
        expectedUnscopedRule.toBuilder().setScope(scope).build(),
        ruleBuilder.build(unscopedRequest.toBuilder().setScope(scope).build()));
  }
}
