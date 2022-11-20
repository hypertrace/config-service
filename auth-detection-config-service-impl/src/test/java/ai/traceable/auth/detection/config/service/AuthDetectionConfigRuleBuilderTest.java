package ai.traceable.auth.detection.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.auth.detection.config.service.v1.AuthDetectionRule;
import ai.traceable.auth.detection.config.service.v1.AuthDetectionRuleScope;
import ai.traceable.auth.detection.config.service.v1.CreateAuthDetectionRuleRequest;
import ai.traceable.auth.detection.config.service.v1.Predicate;
import ai.traceable.auth.detection.config.service.v1.Predicate.KeyValuePredicate;
import ai.traceable.auth.detection.config.service.v1.Predicate.StringPredicate;
import ai.traceable.auth.detection.config.service.v1.Predicate.StringPredicate.RelationalOperator;
import ai.traceable.auth.detection.config.service.v1.UpdateAuthDetectionRuleRequest;
import ai.traceable.config.utils.UuidGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AuthDetectionConfigRuleBuilderTest {

  @Mock UuidGenerator mockUuidGenerator;
  @InjectMocks AuthDetectionConfigRuleBuilder ruleBuilder;

  @Test
  void buildsCreateRules() {
    when(mockUuidGenerator.generateRandomId()).thenReturn("created-id");
    CreateAuthDetectionRuleRequest unscopedRequest =
        CreateAuthDetectionRuleRequest.newBuilder()
            .setAuthType("auth-type")
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

    AuthDetectionRule expectedUnscopedRule =
        AuthDetectionRule.newBuilder()
            .setId("created-id")
            .setAuthType("auth-type")
            .setPredicate(unscopedRequest.getPredicate())
            .build();

    assertEquals(expectedUnscopedRule, ruleBuilder.build(unscopedRequest));

    AuthDetectionRuleScope scope =
        AuthDetectionRuleScope.newBuilder().addEnvironmentNames("test-env").build();

    assertEquals(
        expectedUnscopedRule.toBuilder().setScope(scope).build(),
        ruleBuilder.build(unscopedRequest.toBuilder().setScope(scope).build()));
  }

  @Test
  void buildsUpdateRule() {
    UpdateAuthDetectionRuleRequest unscopedRequest =
        UpdateAuthDetectionRuleRequest.newBuilder()
            .setId("update-id")
            .setAuthType("auth-type")
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

    AuthDetectionRule expectedUnscopedRule =
        AuthDetectionRule.newBuilder()
            .setId("update-id")
            .setAuthType("auth-type")
            .setPredicate(unscopedRequest.getPredicate())
            .build();

    assertEquals(expectedUnscopedRule, ruleBuilder.build(unscopedRequest));

    AuthDetectionRuleScope scope =
        AuthDetectionRuleScope.newBuilder().addEnvironmentNames("test-env").build();

    assertEquals(
        expectedUnscopedRule.toBuilder().setScope(scope).build(),
        ruleBuilder.build(unscopedRequest.toBuilder().setScope(scope).build()));
  }
}
