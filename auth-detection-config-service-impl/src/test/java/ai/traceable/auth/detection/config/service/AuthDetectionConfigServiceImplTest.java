package ai.traceable.auth.detection.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.auth.detection.config.service.v1.AuthDetectionConfigServiceGrpc;
import ai.traceable.auth.detection.config.service.v1.AuthDetectionConfigServiceGrpc.AuthDetectionConfigServiceBlockingStub;
import ai.traceable.auth.detection.config.service.v1.AuthDetectionRule;
import ai.traceable.auth.detection.config.service.v1.AuthDetectionRuleFilter;
import ai.traceable.auth.detection.config.service.v1.AuthDetectionRuleScope;
import ai.traceable.auth.detection.config.service.v1.CreateAuthDetectionRuleRequest;
import ai.traceable.auth.detection.config.service.v1.DeleteAuthDetectionRuleRequest;
import ai.traceable.auth.detection.config.service.v1.GetAuthDetectionRulesRequest;
import ai.traceable.auth.detection.config.service.v1.Predicate;
import ai.traceable.auth.detection.config.service.v1.Predicate.KeyValuePredicate;
import ai.traceable.auth.detection.config.service.v1.Predicate.StringPredicate;
import ai.traceable.auth.detection.config.service.v1.Predicate.StringPredicate.RelationalOperator;
import ai.traceable.auth.detection.config.service.v1.UpdateAuthDetectionRuleRequest;
import ai.traceable.config.utils.UuidGenerator;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AuthDetectionConfigServiceImplTest {
  MockGenericConfigService mockGenericConfigService;
  @Mock ConfigChangeEventGenerator mockConfigChangeEventGenerator;
  @Mock AuthDetectionConfigRequestValidator mockValidator;
  @Mock UuidGenerator mockUuidGenerator;

  AuthDetectionConfigServiceBlockingStub stub;

  static final AuthDetectionRule SCOPED_AUTH_RULE =
      AuthDetectionRule.newBuilder()
          .setId("scoped-rule")
          .setAuthType("auth-type")
          .setPredicate(
              Predicate.newBuilder()
                  .setHeaderPredicate(
                      KeyValuePredicate.newBuilder()
                          .setKeyPredicate(
                              StringPredicate.newBuilder()
                                  .setOperator(RelationalOperator.RELATIONAL_OPERATOR_MATCHES_REGEX)
                                  .setValue("(?i)authorization"))))
          .setScope(AuthDetectionRuleScope.newBuilder().addEnvironmentNames("env"))
          .build();

  static final AuthDetectionRule UNSCOPED_AUTH_RULE =
      AuthDetectionRule.newBuilder()
          .setId("unscoped-rule")
          .setAuthType("other-auth-type")
          .setPredicate(
              Predicate.newBuilder()
                  .setHeaderPredicate(
                      KeyValuePredicate.newBuilder()
                          .setKeyPredicate(
                              StringPredicate.newBuilder()
                                  .setOperator(RelationalOperator.RELATIONAL_OPERATOR_MATCHES_REGEX)
                                  .setValue("(?i).*api-?_?key.*"))))
          .build();

  @BeforeEach
  void setUp() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGetAll().mockDelete();
    mockGenericConfigService
        .addService(
            new AuthDetectionConfigServiceImpl(
                mockValidator,
                new AuthDetectionRuleStore(
                    ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel()),
                    mockConfigChangeEventGenerator),
                new AuthDetectionConfigRuleBuilder(mockUuidGenerator)))
        .start();
    stub = AuthDetectionConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void createReadUpdateDeleteTest() {
    CreateAuthDetectionRuleRequest unscopedCreateRequest =
        CreateAuthDetectionRuleRequest.newBuilder()
            .setAuthType(UNSCOPED_AUTH_RULE.getAuthType())
            .setPredicate(UNSCOPED_AUTH_RULE.getPredicate())
            .build();

    CreateAuthDetectionRuleRequest scopedCreateRequest =
        CreateAuthDetectionRuleRequest.newBuilder()
            .setAuthType(SCOPED_AUTH_RULE.getAuthType())
            .setPredicate(SCOPED_AUTH_RULE.getPredicate())
            .setScope(SCOPED_AUTH_RULE.getScope())
            .build();
    when(mockUuidGenerator.generateRandomId())
        .thenReturn(UNSCOPED_AUTH_RULE.getId(), SCOPED_AUTH_RULE.getId());

    assertEquals(
        UNSCOPED_AUTH_RULE, this.stub.createAuthDetectionRule(unscopedCreateRequest).getRule());
    assertEquals(
        SCOPED_AUTH_RULE, this.stub.createAuthDetectionRule(scopedCreateRequest).getRule());

    Assertions.assertEquals(
        List.of(SCOPED_AUTH_RULE, UNSCOPED_AUTH_RULE),
        this.stub
            .getAuthDetectionRules(GetAuthDetectionRulesRequest.getDefaultInstance())
            .getRulesList());
    Assertions.assertEquals(
        List.of(SCOPED_AUTH_RULE, UNSCOPED_AUTH_RULE),
        this.stub
            .getAuthDetectionRules(
                GetAuthDetectionRulesRequest.newBuilder()
                    .setFilter(
                        AuthDetectionRuleFilter.newBuilder()
                            .setScope(
                                AuthDetectionRuleScope.newBuilder()
                                    .addAllEnvironmentNames(
                                        SCOPED_AUTH_RULE.getScope().getEnvironmentNamesList())))
                    .build())
            .getRulesList());
    Assertions.assertEquals(
        List.of(UNSCOPED_AUTH_RULE),
        this.stub
            .getAuthDetectionRules(
                GetAuthDetectionRulesRequest.newBuilder()
                    .setFilter(
                        AuthDetectionRuleFilter.newBuilder()
                            .setScope(AuthDetectionRuleScope.getDefaultInstance()))
                    .build())
            .getRulesList());

    AuthDetectionRule expectedUpdatedRule =
        UNSCOPED_AUTH_RULE.toBuilder()
            .setScope(
                AuthDetectionRuleScope.newBuilder()
                    .addAllEnvironmentNames(SCOPED_AUTH_RULE.getScope().getEnvironmentNamesList())
                    .addEnvironmentNames("other-env"))
            .build();

    UpdateAuthDetectionRuleRequest ruleUpdateRequest =
        UpdateAuthDetectionRuleRequest.newBuilder()
            .setId(expectedUpdatedRule.getId())
            .setAuthType(expectedUpdatedRule.getAuthType())
            .setPredicate(expectedUpdatedRule.getPredicate())
            .setScope(expectedUpdatedRule.getScope())
            .build();

    assertEquals(
        expectedUpdatedRule, this.stub.updateAuthDetectionRule(ruleUpdateRequest).getRule());

    Assertions.assertEquals(
        List.of(SCOPED_AUTH_RULE, expectedUpdatedRule),
        this.stub
            .getAuthDetectionRules(
                GetAuthDetectionRulesRequest.newBuilder()
                    .setFilter(
                        AuthDetectionRuleFilter.newBuilder()
                            .setScope(
                                AuthDetectionRuleScope.newBuilder()
                                    .addAllEnvironmentNames(
                                        SCOPED_AUTH_RULE.getScope().getEnvironmentNamesList())))
                    .build())
            .getRulesList());

    Assertions.assertEquals(
        List.of(expectedUpdatedRule),
        this.stub
            .getAuthDetectionRules(
                GetAuthDetectionRulesRequest.newBuilder()
                    .setFilter(
                        AuthDetectionRuleFilter.newBuilder()
                            .setScope(
                                AuthDetectionRuleScope.newBuilder()
                                    .addEnvironmentNames("other-env")))
                    .build())
            .getRulesList());

    this.stub.deleteAuthDetectionRule(
        DeleteAuthDetectionRuleRequest.newBuilder().setId(SCOPED_AUTH_RULE.getId()).build());

    Assertions.assertEquals(
        List.of(expectedUpdatedRule),
        this.stub
            .getAuthDetectionRules(
                GetAuthDetectionRulesRequest.newBuilder()
                    .setFilter(
                        AuthDetectionRuleFilter.newBuilder()
                            .setScope(
                                AuthDetectionRuleScope.newBuilder()
                                    .addAllEnvironmentNames(
                                        SCOPED_AUTH_RULE.getScope().getEnvironmentNamesList())))
                    .build())
            .getRulesList());

    this.stub.deleteAuthDetectionRule(
        DeleteAuthDetectionRuleRequest.newBuilder().setId(expectedUpdatedRule.getId()).build());
    Assertions.assertEquals(
        List.of(),
        this.stub
            .getAuthDetectionRules(GetAuthDetectionRulesRequest.getDefaultInstance())
            .getRulesList());
  }
}
