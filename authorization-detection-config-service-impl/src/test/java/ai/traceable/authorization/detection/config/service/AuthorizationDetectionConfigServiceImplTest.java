package ai.traceable.authorization.detection.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.authorization.detection.config.service.v1.AuthorizationDetectionConfigServiceGrpc;
import ai.traceable.authorization.detection.config.service.v1.AuthorizationDetectionConfigServiceGrpc.AuthorizationDetectionConfigServiceBlockingStub;
import ai.traceable.authorization.detection.config.service.v1.AuthorizationDetectionRule;
import ai.traceable.authorization.detection.config.service.v1.AuthorizationDetectionRuleFilter;
import ai.traceable.authorization.detection.config.service.v1.AuthorizationDetectionRuleScope;
import ai.traceable.authorization.detection.config.service.v1.CreateAuthorizationDetectionRuleRequest;
import ai.traceable.authorization.detection.config.service.v1.DeleteAuthorizationDetectionRuleRequest;
import ai.traceable.authorization.detection.config.service.v1.GetAuthorizationDetectionRulesRequest;
import ai.traceable.authorization.detection.config.service.v1.Predicate;
import ai.traceable.authorization.detection.config.service.v1.Predicate.KeyValuePredicate;
import ai.traceable.authorization.detection.config.service.v1.Predicate.StringPredicate;
import ai.traceable.authorization.detection.config.service.v1.Predicate.StringPredicate.RelationalOperator;
import ai.traceable.authorization.detection.config.service.v1.UpdateAuthorizationDetectionRuleRequest;
import ai.traceable.config.utils.UuidGenerator;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AuthorizationDetectionConfigServiceImplTest {
  MockGenericConfigService mockGenericConfigService;
  @Mock ConfigChangeEventGenerator mockConfigChangeEventGenerator;
  @Mock AuthorizationDetectionConfigRequestValidator mockValidator;
  @Mock UuidGenerator mockUuidGenerator;

  AuthorizationDetectionConfigServiceBlockingStub stub;

  static final AuthorizationDetectionRule SCOPED_AUTH_RULE =
      AuthorizationDetectionRule.newBuilder()
          .setId("scoped-rule")
          .setAuthorizationType("auth-type")
          .setPredicate(
              Predicate.newBuilder()
                  .setHeaderPredicate(
                      KeyValuePredicate.newBuilder()
                          .setKeyPredicate(
                              StringPredicate.newBuilder()
                                  .setOperator(RelationalOperator.RELATIONAL_OPERATOR_MATCHES_REGEX)
                                  .setValue("(?i)authorization"))))
          .setScope(AuthorizationDetectionRuleScope.newBuilder().addEnvironmentNames("env"))
          .build();

  static final AuthorizationDetectionRule UNSCOPED_AUTH_RULE =
      AuthorizationDetectionRule.newBuilder()
          .setId("unscoped-rule")
          .setAuthorizationType("other-auth-type")
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
            new AuthorizationDetectionConfigServiceImpl(
                mockValidator,
                new AuthorizationDetectionRuleStore(
                    ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel()),
                    mockConfigChangeEventGenerator),
                new AuthorizationDetectionConfigRuleBuilder(mockUuidGenerator)))
        .start();
    stub =
        AuthorizationDetectionConfigServiceGrpc.newBlockingStub(
            this.mockGenericConfigService.channel());
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void createReadUpdateDeleteTest() {
    CreateAuthorizationDetectionRuleRequest unscopedCreateRequest =
        CreateAuthorizationDetectionRuleRequest.newBuilder()
            .setAuthorizationType(UNSCOPED_AUTH_RULE.getAuthorizationType())
            .setPredicate(UNSCOPED_AUTH_RULE.getPredicate())
            .build();

    CreateAuthorizationDetectionRuleRequest scopedCreateRequest =
        CreateAuthorizationDetectionRuleRequest.newBuilder()
            .setAuthorizationType(SCOPED_AUTH_RULE.getAuthorizationType())
            .setPredicate(SCOPED_AUTH_RULE.getPredicate())
            .setScope(SCOPED_AUTH_RULE.getScope())
            .build();
    when(mockUuidGenerator.generateRandomId())
        .thenReturn(UNSCOPED_AUTH_RULE.getId(), SCOPED_AUTH_RULE.getId());

    assertEquals(
        UNSCOPED_AUTH_RULE,
        this.stub.createAuthorizationDetectionRule(unscopedCreateRequest).getRule());
    assertEquals(
        SCOPED_AUTH_RULE,
        this.stub.createAuthorizationDetectionRule(scopedCreateRequest).getRule());

    assertEquals(
        List.of(SCOPED_AUTH_RULE, UNSCOPED_AUTH_RULE),
        this.stub
            .getAuthorizationDetectionRules(
                GetAuthorizationDetectionRulesRequest.getDefaultInstance())
            .getRulesList());
    assertEquals(
        List.of(SCOPED_AUTH_RULE, UNSCOPED_AUTH_RULE),
        this.stub
            .getAuthorizationDetectionRules(
                GetAuthorizationDetectionRulesRequest.newBuilder()
                    .setFilter(
                        AuthorizationDetectionRuleFilter.newBuilder()
                            .setScope(
                                AuthorizationDetectionRuleScope.newBuilder()
                                    .addAllEnvironmentNames(
                                        SCOPED_AUTH_RULE.getScope().getEnvironmentNamesList())))
                    .build())
            .getRulesList());
    assertEquals(
        List.of(UNSCOPED_AUTH_RULE),
        this.stub
            .getAuthorizationDetectionRules(
                GetAuthorizationDetectionRulesRequest.newBuilder()
                    .setFilter(
                        AuthorizationDetectionRuleFilter.newBuilder()
                            .setScope(AuthorizationDetectionRuleScope.getDefaultInstance()))
                    .build())
            .getRulesList());

    AuthorizationDetectionRule expectedUpdatedRule =
        UNSCOPED_AUTH_RULE.toBuilder()
            .setScope(
                AuthorizationDetectionRuleScope.newBuilder()
                    .addAllEnvironmentNames(SCOPED_AUTH_RULE.getScope().getEnvironmentNamesList())
                    .addEnvironmentNames("other-env"))
            .build();

    UpdateAuthorizationDetectionRuleRequest ruleUpdateRequest =
        UpdateAuthorizationDetectionRuleRequest.newBuilder()
            .setId(expectedUpdatedRule.getId())
            .setAuthorizationType(expectedUpdatedRule.getAuthorizationType())
            .setPredicate(expectedUpdatedRule.getPredicate())
            .setScope(expectedUpdatedRule.getScope())
            .build();

    assertEquals(
        expectedUpdatedRule,
        this.stub.updateAuthorizationDetectionRule(ruleUpdateRequest).getRule());

    assertEquals(
        List.of(SCOPED_AUTH_RULE, expectedUpdatedRule),
        this.stub
            .getAuthorizationDetectionRules(
                GetAuthorizationDetectionRulesRequest.newBuilder()
                    .setFilter(
                        AuthorizationDetectionRuleFilter.newBuilder()
                            .setScope(
                                AuthorizationDetectionRuleScope.newBuilder()
                                    .addAllEnvironmentNames(
                                        SCOPED_AUTH_RULE.getScope().getEnvironmentNamesList())))
                    .build())
            .getRulesList());

    assertEquals(
        List.of(expectedUpdatedRule),
        this.stub
            .getAuthorizationDetectionRules(
                GetAuthorizationDetectionRulesRequest.newBuilder()
                    .setFilter(
                        AuthorizationDetectionRuleFilter.newBuilder()
                            .setScope(
                                AuthorizationDetectionRuleScope.newBuilder()
                                    .addEnvironmentNames("other-env")))
                    .build())
            .getRulesList());

    this.stub.deleteAuthorizationDetectionRule(
        DeleteAuthorizationDetectionRuleRequest.newBuilder()
            .setId(SCOPED_AUTH_RULE.getId())
            .build());

    assertEquals(
        List.of(expectedUpdatedRule),
        this.stub
            .getAuthorizationDetectionRules(
                GetAuthorizationDetectionRulesRequest.newBuilder()
                    .setFilter(
                        AuthorizationDetectionRuleFilter.newBuilder()
                            .setScope(
                                AuthorizationDetectionRuleScope.newBuilder()
                                    .addAllEnvironmentNames(
                                        SCOPED_AUTH_RULE.getScope().getEnvironmentNamesList())))
                    .build())
            .getRulesList());

    this.stub.deleteAuthorizationDetectionRule(
        DeleteAuthorizationDetectionRuleRequest.newBuilder()
            .setId(expectedUpdatedRule.getId())
            .build());
    assertEquals(
        List.of(),
        this.stub
            .getAuthorizationDetectionRules(
                GetAuthorizationDetectionRulesRequest.getDefaultInstance())
            .getRulesList());
  }
}
