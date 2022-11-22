package ai.traceable.auth.detection.config.service;

import static ai.traceable.auth.detection.config.service.v1.Predicate.StringPredicate.RelationalOperator.RELATIONAL_OPERATOR_MATCHES_REGEX;
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
import com.typesafe.config.ConfigFactory;
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

  AuthDetectionRule DEFAULT_BEARER_RULE =
      AuthDetectionRule.newBuilder()
          .setId("f496d554-4685-4de4-a074-4186ee579a4c")
          .setAuthType("Bearer Token")
          .setDefault(true)
          .setPredicate(
              Predicate.newBuilder()
                  .setHeaderPredicate(
                      KeyValuePredicate.newBuilder()
                          .setKeyPredicate(
                              StringPredicate.newBuilder()
                                  .setOperator(RELATIONAL_OPERATOR_MATCHES_REGEX)
                                  .setValue("(?i)^authorization$"))
                          .setValuePredicate(
                              StringPredicate.newBuilder()
                                  .setOperator(RELATIONAL_OPERATOR_MATCHES_REGEX)
                                  .setValue("^Bearer\\s.*"))))
          .build();
  AuthDetectionRule DEFAULT_BASIC_RULE =
      AuthDetectionRule.newBuilder()
          .setId("c96cdabf-4311-49f7-aedc-0568aeb0c2af")
          .setAuthType("Basic")
          .setDefault(true)
          .setPredicate(
              Predicate.newBuilder()
                  .setHeaderPredicate(
                      KeyValuePredicate.newBuilder()
                          .setKeyPredicate(
                              StringPredicate.newBuilder()
                                  .setOperator(RELATIONAL_OPERATOR_MATCHES_REGEX)
                                  .setValue("(?i)^authorization$"))
                          .setValuePredicate(
                              StringPredicate.newBuilder()
                                  .setOperator(RELATIONAL_OPERATOR_MATCHES_REGEX)
                                  .setValue("^Basic\\s.*"))))
          .build();

  @BeforeEach
  void setUp() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockGenericConfigService
        .addService(
            new AuthDetectionConfigServiceImpl(
                mockValidator,
                new AuthDetectionRuleManager(
                    new DefaultAuthRuleConfig(ConfigFactory.parseResources("default-rules.conf")),
                    new UserDefinedAuthDetectionRuleStore(
                        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel()),
                        mockConfigChangeEventGenerator),
                    new DeletedDefaultAuthDetectionRuleStore(
                        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel()),
                        mockConfigChangeEventGenerator)),
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

    assertEquals(
        List.of(DEFAULT_BEARER_RULE, DEFAULT_BASIC_RULE, SCOPED_AUTH_RULE, UNSCOPED_AUTH_RULE),
        this.stub
            .getAuthDetectionRules(GetAuthDetectionRulesRequest.getDefaultInstance())
            .getRulesList());
    assertEquals(
        List.of(DEFAULT_BEARER_RULE, DEFAULT_BASIC_RULE, SCOPED_AUTH_RULE, UNSCOPED_AUTH_RULE),
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
    assertEquals(
        List.of(DEFAULT_BEARER_RULE, DEFAULT_BASIC_RULE, UNSCOPED_AUTH_RULE),
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

    assertEquals(
        List.of(DEFAULT_BEARER_RULE, DEFAULT_BASIC_RULE, SCOPED_AUTH_RULE, expectedUpdatedRule),
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

    assertEquals(
        List.of(DEFAULT_BEARER_RULE, DEFAULT_BASIC_RULE, expectedUpdatedRule),
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

    assertEquals(
        List.of(DEFAULT_BEARER_RULE, DEFAULT_BASIC_RULE, expectedUpdatedRule),
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

    AuthDetectionRule modifiedBearerRule =
        DEFAULT_BEARER_RULE.toBuilder()
            .setScope(AuthDetectionRuleScope.newBuilder().addEnvironmentNames("other-env"))
            .setDefault(false)
            .build();
    assertEquals(
        modifiedBearerRule,
        this.stub
            .updateAuthDetectionRule(
                UpdateAuthDetectionRuleRequest.newBuilder()
                    .setId(DEFAULT_BEARER_RULE.getId())
                    .setAuthType(DEFAULT_BEARER_RULE.getAuthType())
                    .setPredicate(DEFAULT_BEARER_RULE.getPredicate())
                    .setScope(modifiedBearerRule.getScope())
                    .build())
            .getRule());

    assertEquals(
        List.of(DEFAULT_BASIC_RULE, modifiedBearerRule),
        this.stub
            .getAuthDetectionRules(GetAuthDetectionRulesRequest.getDefaultInstance())
            .getRulesList());

    this.stub.deleteAuthDetectionRule(
        DeleteAuthDetectionRuleRequest.newBuilder().setId(modifiedBearerRule.getId()).build());

    assertEquals(
        List.of(DEFAULT_BASIC_RULE),
        this.stub
            .getAuthDetectionRules(GetAuthDetectionRulesRequest.getDefaultInstance())
            .getRulesList());

    this.stub.deleteAuthDetectionRule(
        DeleteAuthDetectionRuleRequest.newBuilder().setId(DEFAULT_BASIC_RULE.getId()).build());

    assertEquals(
        List.of(),
        this.stub
            .getAuthDetectionRules(GetAuthDetectionRulesRequest.getDefaultInstance())
            .getRulesList());
  }
}
