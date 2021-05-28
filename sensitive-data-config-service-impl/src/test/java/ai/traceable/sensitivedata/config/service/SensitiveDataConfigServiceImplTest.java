package ai.traceable.sensitivedata.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.sensitivedata.config.service.v1.CreateRedactionRuleRequest;
import ai.traceable.sensitivedata.config.service.v1.DeleteRedactionRuleRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAutomaticSecretRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyForTypeRequest;
import ai.traceable.sensitivedata.config.service.v1.MatchType;
import ai.traceable.sensitivedata.config.service.v1.NewRedactionRule;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.UpdateAutomaticSecretRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionRuleRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionStrategyForTypeRequest;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SensitiveDataConfigServiceImplTest {
  SensitiveDataConfigServiceBlockingStub sensitiveDataStub;
  MockGenericConfigService mockGenericConfigService;

  @BeforeEach
  void beforeEach() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();

    SensitiveDataServiceConfig mockConfig = mock(SensitiveDataServiceConfig.class);
    when(mockConfig.defaultAutomaticRedactionStrategy()).thenReturn(true);
    when(mockConfig.defaultParamTypeRedactionStrategy())
        .thenReturn(RedactionStrategy.REDACTION_STRATEGY_RAW);

    mockGenericConfigService
        .addService(
            new SensitiveDataConfigServiceImpl(
                new ConfigServiceCoordinatorImpl(
                    ConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel()),
                    mockConfig)))
        .start();

    sensitiveDataStub =
        SensitiveDataConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void upsertAndGetRedactionStrategyForType() {
    RedactionStrategy redactionStrategy =
        sensitiveDataStub
            .getRedactionStrategyForType(
                GetRedactionStrategyForTypeRequest.newBuilder()
                    .setParamType(ParamType.PARAM_TYPE_HEADER)
                    .build())
            .getRedactionStrategy();
    assertEquals(RedactionStrategy.REDACTION_STRATEGY_RAW, redactionStrategy);

    sensitiveDataStub.updateRedactionStrategyForType(
        UpdateRedactionStrategyForTypeRequest.newBuilder()
            .setParamType(ParamType.PARAM_TYPE_HEADER)
            .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_REDACT)
            .build());
    redactionStrategy =
        sensitiveDataStub
            .getRedactionStrategyForType(
                GetRedactionStrategyForTypeRequest.newBuilder()
                    .setParamType(ParamType.PARAM_TYPE_HEADER)
                    .build())
            .getRedactionStrategy();
    assertEquals(RedactionStrategy.REDACTION_STRATEGY_REDACT, redactionStrategy);
  }

  @Test
  void upsertAndGetAutomaticSecretRedactionStrategy() {
    boolean automaticSecretRedactionEnabled =
        sensitiveDataStub
            .getAutomaticSecretRedactionStrategy(
                GetAutomaticSecretRedactionStrategyRequest.newBuilder().build())
            .getEnabled();
    assertEquals(true, automaticSecretRedactionEnabled);

    sensitiveDataStub.updateAutomaticSecretRedactionStrategy(
        UpdateAutomaticSecretRedactionStrategyRequest.newBuilder().setEnabled(false).build());
    automaticSecretRedactionEnabled =
        sensitiveDataStub
            .getAutomaticSecretRedactionStrategy(
                GetAutomaticSecretRedactionStrategyRequest.newBuilder().build())
            .getEnabled();
    assertEquals(false, automaticSecretRedactionEnabled);
  }

  @Test
  void createReadUpdateDeleteRedactionRules() {
    NewRedactionRule newRedactionRule1 = getNewRedactionRule("rule1", "^password");
    NewRedactionRule newRedactionRule2 = getNewRedactionRule("rule2", "^name");
    RedactionRule redactionRule1 =
        sensitiveDataStub
            .createRedactionRule(
                CreateRedactionRuleRequest.newBuilder()
                    .setNewRedactionRule(newRedactionRule1)
                    .build())
            .getRedactionRule();
    assertEquals(getRedactionRule(newRedactionRule1, redactionRule1.getId()), redactionRule1);

    RedactionRule redactionRule2 =
        sensitiveDataStub
            .createRedactionRule(
                CreateRedactionRuleRequest.newBuilder()
                    .setNewRedactionRule(newRedactionRule2)
                    .build())
            .getRedactionRule();

    assertEquals(
        List.of(redactionRule1, redactionRule2),
        sensitiveDataStub
            .getAllRedactionRules(GetAllRedactionRulesRequest.getDefaultInstance())
            .getRedactionRulesList());

    RedactionRule ruleToUpdate =
        redactionRule1.toBuilder()
            .setName("rule1a")
            .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_HASH)
            .build();
    RedactionRule updatedRule =
        sensitiveDataStub
            .updateRedactionRule(
                UpdateRedactionRuleRequest.newBuilder().setRedactionRule(ruleToUpdate).build())
            .getRedactionRule();
    assertEquals(ruleToUpdate, updatedRule);

    assertEquals(
        List.of(updatedRule, redactionRule2),
        sensitiveDataStub
            .getAllRedactionRules(GetAllRedactionRulesRequest.getDefaultInstance())
            .getRedactionRulesList());

    sensitiveDataStub.deleteRedactionRule(
        DeleteRedactionRuleRequest.newBuilder().setRedactionRuleId(redactionRule2.getId()).build());
    assertEquals(
        List.of(updatedRule),
        sensitiveDataStub
            .getAllRedactionRules(GetAllRedactionRulesRequest.getDefaultInstance())
            .getRedactionRulesList());
  }

  @Test
  void createRedactionRuleWithInvalidRegexShouldFail() {
    NewRedactionRule newRedactionRule = getNewRedactionRule("rule1", "pass**");
    assertThrows(
        StatusRuntimeException.class,
        () ->
            sensitiveDataStub
                .createRedactionRule(
                    CreateRedactionRuleRequest.newBuilder()
                        .setNewRedactionRule(newRedactionRule)
                        .build())
                .getRedactionRule());
  }

  private NewRedactionRule getNewRedactionRule(String name, String regex) {
    return NewRedactionRule.newBuilder()
        .setName(name)
        .setDescription("sample rule")
        .setCategory("auth")
        .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_REDACT)
        .setMatchType(MatchType.MATCH_TYPE_KEY)
        .setRegex(regex)
        .build();
  }

  private RedactionRule getRedactionRule(NewRedactionRule newRedactionRule, String id) {
    RedactionRule.Builder builder =
        RedactionRule.newBuilder()
            .setId(id)
            .setName(newRedactionRule.getName())
            .setDescription(newRedactionRule.getDescription())
            .setCategory(newRedactionRule.getCategory())
            .setRedactionStrategy(newRedactionRule.getRedactionStrategy())
            .setMatchType(newRedactionRule.getMatchType())
            .setRegex(newRedactionRule.getRegex());
    if (newRedactionRule.hasComplexData()) {
      builder.setComplexData(newRedactionRule.getComplexData());
    }
    return builder.build();
  }
}
