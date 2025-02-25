package ai.traceable.customsignature.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.customsignature.config.service.modsec.ModsecRulesManager;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.DeleteCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.EventSeverity;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.ExpiryDetails;
import ai.traceable.customsignature.config.service.v1.KeyValueExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;
import io.grpc.Status;
import io.grpc.Status.Code;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CustomSignatureRulesValidatorTest {

  private static final String ZERO_EXPIRY_DURATION = "P0DT0H0M";
  private static final String NON_ZERO_EXPIRY_DURATION = "P2DT3H4M";

  private ModsecRulesManager modsecRulesManager;
  private CustomSignatureRulesValidator rulesValidator;
  private ClauseValidator clauseValidator;

  @BeforeEach
  public void setup() {
    this.modsecRulesManager = mock(ModsecRulesManager.class);
    this.clauseValidator = new ClauseValidator();
    when(modsecRulesManager.validateModsecRule(any(), any())).thenReturn(Status.OK);
    when(modsecRulesManager.isInlineRuleMappingSupported(any())).thenReturn(true);
    when(modsecRulesManager.containsModsecConvertibleClauses(any())).thenReturn(true);
    this.rulesValidator = new CustomSignatureRulesValidator(modsecRulesManager, clauseValidator);
  }

  @Test
  public void testValidateCreateRule() {
    CreateCustomSignatureRuleRequest request;
    Status status;

    request = CreateCustomSignatureRuleRequest.newBuilder().build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid name"));

    request = CreateCustomSignatureRuleRequest.newBuilder().setName("name").build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid definition"));

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setDefinition(RuleDefinition.newBuilder().build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid effect"));

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setDefinition(RuleDefinition.newBuilder().build())
            .setEffect(RuleEffect.newBuilder().build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid event type"));

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setKeyValueExpression(
                                        KeyValueExpression.newBuilder()
                                            .setMatchCategory(MatchCategory.MATCH_CATEGORY_RESPONSE)
                                            .setTag(KeyValueTag.KEY_VALUE_TAG_HEADER)
                                            .setKeyMatchOperator(
                                                MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .setMatchKey("name")
                                            .setValueMatchOperator(
                                                MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .setMatchValue("traceable")
                                            .build()))
                            .build()))
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_ALLOW)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW))
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(
        status
            .getDescription()
            .contains(
                "Custom signature rule with a response category or a attribute clause is not compatible with the specified event type"));

    RuleEffect ruleEffect =
        RuleEffect.newBuilder()
            .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
            .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
            .build();

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(RuleDefinition.newBuilder().build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid clause group"));

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(ClauseGroup.newBuilder().build())
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid clause operator"));

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .build())
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("at least one clause"));

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(Clause.newBuilder().build())
                            .build())
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("Clause expression"));

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(MatchExpression.newBuilder().build())
                                    .build())
                            .build())
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid match key"));

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(
                                        MatchExpression.newBuilder()
                                            .setMatchKey(MatchKey.MATCH_KEY_HEADER_VALUE)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid match operator"));

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(
                                        MatchExpression.newBuilder()
                                            .setMatchKey(MatchKey.MATCH_KEY_HEADER_VALUE)
                                            .setMatchOperator(
                                                MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                            .setMatchValue("**invalid")
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("Invalid Regex Value"));

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(
                                        MatchExpression.newBuilder()
                                            .setMatchKey(MatchKey.MATCH_KEY_HEADER_VALUE)
                                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                    .build())
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(
                                        MatchExpression.newBuilder()
                                            .setMatchKey(MatchKey.MATCH_KEY_HEADER_VALUE)
                                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
                    .build())
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(
                                        MatchExpression.newBuilder()
                                            .setMatchKey(MatchKey.MATCH_KEY_HEADER_VALUE)
                                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder().setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION).build())
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(
                                        MatchExpression.newBuilder()
                                            .setMatchKey(MatchKey.MATCH_KEY_HEADER_VALUE)
                                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid event severity"));

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setKeyValueExpression(KeyValueExpression.newBuilder().build())
                                    .build())
                            .build())
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid tag"));

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setKeyValueExpression(
                                        KeyValueExpression.newBuilder()
                                            .setTag(KeyValueTag.KEY_VALUE_TAG_HEADER)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid match key"));

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setKeyValueExpression(
                                        KeyValueExpression.newBuilder()
                                            .setTag(KeyValueTag.KEY_VALUE_TAG_HEADER)
                                            .setMatchKey("key")
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid key match operator"));

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setKeyValueExpression(
                                        KeyValueExpression.newBuilder()
                                            .setTag(KeyValueTag.KEY_VALUE_TAG_HEADER)
                                            .setMatchKey("key")
                                            .setKeyMatchOperator(
                                                MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid match value"));

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setKeyValueExpression(
                                        KeyValueExpression.newBuilder()
                                            .setTag(KeyValueTag.KEY_VALUE_TAG_HEADER)
                                            .setMatchKey("key")
                                            .setKeyMatchOperator(
                                                MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .setMatchValue("value")
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid value match operator"));

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setKeyValueExpression(
                                        KeyValueExpression.newBuilder()
                                            .setTag(KeyValueTag.KEY_VALUE_TAG_HEADER)
                                            .setMatchKey("key")
                                            .setKeyMatchOperator(
                                                MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .setMatchValue("inva**lid")
                                            .setValueMatchOperator(
                                                MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("Invalid Regex Value"));

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                    .setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION))
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(
                                        MatchExpression.newBuilder()
                                            .setMatchCategory(MatchCategory.MATCH_CATEGORY_REQUEST)
                                            .setMatchValue("101")
                                            .setMatchKey(MatchKey.MATCH_KEY_BODY_SIZE)
                                            .setMatchOperator(
                                                MatchOperator.MATCH_OPERATOR_GREATER_THAN)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                    .setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION))
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(
                                        MatchExpression.newBuilder()
                                            .setMatchCategory(MatchCategory.MATCH_CATEGORY_RESPONSE)
                                            .setMatchKey(MatchKey.MATCH_KEY_BODY_SIZE)
                                            .setMatchOperator(
                                                MatchOperator.MATCH_OPERATOR_GREATER_THAN)
                                            .setMatchValue("value")
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("numerical value for match operator"));

    // valid Cyrillic regex
    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setKeyValueExpression(
                                        KeyValueExpression.newBuilder()
                                            .setTag(KeyValueTag.KEY_VALUE_TAG_HEADER)
                                            .setMatchKey("key")
                                            .setKeyMatchOperator(
                                                MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .setMatchValue("(*UTF8)or\\p{Cyrillic}something")
                                            .setValueMatchOperator(
                                                MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                    .setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION))
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(
                                        MatchExpression.newBuilder()
                                            .setMatchCategory(MatchCategory.MATCH_CATEGORY_RESPONSE)
                                            .setMatchKey(MatchKey.MATCH_KEY_BODY_SIZE)
                                            .setMatchOperator(
                                                MatchOperator.MATCH_OPERATOR_GREATER_THAN)
                                            .setMatchValue("100.1")
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    RuleDefinition validRuleDefinition =
        RuleDefinition.newBuilder()
            .setClauseGroup(
                ClauseGroup.newBuilder()
                    .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                    .addClauses(
                        Clause.newBuilder()
                            .setKeyValueExpression(
                                KeyValueExpression.newBuilder()
                                    .setTag(KeyValueTag.KEY_VALUE_TAG_HEADER)
                                    .setMatchKey("key")
                                    .setKeyMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                    .setMatchValue("value")
                                    .setValueMatchOperator(
                                        MatchOperator.MATCH_OPERATOR_GREATER_THAN)
                                    .build())
                            .build())
                    .build())
            .build();

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(validRuleDefinition)
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(validRuleDefinition)
            .setBlockingExpiryDetails(ExpiryDetails.newBuilder().setExpiryDuration("1234").build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("Blocking expiry duration can't be parsed"));

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(validRuleDefinition)
            .setBlockingExpiryDetails(
                ExpiryDetails.newBuilder().setExpiryDuration(NON_ZERO_EXPIRY_DURATION).build())
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    when(modsecRulesManager.validateModsecRule(request.getName(), request.getDefinition()))
        .thenReturn(Status.INTERNAL);
    status = rulesValidator.validate(request);
    assertEquals(Code.INTERNAL, status.getCode());

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(validRuleDefinition)
            .setBlockingExpiryDetails(
                ExpiryDetails.newBuilder().setExpiryDuration(NON_ZERO_EXPIRY_DURATION).build())
            .setRuleScope(RuleScope.newBuilder().setEnvironmentScope(EnvironmentScope.newBuilder()))
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Environment scope should have at least one environment", status.getDescription());

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(validRuleDefinition)
            .setBlockingExpiryDetails(
                ExpiryDetails.newBuilder().setExpiryDuration(NON_ZERO_EXPIRY_DURATION).build())
            .setRuleScope(
                RuleScope.newBuilder()
                    .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("")))
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Environment id should not be empty string.", status.getDescription());

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(validRuleDefinition)
            .setBlockingExpiryDetails(
                ExpiryDetails.newBuilder().setExpiryDuration(NON_ZERO_EXPIRY_DURATION).build())
            .setRuleScope(
                RuleScope.newBuilder()
                    .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("dev")))
            .build();
    when(modsecRulesManager.validateModsecRule(request.getName(), request.getDefinition()))
        .thenReturn(Status.OK);
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());
  }

  @Test
  public void testValidateUpdateRule() {
    CustomSignatureRule rule;
    Status status;

    status = rulesValidator.validate(UpdateCustomSignatureRuleRequest.newBuilder().build());
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid id"));

    rule = CustomSignatureRule.newBuilder().setId("id").build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid name"));

    rule = CustomSignatureRule.newBuilder().setId("id").setName("name").build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid definition"));

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setDefinition(RuleDefinition.newBuilder().build())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid effect"));

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setDefinition(RuleDefinition.newBuilder().build())
            .setEffect(RuleEffect.newBuilder().build())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid event type"));

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(
                                        MatchExpression.newBuilder()
                                            .setMatchCategory(MatchCategory.MATCH_CATEGORY_RESPONSE)
                                            .setMatchKey(MatchKey.MATCH_KEY_HOST)
                                            .setMatchValue("traceable")
                                            .build()))
                            .build()))
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW))
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(
        status
            .getDescription()
            .contains(
                "Custom signature rule with a response category or a attribute clause is not compatible with the specified event type"));

    RuleEffect ruleEffect =
        RuleEffect.newBuilder()
            .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
            .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
            .build();

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(RuleDefinition.newBuilder().build())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid clause group"));

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(ClauseGroup.newBuilder().build())
                    .build())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid clause operator"));

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .build())
                    .build())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("at least one clause"));

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(Clause.newBuilder().build())
                            .build())
                    .build())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("Clause expression"));

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(MatchExpression.newBuilder().build())
                                    .build())
                            .build())
                    .build())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid match key"));

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(
                                        MatchExpression.newBuilder()
                                            .setMatchKey(MatchKey.MATCH_KEY_HEADER_VALUE)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid match operator"));

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(
                                        MatchExpression.newBuilder()
                                            .setMatchKey(MatchKey.MATCH_KEY_HEADER_VALUE)
                                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.OK, status.getCode());

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                    .build())
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(
                                        MatchExpression.newBuilder()
                                            .setMatchKey(MatchKey.MATCH_KEY_HEADER_VALUE)
                                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.OK, status.getCode());

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
                    .build())
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(
                                        MatchExpression.newBuilder()
                                            .setMatchKey(MatchKey.MATCH_KEY_HEADER_VALUE)
                                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.OK, status.getCode());

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder().setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION).build())
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(
                                        MatchExpression.newBuilder()
                                            .setMatchKey(MatchKey.MATCH_KEY_HEADER_VALUE)
                                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid event severity"));

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setKeyValueExpression(KeyValueExpression.newBuilder().build())
                                    .build())
                            .build())
                    .build())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid tag"));

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setKeyValueExpression(
                                        KeyValueExpression.newBuilder()
                                            .setTag(KeyValueTag.KEY_VALUE_TAG_HEADER)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid match key"));

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setKeyValueExpression(
                                        KeyValueExpression.newBuilder()
                                            .setTag(KeyValueTag.KEY_VALUE_TAG_HEADER)
                                            .setMatchKey("key")
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid key match operator"));

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setKeyValueExpression(
                                        KeyValueExpression.newBuilder()
                                            .setTag(KeyValueTag.KEY_VALUE_TAG_HEADER)
                                            .setMatchKey("key")
                                            .setKeyMatchOperator(
                                                MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid match value"));

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setKeyValueExpression(
                                        KeyValueExpression.newBuilder()
                                            .setTag(KeyValueTag.KEY_VALUE_TAG_HEADER)
                                            .setMatchKey("key")
                                            .setKeyMatchOperator(
                                                MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .setMatchValue("value")
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid value match operator"));

    RuleDefinition validRuleDefinition =
        RuleDefinition.newBuilder()
            .setClauseGroup(
                ClauseGroup.newBuilder()
                    .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                    .addClauses(
                        Clause.newBuilder()
                            .setKeyValueExpression(
                                KeyValueExpression.newBuilder()
                                    .setTag(KeyValueTag.KEY_VALUE_TAG_HEADER)
                                    .setMatchKey("key")
                                    .setKeyMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                    .setMatchValue("value")
                                    .setValueMatchOperator(
                                        MatchOperator.MATCH_OPERATOR_GREATER_THAN)
                                    .build())
                            .build())
                    .build())
            .build();
    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(validRuleDefinition)
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.OK, status.getCode());

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(validRuleDefinition)
            .setBlockingExpiryDetails(
                ExpiryDetails.newBuilder().setExpiryDuration("invalid").build())
            .build();

    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("Blocking expiry duration can't be parsed"));

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(validRuleDefinition)
            .setBlockingExpiryDetails(
                ExpiryDetails.newBuilder().setExpiryDuration(ZERO_EXPIRY_DURATION).build())
            .build();
    UpdateCustomSignatureRuleRequest request =
        UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    when(modsecRulesManager.validateModsecRule(rule.getName(), rule.getDefinition()))
        .thenReturn(Status.INTERNAL);
    status = rulesValidator.validate(request);
    assertEquals(Code.INTERNAL, status.getCode());

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(
                                        MatchExpression.newBuilder()
                                            .setMatchKey(MatchKey.MATCH_KEY_HEADER_VALUE)
                                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .setRuleScope(RuleScope.newBuilder().setEnvironmentScope(EnvironmentScope.newBuilder()))
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Environment scope should have at least one environment", status.getDescription());

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(
                                        MatchExpression.newBuilder()
                                            .setMatchKey(MatchKey.MATCH_KEY_HEADER_VALUE)
                                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .setRuleScope(
                RuleScope.newBuilder()
                    .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("")))
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Environment id should not be empty string.", status.getDescription());

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(
                                        MatchExpression.newBuilder()
                                            .setMatchKey(MatchKey.MATCH_KEY_HEADER_VALUE)
                                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .setRuleScope(
                RuleScope.newBuilder()
                    .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("dev")))
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertEquals(Code.OK, status.getCode());
  }

  @Test
  public void testValidateDeleteRule() {
    Status status;

    status = rulesValidator.validate(DeleteCustomSignatureRuleRequest.newBuilder().build());
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("valid id"));

    status =
        rulesValidator.validate(DeleteCustomSignatureRuleRequest.newBuilder().setId("id").build());
    assertEquals(Code.OK, status.getCode());
  }
}
