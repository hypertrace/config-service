package ai.traceable.customsignature.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.customsignature.config.service.modsec.ModsecRulesManager;
import ai.traceable.customsignature.config.service.modsec.ModsecRulesSupportChecker;
import ai.traceable.customsignature.config.service.v1.AgentModification;
import ai.traceable.customsignature.config.service.v1.AgentRuleEffect;
import ai.traceable.customsignature.config.service.v1.BodyModification;
import ai.traceable.customsignature.config.service.v1.BulkDeleteCustomSignatureRulesRequest;
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
import ai.traceable.customsignature.config.service.v1.FieldValue;
import ai.traceable.customsignature.config.service.v1.HeaderInjection;
import ai.traceable.customsignature.config.service.v1.IpAddressExpression;
import ai.traceable.customsignature.config.service.v1.IpType;
import ai.traceable.customsignature.config.service.v1.IpTypeExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.LhsRhsKeysExpression;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleEffectWithModifications;
import ai.traceable.customsignature.config.service.v1.RuleEvaluationPoint;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.customsignature.config.service.v1.RuleSource;
import ai.traceable.customsignature.config.service.v1.StatusCodeModification;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;
import com.google.protobuf.Value;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class CustomSignatureRulesValidatorTest {

  private static final String ZERO_EXPIRY_DURATION = "P0DT0H0M";
  private static final String NON_ZERO_EXPIRY_DURATION = "P2DT3H4M";
  private final String HEADER_MATCH_VALUE = "header-value";

  private ModsecRulesManager modsecRulesManager;
  private CustomSignatureRulesValidator rulesValidator;
  private MockedStatic<ModsecRulesSupportChecker> mockedModsecRulesSupportChecker;

  private static final List<RuleEvaluationPoint> allRuleEvaluationPoints =
      List.of(
          RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM,
          RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE,
          RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT);

  @BeforeEach
  public void setup() {
    if (mockedModsecRulesSupportChecker != null) {
      mockedModsecRulesSupportChecker.close();
    }
    this.modsecRulesManager = mock(ModsecRulesManager.class);
    ClauseGroupValidator clauseGroupValidator = new ClauseGroupValidator();
    when(modsecRulesManager.validateModsecRule(any(), any())).thenReturn(Status.OK);
    mockedModsecRulesSupportChecker = Mockito.mockStatic(ModsecRulesSupportChecker.class);
    mockedModsecRulesSupportChecker
        .when(() -> ModsecRulesSupportChecker.isInlineRuleMappingSupported(any()))
        .thenReturn(true);
    when(modsecRulesManager.containsModsecConvertibleClauses(any())).thenReturn(true);
    this.rulesValidator =
        new CustomSignatureRulesValidator(modsecRulesManager, clauseGroupValidator);
  }

  @AfterEach
  public void tearDown() {
    if (mockedModsecRulesSupportChecker != null) {
      mockedModsecRulesSupportChecker.close();
      mockedModsecRulesSupportChecker = null;
    }
  }

  @Test
  public void testValidateCreateRule() {
    CreateCustomSignatureRuleRequest request;
    Status status;

    request = CreateCustomSignatureRuleRequest.newBuilder().build();
    status = rulesValidator.validate(request);
    assertInvalidArgument(status, "valid name");

    request = CreateCustomSignatureRuleRequest.newBuilder().setName("name").build();
    status = rulesValidator.validate(request);
    assertInvalidArgument(status, "valid definition");

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setDefinition(RuleDefinition.getDefaultInstance())
            .build();
    status = rulesValidator.validate(request);
    assertInvalidArgument(status, "valid effect");

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setDefinition(RuleDefinition.getDefaultInstance())
            .setEffect(RuleEffect.getDefaultInstance())
            .build();
    status = rulesValidator.validate(request);
    assertInvalidArgument(status, "valid event type");

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
                                            .setMatchValue("traceable")))))
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_ALLOW)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW))
            .build();
    status = rulesValidator.validate(request);
    assertInvalidArgument(
        status,
        "Custom signature rule with a response category or a attribute clause is not compatible with the specified event type");

    RuleEffect ruleEffect =
        RuleEffect.newBuilder()
            .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
            .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
            .build();

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(RuleDefinition.getDefaultInstance())
            .build();
    status = rulesValidator.validate(request);
    assertInvalidArgument(status, "valid clause group");

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(ClauseGroup.getDefaultInstance())
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertInvalidArgument(status, "valid clause operator");

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND))
                    .build())
            .build();
    status = rulesValidator.validate(request);
    assertInvalidArgument(status, "at least one clause");

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(Clause.getDefaultInstance())))
            .build();
    status = rulesValidator.validate(request);
    assertInvalidArgument(status, "Clause expression");

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
                                    .setMatchExpression(MatchExpression.getDefaultInstance()))))
            .build();
    status = rulesValidator.validate(request);
    assertInvalidArgument(status, "valid match key");

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
                                            .setMatchKey(MatchKey.MATCH_KEY_HEADER_VALUE)))))
            .build();
    status = rulesValidator.validate(request);
    assertInvalidArgument(status, "valid match operator");

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
                                            .setMatchValue("**invalid")))))
            .build();
    status = rulesValidator.validate(request);
    assertInvalidArgument(status, "Invalid Regex Value");

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
                                            .setMatchValue(HEADER_MATCH_VALUE)))))
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM))
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
                                            .setMatchValue(HEADER_MATCH_VALUE)))))
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
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM))
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
                                            .setMatchValue(HEADER_MATCH_VALUE)))))
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION)
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM))
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
                                            .setMatchValue(HEADER_MATCH_VALUE)))))
            .build();
    status = rulesValidator.validate(request);
    assertInvalidArgument(status, "valid event severity");

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
                                        KeyValueExpression.getDefaultInstance()))))
            .build();
    status = rulesValidator.validate(request);
    assertInvalidArgument(status, "valid tag");

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
                                            .setTag(KeyValueTag.KEY_VALUE_TAG_HEADER)))))
            .build();
    status = rulesValidator.validate(request);
    assertInvalidArgument(status, "valid match key");

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
                                            .setMatchKey("key")))))
            .build();
    status = rulesValidator.validate(request);
    assertInvalidArgument(status, "valid key match operator");

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
                                                MatchOperator.MATCH_OPERATOR_EQUALS)))))
            .build();
    status = rulesValidator.validate(request);
    assertInvalidArgument(status, "valid match value");

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
                                            .setMatchValue("value")))))
            .build();
    status = rulesValidator.validate(request);
    assertInvalidArgument(status, "valid value match operator");

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
                                                MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX)))))
            .build();
    status = rulesValidator.validate(request);
    assertInvalidArgument(status, "Invalid Regex Value");

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                    .setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION)
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM))
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
                                                MatchOperator.MATCH_OPERATOR_GREATER_THAN)))))
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                    .setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION)
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM))
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
                                            .setMatchValue("value")))))
            .build();
    status = rulesValidator.validate(request);
    assertInvalidArgument(status, "integer match value for match operator");

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
                                                MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX)))))
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                    .setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION)
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM))
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
                                            .setMatchValue("100")))))
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
                                    .setMatchCategory(MatchCategory.MATCH_CATEGORY_REQUEST)
                                    .setTag(KeyValueTag.KEY_VALUE_TAG_HEADER)
                                    .setMatchKey("key")
                                    .setKeyMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                    .setMatchValue("value")
                                    .setValueMatchOperator(
                                        MatchOperator.MATCH_OPERATOR_GREATER_THAN))))
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
            .setBlockingExpiryDetails(ExpiryDetails.newBuilder().setExpiryDuration("1234"))
            .build();
    status = rulesValidator.validate(request);
    assertInvalidArgument(status, "Blocking expiry duration can't be parsed");

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(validRuleDefinition)
            .setBlockingExpiryDetails(
                ExpiryDetails.newBuilder().setExpiryDuration(NON_ZERO_EXPIRY_DURATION))
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
                ExpiryDetails.newBuilder().setExpiryDuration(NON_ZERO_EXPIRY_DURATION))
            .setRuleScope(
                RuleScope.newBuilder().setEnvironmentScope(EnvironmentScope.getDefaultInstance()))
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
                ExpiryDetails.newBuilder().setExpiryDuration(NON_ZERO_EXPIRY_DURATION))
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
                ExpiryDetails.newBuilder().setExpiryDuration(NON_ZERO_EXPIRY_DURATION))
            .setRuleScope(
                RuleScope.newBuilder()
                    .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("dev")))
            .build();
    when(modsecRulesManager.validateModsecRule(request.getName(), request.getDefinition()))
        .thenReturn(Status.OK);
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                    .addAllRuleEvaluationPoints(allRuleEvaluationPoints))
            .setDefinition(validRuleDefinition)
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_ALLOW)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
                    .addAllRuleEvaluationPoints(allRuleEvaluationPoints))
            .setDefinition(validRuleDefinition)
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_HIGH)
                    .addAllRuleEvaluationPoints(allRuleEvaluationPoints)
                    .addEffects(
                        RuleEffectWithModifications.newBuilder()
                            .setAgentRuleEffect(
                                AgentRuleEffect.newBuilder()
                                    .addAgentModifications(
                                        AgentModification.newBuilder()
                                            .setHeaderInjection(
                                                HeaderInjection.newBuilder()
                                                    .setHeaderName("header-name")
                                                    .setHeaderCategory(
                                                        MatchCategory.MATCH_CATEGORY_REQUEST)
                                                    .setValue(
                                                        FieldValue.newBuilder()
                                                            .setStaticValue("static-value")))))))
            .setDefinition(validRuleDefinition)
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_TESTING_DETECTION)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_HIGH)
                    .addAllRuleEvaluationPoints(allRuleEvaluationPoints)
                    .addEffects(
                        RuleEffectWithModifications.newBuilder()
                            .setAgentRuleEffect(
                                AgentRuleEffect.newBuilder()
                                    .addAgentModifications(
                                        AgentModification.newBuilder()
                                            .setHeaderInjection(
                                                HeaderInjection.newBuilder()
                                                    .setHeaderName("header-name")
                                                    .setHeaderCategory(
                                                        MatchCategory.MATCH_CATEGORY_REQUEST)
                                                    .setValue(
                                                        FieldValue.newBuilder()
                                                            .setStaticValue("static-value")))))))
            .setDefinition(validRuleDefinition)
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_HIGH)
                    .addAllRuleEvaluationPoints(allRuleEvaluationPoints)
                    .addEffects(
                        RuleEffectWithModifications.newBuilder()
                            .setAgentRuleEffect(
                                AgentRuleEffect.newBuilder()
                                    .addAgentModifications(
                                        AgentModification.newBuilder()
                                            .setStatusCodeModification(
                                                StatusCodeModification.newBuilder()
                                                    .setStatusCode(404))))))
            .setDefinition(validRuleDefinition)
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertNotNull(status.getDescription());
    assertEquals("Rule is not AGENT-compatible", status.getDescription());

    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_TESTING_DETECTION)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_HIGH)
                    .addAllRuleEvaluationPoints(allRuleEvaluationPoints)
                    .addEffects(
                        RuleEffectWithModifications.newBuilder()
                            .setAgentRuleEffect(
                                AgentRuleEffect.newBuilder()
                                    .addAgentModifications(
                                        AgentModification.newBuilder()
                                            .setBodyModification(
                                                BodyModification.newBuilder()
                                                    .setLocationCategory(
                                                        MatchCategory.MATCH_CATEGORY_REQUEST)
                                                    .setBodyValue(
                                                        FieldValue.newBuilder()
                                                            .setStaticValue("static-val")))))))
            .setDefinition(validRuleDefinition)
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertNotNull(status.getDescription());
    assertEquals("Rule is not AGENT-compatible", status.getDescription());
  }

  @Test
  void testValidateCreateUpdateRuleForLhsRhsKeysExpression() {
    RuleEffect ruleEffect =
        RuleEffect.newBuilder()
            .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
            .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
            .build();

    CreateCustomSignatureRuleRequest createRequest =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .addClauses(
                                Clause.newBuilder()
                                    .setLhsRhsKeysExpression(
                                        LhsRhsKeysExpression.newBuilder()
                                            .setLhsKeyExpression(
                                                getMatchExpression(
                                                    MatchKey.MATCH_KEY_HEADER_NAME,
                                                    MatchOperator.MATCH_OPERATOR_EQUALS,
                                                    "str-value-1"))
                                            .setRhsKeyExpression(
                                                getMatchExpression(
                                                    MatchKey.MATCH_KEY_COOKIE_NAME,
                                                    MatchOperator.MATCH_OPERATOR_CONTAINS,
                                                    "str-value-2"))
                                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)))
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)))
            .setRuleScope(
                RuleScope.newBuilder()
                    .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env-id")))
            .build();
    Status status = rulesValidator.validate(createRequest);
    assertEquals(Code.OK, status.getCode());

    createRequest =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .addClauses(
                                Clause.newBuilder()
                                    .setLhsRhsKeysExpression(
                                        LhsRhsKeysExpression.newBuilder()
                                            .setLhsKeyExpression(
                                                getMatchExpression(
                                                    MatchKey.MATCH_KEY_HEADER_NAME,
                                                    MatchOperator.MATCH_OPERATOR_EQUALS,
                                                    "str-value-1"))
                                            .setRhsKeyExpression(
                                                getMatchExpression(
                                                    MatchKey.MATCH_KEY_COOKIE_NAME,
                                                    MatchOperator.MATCH_OPERATOR_GREATER_THAN,
                                                    "str-value-2"))
                                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)))
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)))
            .setRuleScope(
                RuleScope.newBuilder()
                    .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env-id")))
            .build();
    status = rulesValidator.validate(createRequest);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());

    UpdateCustomSignatureRuleRequest updateRequest =
        UpdateCustomSignatureRuleRequest.newBuilder()
            .setRule(
                CustomSignatureRule.newBuilder()
                    .setName("name")
                    .setId("id")
                    .setEffect(ruleEffect)
                    .setDefinition(
                        RuleDefinition.newBuilder()
                            .setClauseGroup(
                                ClauseGroup.newBuilder()
                                    .addClauses(
                                        Clause.newBuilder()
                                            .setLhsRhsKeysExpression(
                                                LhsRhsKeysExpression.newBuilder()
                                                    .setLhsKeyExpression(
                                                        getMatchExpression(
                                                            MatchKey.MATCH_KEY_HEADER_NAME,
                                                            MatchOperator.MATCH_OPERATOR_EQUALS,
                                                            "str-value-1"))
                                                    .setRhsKeyExpression(
                                                        getMatchExpression(
                                                            MatchKey.MATCH_KEY_COOKIE_NAME,
                                                            MatchOperator.MATCH_OPERATOR_CONTAINS,
                                                            "str-value-2"))
                                                    .setMatchOperator(
                                                        MatchOperator.MATCH_OPERATOR_EQUALS)))
                                    .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)))
                    .setRuleScope(
                        RuleScope.newBuilder()
                            .setEnvironmentScope(
                                EnvironmentScope.newBuilder().addEnvironmentIds("env-id"))))
            .build();
    status = rulesValidator.validate(updateRequest);
    assertEquals(Code.OK, status.getCode());

    updateRequest =
        UpdateCustomSignatureRuleRequest.newBuilder()
            .setRule(
                CustomSignatureRule.newBuilder()
                    .setName("name")
                    .setId("id")
                    .setEffect(ruleEffect)
                    .setDefinition(
                        RuleDefinition.newBuilder()
                            .setClauseGroup(
                                ClauseGroup.newBuilder()
                                    .addClauses(
                                        Clause.newBuilder()
                                            .setLhsRhsKeysExpression(
                                                LhsRhsKeysExpression.newBuilder()
                                                    .setLhsKeyExpression(
                                                        getMatchExpression(
                                                            MatchKey.MATCH_KEY_HEADER_NAME,
                                                            MatchOperator.MATCH_OPERATOR_EQUALS,
                                                            "str-value-1"))
                                                    .setRhsKeyExpression(
                                                        getMatchExpression(
                                                            MatchKey.MATCH_KEY_COOKIE_NAME,
                                                            MatchOperator
                                                                .MATCH_OPERATOR_GREATER_THAN,
                                                            "str-value-2"))
                                                    .setMatchOperator(
                                                        MatchOperator.MATCH_OPERATOR_EQUALS)))
                                    .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)))
                    .setRuleScope(
                        RuleScope.newBuilder()
                            .setEnvironmentScope(
                                EnvironmentScope.newBuilder().addEnvironmentIds("env-id"))))
            .build();
    status = rulesValidator.validate(updateRequest);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  public void testValidateUpdateRule() {
    CustomSignatureRule rule;
    Status status;
    status = rulesValidator.validate(UpdateCustomSignatureRuleRequest.getDefaultInstance());
    assertInvalidArgument(status, "valid id");

    rule = CustomSignatureRule.newBuilder().setId("id").build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertInvalidArgument(status, "valid name");

    rule = CustomSignatureRule.newBuilder().setId("id").setName("name").build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertInvalidArgument(status, "valid definition");

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setDefinition(RuleDefinition.getDefaultInstance())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertInvalidArgument(status, "valid effect");

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setDefinition(RuleDefinition.getDefaultInstance())
            .setEffect(RuleEffect.getDefaultInstance())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertInvalidArgument(status, "valid event type");

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
                                            .setMatchValue("traceable")))))
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM))
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertInvalidArgument(
        status,
        "Custom signature rule with a response category or a attribute clause is not compatible with the specified event type");

    RuleEffect ruleEffect =
        RuleEffect.newBuilder()
            .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
            .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
            .build();

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(RuleDefinition.getDefaultInstance())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertInvalidArgument(status, "valid clause group");

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(ClauseGroup.getDefaultInstance())
                    .build())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertInvalidArgument(status, "valid clause operator");

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND))
                    .build())
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertInvalidArgument(status, "at least one clause");

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
                            .addClauses(Clause.getDefaultInstance())))
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertInvalidArgument(status, "Clause expression");

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
                                    .setMatchExpression(MatchExpression.getDefaultInstance()))))
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertInvalidArgument(status, "valid match key");

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
                                            .setMatchKey(MatchKey.MATCH_KEY_HEADER_VALUE)))))
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertInvalidArgument(status, "valid match operator");

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
                                            .setMatchValue(HEADER_MATCH_VALUE)))))
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
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM))
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
                                            .setMatchValue(HEADER_MATCH_VALUE)))))
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
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM))
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
                                            .setMatchValue(HEADER_MATCH_VALUE)))))
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
                                            .setMatchValue(HEADER_MATCH_VALUE)))))
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertInvalidArgument(status, "valid event severity");

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
                                        KeyValueExpression.getDefaultInstance()))))
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertInvalidArgument(status, "valid tag");

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
                                            .setTag(KeyValueTag.KEY_VALUE_TAG_HEADER)))))
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertInvalidArgument(status, "valid match key");

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
                                            .setMatchKey("key")))))
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertInvalidArgument(status, "valid key match operator");

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
                                                MatchOperator.MATCH_OPERATOR_EQUALS)))))
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertInvalidArgument(status, "valid match value");

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
                                            .setMatchValue("value")))))
            .build();
    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertInvalidArgument(status, "valid value match operator");

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
                                        MatchOperator.MATCH_OPERATOR_GREATER_THAN))))
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
            .setBlockingExpiryDetails(ExpiryDetails.newBuilder().setExpiryDuration("invalid"))
            .build();

    status =
        rulesValidator.validate(
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule).build());
    assertInvalidArgument(status, "Blocking expiry duration can't be parsed");

    rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(validRuleDefinition)
            .setBlockingExpiryDetails(
                ExpiryDetails.newBuilder().setExpiryDuration(ZERO_EXPIRY_DURATION))
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
                                            .setMatchValue(HEADER_MATCH_VALUE))))
                    .build())
            .setRuleScope(
                RuleScope.newBuilder().setEnvironmentScope(EnvironmentScope.getDefaultInstance()))
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
                                            .setMatchValue(HEADER_MATCH_VALUE)))))
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
                                            .setMatchValue(HEADER_MATCH_VALUE)))))
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

    status = rulesValidator.validate(DeleteCustomSignatureRuleRequest.getDefaultInstance());
    assertInvalidArgument(status, "valid id");

    status =
        rulesValidator.validate(DeleteCustomSignatureRuleRequest.newBuilder().setId("id").build());
    assertEquals(Code.OK, status.getCode());
  }

  @Test
  public void testValidateBulkDeleteRules() {
    Status status;

    // Empty ids list should fail
    status = rulesValidator.validate(BulkDeleteCustomSignatureRulesRequest.getDefaultInstance());
    assertInvalidArgument(status, "at least one id");

    // Empty string id in list should fail
    status =
        rulesValidator.validate(
            BulkDeleteCustomSignatureRulesRequest.newBuilder().addIds("").build());
    assertInvalidArgument(status, "empty ids");

    // Valid single id should pass
    status =
        rulesValidator.validate(
            BulkDeleteCustomSignatureRulesRequest.newBuilder().addIds("id1").build());
    assertEquals(Code.OK, status.getCode());

    // Valid multiple ids should pass
    status =
        rulesValidator.validate(
            BulkDeleteCustomSignatureRulesRequest.newBuilder()
                .addIds("id1")
                .addIds("id2")
                .addIds("id3")
                .build());
    assertEquals(Code.OK, status.getCode());

    // Mix of valid and empty ids should fail
    status =
        rulesValidator.validate(
            BulkDeleteCustomSignatureRulesRequest.newBuilder()
                .addIds("id1")
                .addIds("")
                .addIds("id3")
                .build());
    assertInvalidArgument(status, "empty ids");
  }

  @Test
  void testValidateCreateRuleForAggregateConfigParams() {
    RuleEffect ruleEffect =
        RuleEffect.newBuilder()
            .setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION)
            .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
            .build();
    ExpiryDetails expiryDetails =
        ExpiryDetails.newBuilder().setExpiryDuration(NON_ZERO_EXPIRY_DURATION).build();
    RuleScope ruleScope =
        RuleScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("dev"))
            .build();

    CreateCustomSignatureRuleRequest validCreateRequest1 =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("CreateCustomSignatureRule-Request-QueryParamsCount-Equals")
            .setDescription("rule-description")
            .setDefinition(
                getRuleDefinitionForAggregateConfigParams(
                    MatchKey.MATCH_KEY_QUERY_PARAMS_COUNT,
                    MatchOperator.MATCH_OPERATOR_EQUALS,
                    "10",
                    MatchCategory.MATCH_CATEGORY_REQUEST))
            .setEffect(ruleEffect)
            .setRuleScope(ruleScope)
            .setInternal(true)
            .setRuleSource(RuleSource.RULE_SOURCE_TRACEABLE)
            .build();
    Status status = rulesValidator.validate(validCreateRequest1);
    assertEquals(Code.OK, status.getCode());

    CreateCustomSignatureRuleRequest validCreateRequest2 =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("CreateCustomSignatureRule-Request-HeadersCount-NotEqual")
            .setDescription("rule-description")
            .setDefinition(
                getRuleDefinitionForAggregateConfigParams(
                    MatchKey.MATCH_KEY_HEADERS_COUNT,
                    MatchOperator.MATCH_OPERATOR_NOT_EQUAL,
                    "5",
                    MatchCategory.MATCH_CATEGORY_REQUEST))
            .setEffect(ruleEffect)
            .setRuleScope(ruleScope)
            .setInternal(true)
            .setRuleSource(RuleSource.RULE_SOURCE_TRACEABLE)
            .build();
    status = rulesValidator.validate(validCreateRequest2);
    assertEquals(Code.OK, status.getCode());

    CreateCustomSignatureRuleRequest validCreateRequest3 =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("CreateCustomSignatureRule-Request-CookiesCount-GreaterThan")
            .setDescription("rule-description")
            .setDefinition(
                getRuleDefinitionForAggregateConfigParams(
                    MatchKey.MATCH_KEY_COOKIES_COUNT,
                    MatchOperator.MATCH_OPERATOR_GREATER_THAN,
                    "3",
                    MatchCategory.MATCH_CATEGORY_REQUEST))
            .setEffect(ruleEffect)
            .setRuleScope(ruleScope)
            .setInternal(true)
            .setRuleSource(RuleSource.RULE_SOURCE_TRACEABLE)
            .build();
    status = rulesValidator.validate(validCreateRequest3);
    assertEquals(Code.OK, status.getCode());

    CreateCustomSignatureRuleRequest validCreateRequest4 =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("CreateCustomSignatureRule-Response-HeadersCount-LessThan")
            .setDescription("rule-description")
            .setDefinition(
                getRuleDefinitionForAggregateConfigParams(
                    MatchKey.MATCH_KEY_HEADERS_COUNT,
                    MatchOperator.MATCH_OPERATOR_LESS_THAN,
                    "7",
                    MatchCategory.MATCH_CATEGORY_RESPONSE))
            .setEffect(ruleEffect)
            .setRuleScope(ruleScope)
            .setInternal(true)
            .setRuleSource(RuleSource.RULE_SOURCE_TRACEABLE)
            .build();
    status = rulesValidator.validate(validCreateRequest4);
    assertEquals(Code.OK, status.getCode());

    CreateCustomSignatureRuleRequest invalidCreateRequest1 =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("CreateCustomSignatureRule-Response-QueryParamsCount-Equals")
            .setDescription("rule-description")
            .setDefinition(
                getRuleDefinitionForAggregateConfigParams(
                    MatchKey.MATCH_KEY_QUERY_PARAMS_COUNT,
                    MatchOperator.MATCH_OPERATOR_EQUALS,
                    "7",
                    MatchCategory.MATCH_CATEGORY_RESPONSE))
            .setEffect(ruleEffect)
            .setRuleScope(ruleScope)
            .setInternal(true)
            .setRuleSource(RuleSource.RULE_SOURCE_TRACEABLE)
            .build();
    status = rulesValidator.validate(invalidCreateRequest1);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());

    CreateCustomSignatureRuleRequest invalidCreateRequest2 =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("CreateCustomSignatureRule-Response-HeadersCount-LessThan")
            .setDescription("rule-description")
            .setDefinition(
                getRuleDefinitionForAggregateConfigParams(
                    MatchKey.MATCH_KEY_HEADERS_COUNT,
                    MatchOperator.MATCH_OPERATOR_LESS_THAN,
                    "7",
                    MatchCategory.MATCH_CATEGORY_RESPONSE))
            .setEffect(ruleEffect)
            .setBlockingExpiryDetails(expiryDetails)
            .setRuleScope(ruleScope)
            .setInternal(true)
            .setRuleSource(RuleSource.RULE_SOURCE_TRACEABLE)
            .build();
    status = rulesValidator.validate(invalidCreateRequest2);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testCreateRuleRequestWithRuleEvaluationPoints() {
    CreateCustomSignatureRuleRequest request;
    Status status;
    RuleScope ruleScope = getRuleScope();
    RuleEffect ruleEffect1 = getRuleEffectWithInlineAgentRuleEvaluationPoint();
    RuleEffect ruleEffect2 = getRuleEffectWithoutInlineAgentRuleEvaluationPoint();

    // invalid request - nested clause group in a clause for inline tracing agent as 1 of the rule
    // evaluation points
    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("rule-name")
            .setDescription("rule-description")
            .setEffect(ruleEffect1)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .addClauses(
                                Clause.newBuilder()
                                    .setIpTypeExpression(
                                        IpTypeExpression.newBuilder()
                                            .addIpTypes(IpType.IP_TYPE_SCANNER)))
                            .addClauses(
                                Clause.newBuilder()
                                    .setClauseGroup(
                                        ClauseGroup.newBuilder()
                                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                                            .addClauses(
                                                Clause.newBuilder()
                                                    .setMatchExpression(
                                                        MatchExpression.newBuilder()
                                                            .setMatchKey(MatchKey.MATCH_KEY_URL)
                                                            .setMatchOperator(
                                                                MatchOperator
                                                                    .MATCH_OPERATOR_CONTAINS)
                                                            .setMatchCategory(
                                                                MatchCategory
                                                                    .MATCH_CATEGORY_REQUEST)
                                                            .setValue(
                                                                Value.newBuilder()
                                                                    .setStringValue("url-value"))))
                                            .addClauses(
                                                Clause.newBuilder()
                                                    .setIpAddressExpression(
                                                        IpAddressExpression.newBuilder()
                                                            .addIpAddresses("1.2.3.4")))))
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)))
            .setRuleScope(ruleScope)
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertSame(
        "Rules evaluated at INLINE_TRACING_AGENT cannot have nested clauses.",
        status.getDescription());

    // invalid request - OR clause operator for inline tracing agent as 1 of the rule evaluation
    // points
    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("rule-name")
            .setDescription("rule-description")
            .setEffect(ruleEffect1)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .addClauses(
                                Clause.newBuilder()
                                    .setIpTypeExpression(
                                        IpTypeExpression.newBuilder()
                                            .addIpTypes(IpType.IP_TYPE_SCANNER)))
                            .addClauses(
                                Clause.newBuilder()
                                    .setIpAddressExpression(
                                        IpAddressExpression.newBuilder().addIpAddresses("1.2.3.4")))
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_OR)))
            .setRuleScope(ruleScope)
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertSame(
        "Rules evaluated at INLINE_TRACING_AGENT cannot have OR clause operator.",
        status.getDescription());

    // valid request - no nested clause groups for inline tracing agent as 1 of the rule evaluation
    // points
    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("rule-name")
            .setDescription("rule-description")
            .setEffect(ruleEffect1)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .addClauses(
                                Clause.newBuilder()
                                    .setIpTypeExpression(
                                        IpTypeExpression.newBuilder()
                                            .addIpTypes(IpType.IP_TYPE_SCANNER)))
                            .addClauses(
                                Clause.newBuilder()
                                    .setIpAddressExpression(
                                        IpAddressExpression.newBuilder().addIpAddresses("1.2.3.4")))
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)))
            .setRuleScope(ruleScope)
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    // valid request - nested clause groups without INLINE_TRACING_AGENT as 1 of the rule evaluation
    // points
    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("rule-name")
            .setDescription("rule-description")
            .setEffect(ruleEffect2)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .addClauses(
                                Clause.newBuilder()
                                    .setIpTypeExpression(
                                        IpTypeExpression.newBuilder()
                                            .addIpTypes(IpType.IP_TYPE_SCANNER)))
                            .addClauses(
                                Clause.newBuilder()
                                    .setClauseGroup(
                                        ClauseGroup.newBuilder()
                                            .addClauses(
                                                Clause.newBuilder()
                                                    .setIpAddressExpression(
                                                        IpAddressExpression.newBuilder()
                                                            .addIpAddresses("1.2.3.4")))
                                            .addClauses(
                                                Clause.newBuilder()
                                                    .setMatchExpression(
                                                        MatchExpression.newBuilder()
                                                            .setMatchKey(
                                                                MatchKey.MATCH_KEY_HEADER_VALUE)
                                                            .setMatchOperator(
                                                                MatchOperator.MATCH_OPERATOR_EQUALS)
                                                            .setMatchValue("header-value")))
                                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_OR)))
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)))
            .setRuleScope(ruleScope)
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    // valid request - OR clause operator without inline tracing agent as 1 of the rule evaluation
    // points
    request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("rule-name")
            .setDescription("rule-description")
            .setEffect(ruleEffect2)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .addClauses(
                                Clause.newBuilder()
                                    .setIpTypeExpression(
                                        IpTypeExpression.newBuilder()
                                            .addIpTypes(IpType.IP_TYPE_SCANNER)))
                            .addClauses(
                                Clause.newBuilder()
                                    .setIpAddressExpression(
                                        IpAddressExpression.newBuilder().addIpAddresses("1.2.3.4")))
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_OR)))
            .setRuleScope(ruleScope)
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());
  }

  @Test
  void testUpdateRuleRequestWithRuleEvaluationPoints() {
    UpdateCustomSignatureRuleRequest request;
    Status status;
    RuleEffect ruleEffect1 = getRuleEffectWithInlineAgentRuleEvaluationPoint();
    RuleEffect ruleEffect2 = getRuleEffectWithoutInlineAgentRuleEvaluationPoint();
    RuleScope ruleScope = getRuleScope();

    // invalid request - nested clause group in a clause for inline tracing agent as 1 of the rule
    // evaluation points
    request =
        UpdateCustomSignatureRuleRequest.newBuilder()
            .setRule(
                CustomSignatureRule.newBuilder()
                    .setId("rule-id")
                    .setName("rule-name")
                    .setDescription("rule-description")
                    .setEffect(ruleEffect1)
                    .setDefinition(
                        RuleDefinition.newBuilder()
                            .setClauseGroup(
                                ClauseGroup.newBuilder()
                                    .addClauses(
                                        Clause.newBuilder()
                                            .setIpTypeExpression(
                                                IpTypeExpression.newBuilder()
                                                    .addIpTypes(IpType.IP_TYPE_SCANNER)))
                                    .addClauses(
                                        Clause.newBuilder()
                                            .setClauseGroup(
                                                ClauseGroup.newBuilder()
                                                    .setClauseOperator(
                                                        ClauseOperator.CLAUSE_OPERATOR_AND)
                                                    .addClauses(
                                                        Clause.newBuilder()
                                                            .setIpAddressExpression(
                                                                IpAddressExpression.newBuilder()
                                                                    .addIpAddresses("1.2.3.4")))))
                                    .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)))
                    .setRuleScope(ruleScope))
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertSame(
        "Rules evaluated at INLINE_TRACING_AGENT cannot have nested clauses.",
        status.getDescription());

    // invalid request - OR clause operator for inline tracing agent as 1 of the rule evaluation
    // points
    request =
        UpdateCustomSignatureRuleRequest.newBuilder()
            .setRule(
                CustomSignatureRule.newBuilder()
                    .setId("rule-id")
                    .setName("rule-name")
                    .setDescription("rule-description")
                    .setEffect(ruleEffect1)
                    .setDefinition(
                        RuleDefinition.newBuilder()
                            .setClauseGroup(
                                ClauseGroup.newBuilder()
                                    .addClauses(
                                        Clause.newBuilder()
                                            .setIpTypeExpression(
                                                IpTypeExpression.newBuilder()
                                                    .addIpTypes(IpType.IP_TYPE_SCANNER)))
                                    .addClauses(
                                        Clause.newBuilder()
                                            .setIpAddressExpression(
                                                IpAddressExpression.newBuilder()
                                                    .addIpAddresses("1.2.3.4")))
                                    .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_OR)))
                    .setRuleScope(ruleScope))
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertSame(
        "Rules evaluated at INLINE_TRACING_AGENT cannot have OR clause operator.",
        status.getDescription());

    // valid request - no nested clause groups for inline tracing agent as 1 of the rule evaluation
    // points
    request =
        UpdateCustomSignatureRuleRequest.newBuilder()
            .setRule(
                CustomSignatureRule.newBuilder()
                    .setId("rule-id")
                    .setName("rule-name")
                    .setDescription("rule-description")
                    .setEffect(ruleEffect1)
                    .setDefinition(
                        RuleDefinition.newBuilder()
                            .setClauseGroup(
                                ClauseGroup.newBuilder()
                                    .addClauses(
                                        Clause.newBuilder()
                                            .setIpTypeExpression(
                                                IpTypeExpression.newBuilder()
                                                    .addIpTypes(IpType.IP_TYPE_SCANNER)))
                                    .addClauses(
                                        Clause.newBuilder()
                                            .setIpAddressExpression(
                                                IpAddressExpression.newBuilder()
                                                    .addIpAddresses("1.2.3.4")))
                                    .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)))
                    .setRuleScope(ruleScope))
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    // valid request - nested clause groups without INLINE_TRACING_AGENT as 1 of the rule evaluation
    // points
    request =
        UpdateCustomSignatureRuleRequest.newBuilder()
            .setRule(
                CustomSignatureRule.newBuilder()
                    .setId("rule-id")
                    .setName("rule-name")
                    .setDescription("rule-description")
                    .setEffect(ruleEffect2)
                    .setDefinition(
                        RuleDefinition.newBuilder()
                            .setClauseGroup(
                                ClauseGroup.newBuilder()
                                    .addClauses(
                                        Clause.newBuilder()
                                            .setIpTypeExpression(
                                                IpTypeExpression.newBuilder()
                                                    .addIpTypes(IpType.IP_TYPE_SCANNER)))
                                    .addClauses(
                                        Clause.newBuilder()
                                            .setClauseGroup(
                                                ClauseGroup.newBuilder()
                                                    .addClauses(
                                                        Clause.newBuilder()
                                                            .setIpAddressExpression(
                                                                IpAddressExpression.newBuilder()
                                                                    .addIpAddresses("1.2.3.4")))
                                                    .addClauses(
                                                        Clause.newBuilder()
                                                            .setMatchExpression(
                                                                MatchExpression.newBuilder()
                                                                    .setMatchKey(
                                                                        MatchKey
                                                                            .MATCH_KEY_HEADER_VALUE)
                                                                    .setMatchOperator(
                                                                        MatchOperator
                                                                            .MATCH_OPERATOR_EQUALS)
                                                                    .setMatchValue("header-value")))
                                                    .setClauseOperator(
                                                        ClauseOperator.CLAUSE_OPERATOR_OR)))
                                    .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)))
                    .setRuleScope(ruleScope))
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());

    // valid request - OR clause operator without inline tracing agent as 1 of the rule evaluation
    // points
    request =
        UpdateCustomSignatureRuleRequest.newBuilder()
            .setRule(
                CustomSignatureRule.newBuilder()
                    .setId("rule-id")
                    .setName("rule-name")
                    .setDescription("rule-description")
                    .setEffect(ruleEffect2)
                    .setDefinition(
                        RuleDefinition.newBuilder()
                            .setClauseGroup(
                                ClauseGroup.newBuilder()
                                    .addClauses(
                                        Clause.newBuilder()
                                            .setIpTypeExpression(
                                                IpTypeExpression.newBuilder()
                                                    .addIpTypes(IpType.IP_TYPE_SCANNER)))
                                    .addClauses(
                                        Clause.newBuilder()
                                            .setIpAddressExpression(
                                                IpAddressExpression.newBuilder()
                                                    .addIpAddresses("1.2.3.4")))
                                    .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_OR)))
                    .setRuleScope(ruleScope))
            .build();
    status = rulesValidator.validate(request);
    assertEquals(Code.OK, status.getCode());
  }

  private RuleScope getRuleScope() {
    return RuleScope.newBuilder()
        .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env"))
        .build();
  }

  private RuleEffect getRuleEffectWithInlineAgentRuleEvaluationPoint() {
    return RuleEffect.newBuilder()
        .setEventType(EventType.EVENT_TYPE_ALLOW)
        .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
        .addAllRuleEvaluationPoints(
            List.of(
                RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT,
                RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM))
        .build();
  }

  private RuleEffect getRuleEffectWithoutInlineAgentRuleEvaluationPoint() {
    return RuleEffect.newBuilder()
        .setEventType(EventType.EVENT_TYPE_ALLOW)
        .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
        .addAllRuleEvaluationPoints(
            List.of(
                RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE,
                RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM))
        .build();
  }

  private RuleDefinition getRuleDefinitionForAggregateConfigParams(
      MatchKey matchKey,
      MatchOperator matchOperator,
      String matchValue,
      MatchCategory matchCategory) {
    return RuleDefinition.newBuilder()
        .setClauseGroup(
            ClauseGroup.newBuilder()
                .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                .addClauses(
                    Clause.newBuilder()
                        .setMatchExpression(
                            MatchExpression.newBuilder()
                                .setMatchKey(matchKey)
                                .setMatchOperator(matchOperator)
                                .setMatchValue(matchValue)
                                .setMatchCategory(matchCategory))))
        .build();
  }

  private MatchExpression getMatchExpression(
      MatchKey matchKey, MatchOperator matchOperator, String strValue) {
    return MatchExpression.newBuilder()
        .setMatchKey(matchKey)
        .setMatchOperator(matchOperator)
        .setMatchCategory(MatchCategory.MATCH_CATEGORY_REQUEST)
        .setValue(Value.newBuilder().setStringValue(strValue))
        .build();
  }

  private void assertInvalidArgument(Status status, String expectedDescription) {
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    assertNotNull(status.getDescription());
    assertTrue(status.getDescription().contains(expectedDescription));
  }
}
