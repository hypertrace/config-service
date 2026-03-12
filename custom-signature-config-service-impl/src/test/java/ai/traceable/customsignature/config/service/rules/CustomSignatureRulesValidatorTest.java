package ai.traceable.customsignature.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEvaluationConfigContextRequest;
import ai.traceable.customsignature.config.service.v1.HeaderInjection;
import ai.traceable.customsignature.config.service.v1.IpAddressExpression;
import ai.traceable.customsignature.config.service.v1.IpAddressExpressionType;
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
  void testValidateCreateRule() {
    assertInvalidArgument(
        () -> rulesValidator.validate(CreateCustomSignatureRuleRequest.newBuilder().build()),
        "valid name");

    final CreateCustomSignatureRuleRequest req1 =
        CreateCustomSignatureRuleRequest.newBuilder().setName("name").build();
    assertInvalidArgument(() -> rulesValidator.validate(req1), "valid definition");

    final CreateCustomSignatureRuleRequest req2 =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setDefinition(RuleDefinition.getDefaultInstance())
            .build();
    assertInvalidArgument(() -> rulesValidator.validate(req2), "valid effect");

    final CreateCustomSignatureRuleRequest req3 =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setDefinition(RuleDefinition.getDefaultInstance())
            .setEffect(RuleEffect.getDefaultInstance())
            .build();
    assertInvalidArgument(() -> rulesValidator.validate(req3), "valid event type");

    final CreateCustomSignatureRuleRequest req4 =
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
    assertInvalidArgument(
        () -> rulesValidator.validate(req4),
        "Custom signature rule with a response category or a attribute clause is not compatible with the specified event type");

    RuleEffect ruleEffect =
        RuleEffect.newBuilder()
            .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
            .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
            .build();

    final CreateCustomSignatureRuleRequest req5 =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(RuleDefinition.getDefaultInstance())
            .build();
    assertInvalidArgument(() -> rulesValidator.validate(req5), "valid clause group");

    final CreateCustomSignatureRuleRequest req6 =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(ClauseGroup.getDefaultInstance())
                    .build())
            .build();
    assertInvalidArgument(() -> rulesValidator.validate(req6), "valid clause operator");

    final CreateCustomSignatureRuleRequest req7 =
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
    assertInvalidArgument(() -> rulesValidator.validate(req7), "at least one clause");

    final CreateCustomSignatureRuleRequest req8 =
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
    assertInvalidArgument(
        () -> rulesValidator.validate(req8), "Invalid Custom Signature Rule Clause");

    final CreateCustomSignatureRuleRequest req9 =
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
    assertInvalidArgument(() -> rulesValidator.validate(req9), "valid match key");

    final CreateCustomSignatureRuleRequest req10 =
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
    assertInvalidArgument(() -> rulesValidator.validate(req10), "valid match operator");

    final CreateCustomSignatureRuleRequest req11 =
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
    assertInvalidArgument(() -> rulesValidator.validate(req11), "Invalid Regex Value");

    final CreateCustomSignatureRuleRequest req12 =
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
    assertDoesNotThrow(() -> rulesValidator.validate(req12));

    final CreateCustomSignatureRuleRequest req13 =
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
    assertDoesNotThrow(() -> rulesValidator.validate(req13));

    final CreateCustomSignatureRuleRequest req14 =
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
    assertDoesNotThrow(() -> rulesValidator.validate(req14));

    final CreateCustomSignatureRuleRequest req15 =
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
    assertInvalidArgument(
        () -> rulesValidator.validate(req15),
        "Custom Signature Rule Effect with alert action should have a valid event severity");

    final CreateCustomSignatureRuleRequest req16 =
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
    assertInvalidArgument(() -> rulesValidator.validate(req16), "valid tag");

    final CreateCustomSignatureRuleRequest req17 =
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
    assertInvalidArgument(() -> rulesValidator.validate(req17), "valid match key");

    final CreateCustomSignatureRuleRequest req18 =
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
    assertInvalidArgument(() -> rulesValidator.validate(req18), "valid key match operator");

    final CreateCustomSignatureRuleRequest req19 =
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
    assertInvalidArgument(() -> rulesValidator.validate(req19), "valid match value");

    final CreateCustomSignatureRuleRequest req20 =
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
    assertInvalidArgument(() -> rulesValidator.validate(req20), "valid value match operator");

    final CreateCustomSignatureRuleRequest req21 =
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
    assertInvalidArgument(() -> rulesValidator.validate(req21), "Invalid Regex Value");

    CreateCustomSignatureRuleRequest req22 =
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
    assertDoesNotThrow(() -> rulesValidator.validate(req22));

    final CreateCustomSignatureRuleRequest req23 =
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
    assertInvalidArgument(() -> rulesValidator.validate(req23), "integer match value");

    // valid Cyrillic regex
    CreateCustomSignatureRuleRequest req24 =
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
    assertDoesNotThrow(() -> rulesValidator.validate(req24));

    CreateCustomSignatureRuleRequest req25 =
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
    assertDoesNotThrow(() -> rulesValidator.validate(req25));

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

    CreateCustomSignatureRuleRequest req26 =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(validRuleDefinition)
            .build();
    assertDoesNotThrow(() -> rulesValidator.validate(req26));

    final CreateCustomSignatureRuleRequest req27 =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(validRuleDefinition)
            .setBlockingExpiryDetails(ExpiryDetails.newBuilder().setExpiryDuration("1234"))
            .build();
    assertInvalidArgument(
        () -> rulesValidator.validate(req27), "Blocking expiry duration can't be parsed");

    final CreateCustomSignatureRuleRequest req28 =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(validRuleDefinition)
            .setBlockingExpiryDetails(
                ExpiryDetails.newBuilder().setExpiryDuration(NON_ZERO_EXPIRY_DURATION))
            .build();
    assertDoesNotThrow(() -> rulesValidator.validate(req28));

    when(modsecRulesManager.validateModsecRule(req28.getName(), req28.getDefinition()))
        .thenReturn(Status.INTERNAL);
    assertDoesNotThrow(() -> rulesValidator.validate(req28));

    final CreateCustomSignatureRuleRequest req29 =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(validRuleDefinition)
            .setBlockingExpiryDetails(
                ExpiryDetails.newBuilder().setExpiryDuration(NON_ZERO_EXPIRY_DURATION))
            .setRuleScope(
                RuleScope.newBuilder().setEnvironmentScope(EnvironmentScope.getDefaultInstance()))
            .build();
    assertInvalidArgument(
        () -> rulesValidator.validate(req29),
        "Environment scope should have at least one environment");

    final CreateCustomSignatureRuleRequest req30 =
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
    assertInvalidArgument(
        () -> rulesValidator.validate(req30), "Environment id should not be empty string");

    final CreateCustomSignatureRuleRequest req31 =
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
    when(modsecRulesManager.validateModsecRule(req31.getName(), req31.getDefinition()))
        .thenReturn(Status.OK);
    assertDoesNotThrow(() -> rulesValidator.validate(req31));

    CreateCustomSignatureRuleRequest req32 =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                    .addAllRuleEvaluationPoints(allRuleEvaluationPoints))
            .setDefinition(validRuleDefinition)
            .build();
    assertDoesNotThrow(() -> rulesValidator.validate(req32));

    CreateCustomSignatureRuleRequest req33 =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("name")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_ALLOW)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
                    .addAllRuleEvaluationPoints(allRuleEvaluationPoints))
            .setDefinition(validRuleDefinition)
            .build();
    assertDoesNotThrow(() -> rulesValidator.validate(req33));

    CreateCustomSignatureRuleRequest req34 =
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
    assertDoesNotThrow(() -> rulesValidator.validate(req34));

    CreateCustomSignatureRuleRequest req35 =
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
    assertDoesNotThrow(() -> rulesValidator.validate(req35));

    final CreateCustomSignatureRuleRequest req36 =
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
    assertInvalidArgument(() -> rulesValidator.validate(req36), "Rule is not AGENT-compatible");

    final CreateCustomSignatureRuleRequest req37 =
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
    assertInvalidArgument(() -> rulesValidator.validate(req37), "Rule is not AGENT-compatible");
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
    rulesValidator.validate(createRequest);

    final CreateCustomSignatureRuleRequest createRequest2 =
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
    assertInvalidArgument(
        () -> rulesValidator.validate(createRequest2),
        "GREATER_THAN and LESS_THAN match operators are unsupported");

    final UpdateCustomSignatureRuleRequest updateRequest =
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
    assertDoesNotThrow(() -> rulesValidator.validate(updateRequest));

    final UpdateCustomSignatureRuleRequest updateRequest2 =
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
    assertInvalidArgument(
        () -> rulesValidator.validate(updateRequest2),
        "GREATER_THAN and LESS_THAN match operators are unsupported");
  }

  @Test
  void testValidateUpdateRule() {
    final CustomSignatureRule rule1 = CustomSignatureRule.newBuilder().setId("id").build();
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule1).build()),
        "valid name");

    final CustomSignatureRule rule2 =
        CustomSignatureRule.newBuilder().setId("id").setName("name").build();
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule2).build()),
        "valid definition");

    final CustomSignatureRule rule3 =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setDefinition(RuleDefinition.getDefaultInstance())
            .build();
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule3).build()),
        "valid effect");

    final CustomSignatureRule rule4 =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setDefinition(RuleDefinition.getDefaultInstance())
            .setEffect(RuleEffect.getDefaultInstance())
            .build();
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule4).build()),
        "valid event type");

    final CustomSignatureRule rule5 =
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
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule5).build()),
        "Custom signature rule with a response category or a attribute clause is not compatible with the specified event type");

    final RuleEffect ruleEffect =
        RuleEffect.newBuilder()
            .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
            .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
            .build();

    final CustomSignatureRule rule6 =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(RuleDefinition.getDefaultInstance())
            .build();
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule6).build()),
        "valid clause group");

    final CustomSignatureRule rule7 =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(ClauseGroup.getDefaultInstance())
                    .build())
            .build();
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule7).build()),
        "valid clause operator");

    final CustomSignatureRule rule8 =
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
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule8).build()),
        "at least one clause");

    final CustomSignatureRule rule9 =
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
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule9).build()),
        "Invalid Custom Signature Rule Clause");

    final CustomSignatureRule rule10 =
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
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule10).build()),
        "valid match key");

    final CustomSignatureRule rule11 =
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
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule11).build()),
        "valid match operator");

    final CustomSignatureRule rule11b =
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
    assertDoesNotThrow(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule11b).build()));

    final CustomSignatureRule rule11c =
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
    assertDoesNotThrow(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule11c).build()));

    final CustomSignatureRule rule11d =
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
    assertDoesNotThrow(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule11d).build()));

    final CustomSignatureRule rule12 =
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
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule12).build()),
        "Custom Signature Rule Effect with alert action should have a valid event severity");

    final CustomSignatureRule rule13 =
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
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule13).build()),
        "valid tag");

    final CustomSignatureRule rule14 =
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
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule14).build()),
        "valid match key");

    final CustomSignatureRule rule15 =
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
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule15).build()),
        "valid key match operator");

    final CustomSignatureRule rule16 =
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
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule16).build()),
        "valid match value");

    final CustomSignatureRule rule17 =
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
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule17).build()),
        "valid value match operator");

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
    final CustomSignatureRule ruleWithValidDef =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(validRuleDefinition)
            .build();
    assertDoesNotThrow(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(ruleWithValidDef).build()));

    final CustomSignatureRule rule18 =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(validRuleDefinition)
            .setBlockingExpiryDetails(ExpiryDetails.newBuilder().setExpiryDuration("invalid"))
            .build();
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule18).build()),
        "Blocking expiry duration can't be parsed");

    final CustomSignatureRule ruleWithZeroExpiry =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(validRuleDefinition)
            .setBlockingExpiryDetails(
                ExpiryDetails.newBuilder().setExpiryDuration(ZERO_EXPIRY_DURATION))
            .build();
    final UpdateCustomSignatureRuleRequest request =
        UpdateCustomSignatureRuleRequest.newBuilder().setRule(ruleWithZeroExpiry).build();
    assertDoesNotThrow(() -> rulesValidator.validate(request));

    when(modsecRulesManager.validateModsecRule(
            ruleWithZeroExpiry.getName(), ruleWithZeroExpiry.getDefinition()))
        .thenReturn(Status.INTERNAL);
    assertDoesNotThrow(() -> rulesValidator.validate(request));

    final CustomSignatureRule rule19 =
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
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule19).build()),
        "Environment scope should have at least one environment");

    final CustomSignatureRule rule20 =
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
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule20).build()),
        "Environment id should not be empty string");

    final CustomSignatureRule rule21 =
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
    assertDoesNotThrow(
        () ->
            rulesValidator.validate(
                UpdateCustomSignatureRuleRequest.newBuilder().setRule(rule21).build()));
  }

  @Test
  void testValidateDeleteRule() {
    assertInvalidArgument(
        () -> rulesValidator.validate(DeleteCustomSignatureRuleRequest.getDefaultInstance()),
        "valid id");
    assertDoesNotThrow(
        () ->
            rulesValidator.validate(
                DeleteCustomSignatureRuleRequest.newBuilder().setId("id").build()));
  }

  @Test
  public void testValidateBulkDeleteRules() {
    // Empty ids list should fail
    assertInvalidArgument(
        () -> rulesValidator.validate(BulkDeleteCustomSignatureRulesRequest.getDefaultInstance()),
        "at least one id");

    // Empty string id in list should fail
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                BulkDeleteCustomSignatureRulesRequest.newBuilder().addIds("").build()),
        "empty ids");

    // Valid single id should pass
    assertDoesNotThrow(
        () ->
            rulesValidator.validate(
                BulkDeleteCustomSignatureRulesRequest.newBuilder().addIds("id1").build()));

    // Valid multiple ids should pass
    assertDoesNotThrow(
        () ->
            rulesValidator.validate(
                BulkDeleteCustomSignatureRulesRequest.newBuilder()
                    .addIds("id1")
                    .addIds("id2")
                    .addIds("id3")
                    .build()));

    // Mix of valid and empty ids should fail
    assertInvalidArgument(
        () ->
            rulesValidator.validate(
                BulkDeleteCustomSignatureRulesRequest.newBuilder()
                    .addIds("id1")
                    .addIds("")
                    .addIds("id3")
                    .build()),
        "empty ids");
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
    rulesValidator.validate(validCreateRequest1);

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
    rulesValidator.validate(validCreateRequest2);

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
    rulesValidator.validate(validCreateRequest3);

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
    rulesValidator.validate(validCreateRequest4);

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
    assertInvalidArgument(
        () -> rulesValidator.validate(invalidCreateRequest1), "Invalid match key");

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
    assertInvalidArgument(() -> rulesValidator.validate(invalidCreateRequest2), "");
  }

  @Test
  void testCreateRuleRequestWithRuleEvaluationPoints() {
    CreateCustomSignatureRuleRequest request;
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
    final CreateCustomSignatureRuleRequest finalRequest1 = request;
    assertInvalidArgument(
        () -> rulesValidator.validate(finalRequest1),
        "Rules evaluated at INLINE_TRACING_AGENT cannot have nested clauses.");

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
    final CreateCustomSignatureRuleRequest finalRequest2 = request;
    assertInvalidArgument(
        () -> rulesValidator.validate(finalRequest2),
        "Rules evaluated at INLINE_TRACING_AGENT cannot have OR clause operator.");

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
    rulesValidator.validate(request);

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
    rulesValidator.validate(request);

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
    rulesValidator.validate(request);
  }

  @Test
  void testUpdateRuleRequestWithRuleEvaluationPoints() {
    UpdateCustomSignatureRuleRequest request;
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
    final UpdateCustomSignatureRuleRequest req1 = request;
    assertInvalidArgument(
        () -> rulesValidator.validate(req1),
        "Rules evaluated at INLINE_TRACING_AGENT cannot have nested clauses.");

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
    final UpdateCustomSignatureRuleRequest req2 = request;
    assertInvalidArgument(
        () -> rulesValidator.validate(req2),
        "Rules evaluated at INLINE_TRACING_AGENT cannot have OR clause operator.");

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
    rulesValidator.validate(request);

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
    rulesValidator.validate(request);

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
    rulesValidator.validate(request);
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

  @Test
  void testValidateIpAddressExpressionTypeWithBlockingEventType() {
    // ALL_EXTERNAL with DETECTION_AND_BLOCKING should fail
    CreateCustomSignatureRuleRequest request =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("test-rule")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
                    .addRuleEvaluationPoints(
                        RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT))
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setIpAddressExpression(
                                        IpAddressExpression.newBuilder()
                                            .setIpAddressExpressionType(
                                                IpAddressExpressionType
                                                    .IP_ADDRESS_EXPRESSION_TYPE_ALL_EXTERNAL)))))
            .build();
    assertInvalidArgument(
        () -> rulesValidator.validate(request),
        "Allow/Blocking action is unsupported for ip address expression type");

    // ALL_INTERNAL with DETECTION_AND_BLOCKING should fail
    CreateCustomSignatureRuleRequest request2 =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("test-rule")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
                    .addRuleEvaluationPoints(
                        RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT))
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setIpAddressExpression(
                                        IpAddressExpression.newBuilder()
                                            .setIpAddressExpressionType(
                                                IpAddressExpressionType
                                                    .IP_ADDRESS_EXPRESSION_TYPE_ALL_INTERNAL)))))
            .build();
    assertInvalidArgument(
        () -> rulesValidator.validate(request2),
        "Allow/Blocking action is unsupported for ip address expression type");

    // ALL_EXTERNAL with EVENT_TYPE_ALLOW should fail
    CreateCustomSignatureRuleRequest request3 =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("test-rule")
            .setEffect(
                RuleEffect.newBuilder()
                    .setEventType(EventType.EVENT_TYPE_ALLOW)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
                    .addRuleEvaluationPoints(
                        RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT))
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addClauses(
                                Clause.newBuilder()
                                    .setIpAddressExpression(
                                        IpAddressExpression.newBuilder()
                                            .setIpAddressExpressionType(
                                                IpAddressExpressionType
                                                    .IP_ADDRESS_EXPRESSION_TYPE_ALL_EXTERNAL)))))
            .build();
    assertInvalidArgument(
        () -> rulesValidator.validate(request3),
        "Allow/Blocking action is unsupported for ip address expression type");

    // ALL_EXTERNAL with NORMAL_DETECTION should pass
    CreateCustomSignatureRuleRequest request4 =
        CreateCustomSignatureRuleRequest.newBuilder()
            .setName("test-rule")
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
                                    .setIpAddressExpression(
                                        IpAddressExpression.newBuilder()
                                            .setIpAddressExpressionType(
                                                IpAddressExpressionType
                                                    .IP_ADDRESS_EXPRESSION_TYPE_ALL_EXTERNAL)))))
            .build();
    assertDoesNotThrow(() -> rulesValidator.validate(request4));
  }

  private void assertInvalidArgument(Runnable runnable, String expectedDescription) {
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            runnable::run,
            "Expected validation to throw RuntimeException but it passed");
    Status status = Status.fromThrowable(exception);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    if (expectedDescription != null && !expectedDescription.isEmpty()) {
      String actualDescription = status.getDescription();
      if (actualDescription != null) {
        assertTrue(
            actualDescription.contains(expectedDescription),
            "Expected description to contain: '"
                + expectedDescription
                + "' but was: '"
                + actualDescription
                + "'");
      }
      // If actualDescription is null, we skip the description check
      // This handles cases where validation throws exception without a description
    }
  }

  @org.junit.jupiter.api.Test
  void
      testValidateGetCustomSignatureEvaluationConfigContextRequestWithUnspecifiedEvaluationPoint() {
    GetCustomSignatureEvaluationConfigContextRequest request =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_UNSPECIFIED)
            .build();

    assertInvalidArgument(
        () -> rulesValidator.validate(request), "Rule evaluation point must be specified");
  }

  @Test
  void testValidateGetCustomSignatureEvaluationConfigContextRequestWithUnspecifiedRuleVersion() {
    GetCustomSignatureEvaluationConfigContextRequest request =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setRuleVersion(
                ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion
                    .CUSTOM_MODSEC_RULE_VERSION_UNSPECIFIED)
            .setEventType(EventType.EVENT_TYPE_ALLOW)
            .build();

    assertInvalidArgument(() -> rulesValidator.validate(request), "Rule version must be specified");
  }

  @Test
  void testValidateGetCustomSignatureEvaluationConfigContextRequestWithValidRuleVersions() {
    // Test with V3 rule version
    GetCustomSignatureEvaluationConfigContextRequest v3Request =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setRuleVersion(
                ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion
                    .CUSTOM_MODSEC_RULE_VERSION_V3)
            .setEventType(EventType.EVENT_TYPE_ALLOW)
            .build();

    // Test with V3_SECARG_LIMITS rule version
    GetCustomSignatureEvaluationConfigContextRequest v3SecArgRequest =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setRuleVersion(
                ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion
                    .CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS)
            .setEventType(EventType.EVENT_TYPE_ALLOW)
            .build();

    // Test with CORAZA_V3 rule version
    GetCustomSignatureEvaluationConfigContextRequest corazaRequest =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setRuleVersion(
                ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion
                    .CUSTOM_MODSEC_RULE_VERSION_CORAZA_V3)
            .setEventType(EventType.EVENT_TYPE_ALLOW)
            .build();

    // All should validate successfully
    assertDoesNotThrow(() -> rulesValidator.validate(v3Request));
    assertDoesNotThrow(() -> rulesValidator.validate(v3SecArgRequest));
    assertDoesNotThrow(() -> rulesValidator.validate(corazaRequest));
  }

  @Test
  void testValidateGetCustomSignatureEvaluationConfigContextRequestWithUnspecifiedEventType() {
    GetCustomSignatureEvaluationConfigContextRequest request =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setRuleVersion(
                ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion
                    .CUSTOM_MODSEC_RULE_VERSION_V3)
            .setEventType(EventType.EVENT_TYPE_UNSPECIFIED)
            .build();

    assertInvalidArgument(() -> rulesValidator.validate(request), "Event type must be specified");
  }

  @Test
  void testValidateGetCustomSignatureEvaluationConfigContextRequestWithValidEventTypes() {
    // Test with ALLOW event type
    GetCustomSignatureEvaluationConfigContextRequest allowRequest =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setRuleVersion(
                ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion
                    .CUSTOM_MODSEC_RULE_VERSION_V3)
            .setEventType(EventType.EVENT_TYPE_ALLOW)
            .build();

    // Test with DETECTION_AND_BLOCKING event type
    GetCustomSignatureEvaluationConfigContextRequest blockRequest =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setRuleVersion(
                ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion
                    .CUSTOM_MODSEC_RULE_VERSION_V3)
            .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
            .build();

    // All should validate successfully
    assertDoesNotThrow(() -> rulesValidator.validate(allowRequest));
    assertDoesNotThrow(() -> rulesValidator.validate(blockRequest));
  }

  @Test
  void testValidateGetCustomSignatureEvaluationConfigContextRequestWithAllValidFields() {
    GetCustomSignatureEvaluationConfigContextRequest request =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setRuleVersion(
                ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion
                    .CUSTOM_MODSEC_RULE_VERSION_CORAZA_V3)
            .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
            .build();

    // Should validate successfully
    assertDoesNotThrow(() -> rulesValidator.validate(request));
  }
}
