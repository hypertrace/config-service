package ai.traceable.customsignature.config.service.modsec;

import static ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesTarget;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.customsignature.config.service.modsec.directives.ModsecDirectivesManager;
import ai.traceable.customsignature.config.service.v1.AttributeKeyValueExpression;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EmailDomainExpression;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.EventSeverity;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.ExpiryDetails;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.IpAbuseVelocityExpression;
import ai.traceable.customsignature.config.service.v1.IpAddressExpression;
import ai.traceable.customsignature.config.service.v1.IpAsnExpression;
import ai.traceable.customsignature.config.service.v1.IpConnectionTypeExpression;
import ai.traceable.customsignature.config.service.v1.IpOrganisationExpression;
import ai.traceable.customsignature.config.service.v1.IpReputationExpression;
import ai.traceable.customsignature.config.service.v1.IpTypeExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RegionExpression;
import ai.traceable.customsignature.config.service.v1.RequestScannerTypeExpression;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleEvaluationPoint;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.customsignature.config.service.v1.UserAgentExpression;
import ai.traceable.customsignature.config.service.v1.UserIdExpression;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider;
import ai.traceable.modsecurity.rule.conversion.ModsecRuleConverterImpl;
import ai.traceable.modsecurity.rule.conversion.clause.CustomModsecKeyValueMatchClauseConverter;
import ai.traceable.modsecurity.rule.conversion.clause.CustomModsecValueMatchClauseConverter;
import ai.traceable.modsecurity.rule.conversion.clause.ModsecOperatorConverter;
import ai.traceable.modsecurity.rule.conversion.clause.ModsecVariableConverter;
import ai.traceable.modsecurity.utils.ModsecRuleEngineUtils;
import com.github.f4b6a3.uuid.UuidCreator;
import com.google.common.io.Resources;
import com.google.protobuf.Value;
import io.grpc.Status;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.SystemUtils;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;

class CustomSignatureModsecRulesManagerTest {

  private static final int EXPIRY_TIMESTAMP_MILLIS = 12345678;
  private static final String EXPIRY_DURATION = "P3M";
  private static final String TENANT_ID = "id";
  private static final CustomSignatureRule ruleWithModsecConvertibleClause =
      CustomSignatureRule.newBuilder()
          .setDefinition(
              RuleDefinition.newBuilder()
                  .setClauseGroup(
                      ClauseGroup.newBuilder()
                          .addClauses(
                              Clause.newBuilder()
                                  .setMatchExpression(
                                      MatchExpression.newBuilder()
                                          .setMatchKey(MatchKey.MATCH_KEY_URL)
                                          .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                          .setValue(Value.newBuilder().setStringValue("url"))))))
          .build();
  @Mock private ModsecRulesRegistry mockModsecRulesRegistry;
  @Mock private ModsecBlobValidator mockModsecBlobValidator;
  @Mock private CachedServiceMappingProvider mockCachedServiceMappingProvider;

  @BeforeEach
  void setup() {
    mockModsecRulesRegistry = mock(ModsecRulesRegistry.class);
    when(mockModsecRulesRegistry.getModsecHeader(
            MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE))
        .thenReturn("SecRuleEngine DetectionOnly");
    mockModsecBlobValidator = mock(ModsecBlobValidator.class);
    mockCachedServiceMappingProvider = mock(CachedServiceMappingProvider.class);
  }

  @Test
  public void testConvertRulesException() throws Exception {
    ModsecDirectivesManager mockDirectivesManager = mock(ModsecDirectivesManager.class);
    when(mockDirectivesManager.getModsecHeader(ModsecRuleVersion.MODSEC_RULE_VERSION_V3))
        .thenReturn("");

    CustomModsecRuleConverter customModsecRuleConverter = mock(CustomModsecRuleConverter.class);
    CustomSignatureModsecRulesManager modsecRulesManager =
        new CustomSignatureModsecRulesManager(
            customModsecRuleConverter,
            mockDirectivesManager,
            mockModsecRulesRegistry,
            mockModsecBlobValidator,
            mockCachedServiceMappingProvider);

    GetCustomSignatureModsecRulesResponse response =
        modsecRulesManager.getModsecRules(
            RequestContext.forTenantId(TENANT_ID),
            List.of(ruleWithModsecConvertibleClause),
            CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_UNSPECIFIED,
            false,
            ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TA_BLOCKING,
            List.of());
    assertTrue(response.getModsecRulesBlob().isEmpty());
    assertTrue(response.getInlineRulesList().isEmpty());

    response =
        modsecRulesManager.getModsecRules(
            RequestContext.forTenantId(TENANT_ID),
            List.of(ruleWithModsecConvertibleClause),
            CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3,
            false,
            ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TA_BLOCKING,
            List.of());
    assertTrue(response.getModsecRulesBlob().isEmpty());
    assertTrue(response.getInlineRulesList().isEmpty());

    when(customModsecRuleConverter.getValidatedModsecRule(
            anyLong(), anyString(), anyString(), anyList()))
        .thenThrow(new UnsupportedOperationException());
    response =
        modsecRulesManager.getModsecRules(
            RequestContext.forTenantId(TENANT_ID),
            List.of(ruleWithModsecConvertibleClause),
            CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3,
            false,
            ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TA_BLOCKING,
            List.of());
    assertTrue(response.getModsecRulesBlob().isEmpty());
    assertTrue(response.getInlineRulesList().isEmpty());
  }

  @Test
  public void testConversionNotSupported() {
    ClauseGroup clauseGroup =
        ClauseGroup.newBuilder()
            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
            .addClauses(
                Clause.newBuilder()
                    .setKeyValueExpression(
                        KeyValueExpression.newBuilder().setTag(KeyValueTag.KEY_VALUE_TAG_HEADER)))
            .addClauses(
                Clause.newBuilder()
                    .setMatchExpression(
                        MatchExpression.newBuilder().setMatchKey(MatchKey.MATCH_KEY_BODY)))
            .build();

    assertTrue(ModsecRulesSupportChecker.isInlineRuleMappingSupported(clauseGroup));
    assertFalse(
        ModsecRulesSupportChecker.isInlineRuleMappingSupported(
            clauseGroup.toBuilder()
                .addClauses(
                    Clause.newBuilder()
                        .setAttributeKeyValueExpression(
                            AttributeKeyValueExpression.getDefaultInstance()))
                .build()));
    assertFalse(
        ModsecRulesSupportChecker.isInlineRuleMappingSupported(
            clauseGroup.toBuilder()
                .addClauses(
                    Clause.newBuilder()
                        .setKeyValueExpression(
                            KeyValueExpression.newBuilder()
                                .setMatchCategory(MatchCategory.MATCH_CATEGORY_RESPONSE)
                                .setTag(KeyValueTag.KEY_VALUE_TAG_COOKIE)))
                .build()));
    assertFalse(
        ModsecRulesSupportChecker.isInlineRuleMappingSupported(
            clauseGroup.toBuilder()
                .addClauses(
                    Clause.newBuilder()
                        .setMatchExpression(
                            MatchExpression.newBuilder()
                                .setMatchCategory(MatchCategory.MATCH_CATEGORY_RESPONSE)
                                .setMatchKey(MatchKey.MATCH_KEY_COOKIE_VALUE)))
                .build()));
    assertTrue(
        ModsecRulesSupportChecker.isInlineRuleMappingSupported(
            clauseGroup.toBuilder()
                .addClauses(
                    Clause.newBuilder()
                        .setIpAddressExpression(IpAddressExpression.getDefaultInstance()))
                .build()));
    assertTrue(
        ModsecRulesSupportChecker.isInlineRuleMappingSupported(
            clauseGroup.toBuilder()
                .addClauses(
                    Clause.newBuilder().setIpTypeExpression(IpTypeExpression.getDefaultInstance()))
                .build()));
    assertFalse(
        ModsecRulesSupportChecker.isInlineRuleMappingSupported(
            clauseGroup.toBuilder()
                .addClauses(
                    Clause.newBuilder()
                        .setIpReputationExpression(IpReputationExpression.getDefaultInstance()))
                .build()));
    assertFalse(
        ModsecRulesSupportChecker.isInlineRuleMappingSupported(
            clauseGroup.toBuilder()
                .addClauses(
                    Clause.newBuilder()
                        .setIpConnectionTypeExpression(
                            IpConnectionTypeExpression.getDefaultInstance()))
                .build()));
    assertFalse(
        ModsecRulesSupportChecker.isInlineRuleMappingSupported(
            clauseGroup.toBuilder()
                .addClauses(
                    Clause.newBuilder()
                        .setIpOrganisationExpression(IpOrganisationExpression.getDefaultInstance()))
                .build()));
    assertFalse(
        ModsecRulesSupportChecker.isInlineRuleMappingSupported(
            clauseGroup.toBuilder()
                .addClauses(
                    Clause.newBuilder().setIpAsnExpression(IpAsnExpression.getDefaultInstance()))
                .build()));
    assertFalse(
        ModsecRulesSupportChecker.isInlineRuleMappingSupported(
            clauseGroup.toBuilder()
                .addClauses(
                    Clause.newBuilder()
                        .setIpAbuseVelocityExpression(
                            IpAbuseVelocityExpression.getDefaultInstance()))
                .build()));
    assertTrue(
        ModsecRulesSupportChecker.isInlineRuleMappingSupported(
            clauseGroup.toBuilder()
                .addClauses(
                    Clause.newBuilder().setRegionExpression(RegionExpression.getDefaultInstance()))
                .build()));
    assertFalse(
        ModsecRulesSupportChecker.isInlineRuleMappingSupported(
            clauseGroup.toBuilder()
                .addClauses(
                    Clause.newBuilder().setUserIdExpression(UserIdExpression.getDefaultInstance()))
                .build()));
    assertFalse(
        ModsecRulesSupportChecker.isInlineRuleMappingSupported(
            clauseGroup.toBuilder()
                .addClauses(
                    Clause.newBuilder()
                        .setEmailDomainExpression(EmailDomainExpression.getDefaultInstance()))
                .build()));
    assertFalse(
        ModsecRulesSupportChecker.isInlineRuleMappingSupported(
            clauseGroup.toBuilder()
                .addClauses(
                    Clause.newBuilder()
                        .setUserAgentExpression(UserAgentExpression.getDefaultInstance()))
                .build()));
    assertFalse(
        ModsecRulesSupportChecker.isInlineRuleMappingSupported(
            clauseGroup.toBuilder()
                .addClauses(
                    Clause.newBuilder()
                        .setRequestScannerTypeExpression(
                            RequestScannerTypeExpression.getDefaultInstance()))
                .build()));
  }

  @Test
  public void testConvertRules() throws IOException {
    try (MockedStatic<ModsecRuleEngineUtils> mockModsecUtils =
        mockStatic(ModsecRuleEngineUtils.class)) {
      mockModsecUtils
          .when(() -> ModsecRuleEngineUtils.modsecValidate(anyString()))
          .thenReturn(Status.OK);
      mockModsecUtils
          .when(() -> ModsecRuleEngineUtils.corazaValidate(anyString()))
          .thenReturn(Status.OK);
      CustomSignatureModsecRulesManager modsecRulesManager = getCustomSignatureModsecRulesManager();

      List<CustomSignatureRule> rules = new ArrayList<>();

      createRules(
          rules,
          List.of(
              new MatchCombination(MatchKey.MATCH_KEY_URL, "/foo"),
              new MatchCombination(MatchKey.MATCH_KEY_HOST, "127.0.0.1"),
              new MatchCombination(MatchKey.MATCH_KEY_HTTP_METHOD, "post"),
              new MatchCombination(MatchKey.MATCH_KEY_USER_AGENT, "Chrome"),
              new MatchCombination(MatchKey.MATCH_KEY_HEADER_NAME, "x-real"),
              new MatchCombination(MatchKey.MATCH_KEY_HEADER_VALUE, "128.0.0.1"),
              new MatchCombination(MatchKey.MATCH_KEY_PARAMETER_NAME, "paramLevel"),
              new MatchCombination(MatchKey.MATCH_KEY_PARAMETER_VALUE, "5")),
          List.of(
              MatchOperator.MATCH_OPERATOR_EQUALS,
              MatchOperator.MATCH_OPERATOR_NOT_EQUAL,
              MatchOperator.MATCH_OPERATOR_CONTAINS,
              MatchOperator.MATCH_OPERATOR_NOT_CONTAIN),
          EventType.EVENT_TYPE_DETECTION_AND_BLOCKING);

      createRules(
          rules,
          List.of(
              new MatchCombination(MatchKey.MATCH_KEY_URL, "\\/foo$"),
              new MatchCombination(MatchKey.MATCH_KEY_HOST, "^127\\.0\\.0\\.1"),
              new MatchCombination(MatchKey.MATCH_KEY_HTTP_METHOD, "post|POST"),
              new MatchCombination(MatchKey.MATCH_KEY_USER_AGENT, "^Chrome"),
              new MatchCombination(MatchKey.MATCH_KEY_HEADER_NAME, "^x\\-real"),
              new MatchCombination(MatchKey.MATCH_KEY_HEADER_VALUE, ".*\\<script\\>.*"),
              new MatchCombination(MatchKey.MATCH_KEY_PARAMETER_NAME, "^(param)[a-s1-9_-]{3,16}$"),
              new MatchCombination(MatchKey.MATCH_KEY_PARAMETER_VALUE, "^\\d+$")),
          List.of(
              MatchOperator.MATCH_OPERATOR_MATCHES_REGEX,
              MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX),
          EventType.EVENT_TYPE_ALLOW);

      createRules(
          rules,
          List.of(
              new KeyValueCombination(KeyValueTag.KEY_VALUE_TAG_HEADER, "x-real", "128.0.0.1"),
              new KeyValueCombination(KeyValueTag.KEY_VALUE_TAG_PARAMETER, "paramLevel", "5")),
          List.of(MatchOperator.MATCH_OPERATOR_EQUALS, MatchOperator.MATCH_OPERATOR_NOT_EQUAL),
          List.of(
              MatchOperator.MATCH_OPERATOR_EQUALS,
              MatchOperator.MATCH_OPERATOR_NOT_EQUAL,
              MatchOperator.MATCH_OPERATOR_CONTAINS,
              MatchOperator.MATCH_OPERATOR_NOT_CONTAIN));

      createRules(
          rules,
          List.of(
              new KeyValueCombination(KeyValueTag.KEY_VALUE_TAG_HEADER, "^x\\-real", "128.0.0.1"),
              new KeyValueCombination(
                  KeyValueTag.KEY_VALUE_TAG_PARAMETER, "^(param)[a-s1-9_-]{3,16}$", "5")),
          List.of(
              MatchOperator.MATCH_OPERATOR_MATCHES_REGEX,
              MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX),
          List.of(
              MatchOperator.MATCH_OPERATOR_EQUALS,
              MatchOperator.MATCH_OPERATOR_NOT_EQUAL,
              MatchOperator.MATCH_OPERATOR_CONTAINS,
              MatchOperator.MATCH_OPERATOR_NOT_CONTAIN));

      createRules(
          rules,
          List.of(
              new KeyValueCombination(
                  KeyValueTag.KEY_VALUE_TAG_HEADER, "x-real", ".*\\<script\\>.*"),
              new KeyValueCombination(KeyValueTag.KEY_VALUE_TAG_PARAMETER, "paramLevel", "^\\d+$")),
          List.of(MatchOperator.MATCH_OPERATOR_EQUALS, MatchOperator.MATCH_OPERATOR_NOT_EQUAL),
          List.of(
              MatchOperator.MATCH_OPERATOR_MATCHES_REGEX,
              MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX));

      createRules(
          rules,
          List.of(
              new KeyValueCombination(
                  KeyValueTag.KEY_VALUE_TAG_HEADER, "^x\\-real", ".*\\<script\\>.*"),
              new KeyValueCombination(
                  KeyValueTag.KEY_VALUE_TAG_PARAMETER, "^(param)[a-s1-9_-]{3,16}$", "^\\d+$")),
          List.of(
              MatchOperator.MATCH_OPERATOR_MATCHES_REGEX,
              MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX),
          List.of(
              MatchOperator.MATCH_OPERATOR_MATCHES_REGEX,
              MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX));

      createRules(
          rules,
          List.of(new KeyValueCombination(KeyValueTag.KEY_VALUE_TAG_PARAMETER, "paramLevel", "5")),
          List.of(
              MatchOperator.MATCH_OPERATOR_EQUALS,
              MatchOperator.MATCH_OPERATOR_NOT_EQUAL,
              MatchOperator.MATCH_OPERATOR_MATCHES_REGEX,
              MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX),
          List.of(
              MatchOperator.MATCH_OPERATOR_GREATER_THAN, MatchOperator.MATCH_OPERATOR_LESS_THAN));

      createRules(
          rules,
          List.of(
              new KeyValueCombination(
                  KeyValueTag.KEY_VALUE_TAG_HEADER, "x\\-(real|forward)", "128.0.0.1"),
              new KeyValueCombination(
                  KeyValueTag.KEY_VALUE_TAG_PARAMETER, "^(param|parameter)[a-s1-9_-]{3,16}$", "5")),
          List.of(
              MatchOperator.MATCH_OPERATOR_MATCHES_REGEX,
              MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX),
          List.of(MatchOperator.MATCH_OPERATOR_EQUALS));

      createRules(
          rules,
          List.of(
              new KeyValueCombination(
                  KeyValueTag.KEY_VALUE_TAG_QUERY_PARAMETER, "^q-apple|q-banana", "mango"),
              new KeyValueCombination(
                  KeyValueTag.KEY_VALUE_TAG_BODY_PARAMETER, "^b-apple|b-banana", "mango")),
          List.of(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX),
          List.of(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX));

      createChainedRule(rules, 0, 10, 80, 100);
      createChainedRule(rules, 15, 95);

      GetCustomSignatureModsecRulesResponse response =
          modsecRulesManager.getModsecRules(
              RequestContext.forTenantId(TENANT_ID),
              rules,
              CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS,
              false,
              ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TA_BLOCKING,
              List.of());
      assertEquals(
          rules.size()
              + 4
              + 6
              + 4
              + 23, /* 4 extra chained rules for NOT_CONTAIN rules, 6 extra chained rules for REGEX with PIPE KeyValue rules, 3+1 extra chained rules and 23 lines of modsec directives */
          response.getModsecRulesBlob().split("\r\n|\n\n|\r|\n").length);
      assertEquals(rules.size(), response.getInlineRulesCount());
      assertEquals(
          EXPIRY_TIMESTAMP_MILLIS,
          response
              .getInlineRulesList()
              .get(0)
              .getRule()
              .getBlockingExpiryDetails()
              .getExpiryTimestampMillis());
      assertEquals(
          "dev",
          response
              .getInlineRules(0)
              .getRule()
              .getRuleScope()
              .getEnvironmentScope()
              .getEnvironmentIds(0));
      String fileRules =
          Resources.toString(
              Objects.requireNonNull(
                  CustomSignatureModsecRulesManagerTest.class
                      .getClassLoader()
                      .getResource("sample-custom-modsec-rules.conf")),
              StandardCharsets.UTF_8);
      assertEquals(fileRules, response.getModsecRulesBlob());
      if (SystemUtils.IS_OS_LINUX) {
        assertEquals(Status.OK, ModsecRuleEngineUtils.modsecValidate(fileRules));
      }
    }
  }

  @Test
  public void testIncludeAllPartialRules() throws Exception {
    ModsecDirectivesManager mockDirectivesManager = mock(ModsecDirectivesManager.class);
    when(mockDirectivesManager.getModsecHeader(ModsecRuleVersion.MODSEC_RULE_VERSION_V3))
        .thenReturn("");
    CustomModsecRuleConverter customModsecRuleConverter = mock(CustomModsecRuleConverter.class);

    CustomSignatureModsecRulesManager modsecRulesManager =
        new CustomSignatureModsecRulesManager(
            customModsecRuleConverter,
            mockDirectivesManager,
            mockModsecRulesRegistry,
            mockModsecBlobValidator,
            mockCachedServiceMappingProvider);
    when(customModsecRuleConverter.getValidatedModsecRule(
            anyLong(), anyString(), anyString(), anyList()))
        .thenReturn("SecRule");

    CustomSignatureRule ruleWithModsecConvertibleClause =
        CustomSignatureRule.newBuilder()
            .setId(UUID.randomUUID().toString())
            .setName("ruleWithModsecConvertibleClause")
            .setEffect(
                RuleEffect.newBuilder()
                    .addRuleEvaluationPoints(
                        RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT))
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(
                                        MatchExpression.newBuilder()
                                            .setMatchCategory(MatchCategory.MATCH_CATEGORY_REQUEST)
                                            .setMatchKey(MatchKey.MATCH_KEY_URL)
                                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_CONTAINS)
                                            .setMatchValue("/foo")))))
            .build();

    CustomSignatureRule ruleWithConvertibleAndNonConvertibleClause =
        CustomSignatureRule.newBuilder()
            .setId(UUID.randomUUID().toString())
            .setName("ruleWithConvertibleAndNonConvertibleClause")
            .setEffect(
                RuleEffect.newBuilder()
                    .addRuleEvaluationPoints(
                        RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT))
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .addClauses(
                                Clause.newBuilder()
                                    .setMatchExpression(
                                        MatchExpression.newBuilder()
                                            .setMatchCategory(MatchCategory.MATCH_CATEGORY_REQUEST)
                                            .setMatchKey(MatchKey.MATCH_KEY_URL)
                                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_CONTAINS)
                                            .setMatchValue("/foo")))
                            .addClauses(
                                Clause.newBuilder()
                                    .setIpOrganisationExpression(
                                        IpOrganisationExpression.getDefaultInstance()))
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)))
            .build();

    List<CustomSignatureRule> rules = new ArrayList<>();
    rules.add(ruleWithModsecConvertibleClause);
    rules.add(ruleWithConvertibleAndNonConvertibleClause);

    GetCustomSignatureModsecRulesResponse response =
        modsecRulesManager.getModsecRules(
            RequestContext.forTenantId(TENANT_ID),
            rules,
            CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS,
            true,
            ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TA_BLOCKING,
            List.of());
    assertEquals(2, response.getInlineRulesList().size());

    response =
        modsecRulesManager.getModsecRules(
            RequestContext.forTenantId(TENANT_ID),
            rules,
            CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS,
            true,
            ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TPA_DETECTION,
            List.of());
    assertEquals(1, response.getInlineRulesList().size());

    response =
        modsecRulesManager.getModsecRules(
            RequestContext.forTenantId(TENANT_ID),
            rules,
            CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS,
            false,
            ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TA_BLOCKING,
            List.of());
    assertEquals(1, response.getInlineRulesList().size());
  }

  private CustomSignatureModsecRulesManager getCustomSignatureModsecRulesManager() {
    ModsecDirectivesManager mockDirectivesManager = mock(ModsecDirectivesManager.class);
    when(mockDirectivesManager.getModsecHeader(
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS))
        .thenReturn(
            "SecRuleEngine On\n"
                + "SecRequestBodyAccess On\n"
                + "SecRequestBodyLimit 13107200\n"
                + "SecRequestBodyNoFilesLimit 131072\n"
                + "SecRequestBodyLimitAction Reject\n"
                + "SecPcreMatchLimit 1000\n"
                + "SecPcreMatchLimitRecursion 1000\n"
                + "SecResponseBodyAccess On\n"
                + "SecResponseBodyLimit 524288\n"
                + "SecTmpDir /tmp/\n"
                + "SecDataDir /tmp/\n"
                + "SecAuditEngine Off\n"
                + "SecAuditLogRelevantStatus \"^(?:5|404|403|401)\"\n"
                + "SecAuditLogParts ABIJDEFHZ\n"
                + "SecAuditLogType Serial\n"
                + "SecAuditLog /var/log/modsec_audit.log\n"
                + "SecArgumentSeparator &\n"
                + "SecCookieFormat 0\n"
                + "SecStatusEngine Off\n"
                + "SecDefaultAction \"phase:1,log,auditlog,deny,status:403\"\n"
                + "SecDefaultAction \"phase:2,log,auditlog,deny,status:403\"\n"
                + "SecCollectionTimeout 600\n"
                + "SecArgumentsLimit 1000"
                + "\n\n");

    ModsecVariableConverter modsecVariableConverter = new ModsecVariableConverter();
    ModsecOperatorConverter modsecOperatorConverter = new ModsecOperatorConverter();

    return new CustomSignatureModsecRulesManager(
        new CustomModsecRuleConverter(
            new ModsecRuleConverterImpl(
                new CustomModsecValueMatchClauseConverter(
                    modsecVariableConverter, modsecOperatorConverter),
                new CustomModsecKeyValueMatchClauseConverter(
                    modsecVariableConverter, modsecOperatorConverter))),
        mockDirectivesManager,
        mockModsecRulesRegistry,
        mockModsecBlobValidator,
        mockCachedServiceMappingProvider);
  }

  private void createRules(
      List<CustomSignatureRule> rules,
      List<MatchCombination> matchCombinations,
      List<MatchOperator> matchOperators,
      EventType eventType) {
    matchCombinations.forEach(
        matchCombination ->
            matchOperators.forEach(
                matchOperator ->
                    rules.add(
                        CustomSignatureRule.newBuilder()
                            .setId(getUUID(matchCombination.getMatchKey() + " : " + matchOperator))
                            .setName(matchCombination.getMatchKey() + " : " + matchOperator)
                            .setDescription(matchCombination.getMatchKey() + " : " + matchOperator)
                            .setEffect(getRuleEffect(eventType))
                            .setBlockingExpiryDetails(
                                ExpiryDetails.newBuilder()
                                    .setExpiryTimestampMillis(EXPIRY_TIMESTAMP_MILLIS)
                                    .setExpiryDuration(EXPIRY_DURATION)
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
                                                            .setMatchKey(
                                                                matchCombination.getMatchKey())
                                                            .setMatchOperator(matchOperator)
                                                            .setMatchValue(
                                                                matchCombination.getMatchValue())
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .setRuleScope(
                                RuleScope.newBuilder()
                                    .setEnvironmentScope(
                                        EnvironmentScope.newBuilder().addEnvironmentIds("dev"))
                                    .build())
                            .build())));
  }

  private void createRules(
      List<CustomSignatureRule> rules,
      List<KeyValueCombination> keyValueCombinations,
      List<MatchOperator> keyMatchOperators,
      List<MatchOperator> valueMatchOperators) {
    keyValueCombinations.forEach(
        keyValueCombination ->
            keyMatchOperators.forEach(
                keyMatchOperator ->
                    valueMatchOperators.forEach(
                        valueMatchOperator ->
                            rules.add(
                                CustomSignatureRule.newBuilder()
                                    .setId(
                                        getUUID(
                                            keyValueCombination.getKeyValueTag()
                                                + " : keyMatch("
                                                + keyMatchOperator
                                                + ") valueMatch("
                                                + valueMatchOperator
                                                + ")"))
                                    .setName(
                                        keyValueCombination.getKeyValueTag()
                                            + " : keyMatch("
                                            + keyMatchOperator
                                            + ") valueMatch("
                                            + valueMatchOperator
                                            + ")")
                                    .setDescription(
                                        keyValueCombination.getKeyValueTag()
                                            + " : keyMatch("
                                            + keyMatchOperator
                                            + ") valueMatch("
                                            + valueMatchOperator
                                            + ")")
                                    .setEffect(getRuleEffect())
                                    .setDefinition(
                                        RuleDefinition.newBuilder()
                                            .setClauseGroup(
                                                ClauseGroup.newBuilder()
                                                    .setClauseOperator(
                                                        ClauseOperator.CLAUSE_OPERATOR_AND)
                                                    .addClauses(
                                                        Clause.newBuilder()
                                                            .setKeyValueExpression(
                                                                KeyValueExpression.newBuilder()
                                                                    .setTag(
                                                                        keyValueCombination
                                                                            .getKeyValueTag())
                                                                    .setMatchKey(
                                                                        keyValueCombination
                                                                            .getMatchKey())
                                                                    .setKeyMatchOperator(
                                                                        keyMatchOperator)
                                                                    .setValueMatchOperator(
                                                                        valueMatchOperator)
                                                                    .setMatchValue(
                                                                        keyValueCombination
                                                                            .getMatchValue())
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .build())
                                    .setEffect(
                                        RuleEffect.newBuilder()
                                            .setEventType(
                                                EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                                            .addRuleEvaluationPoints(
                                                RuleEvaluationPoint
                                                    .RULE_EVALUATION_POINT_INLINE_TRACING_AGENT))
                                    .setBlockingExpiryDetails(
                                        ExpiryDetails.newBuilder()
                                            .setExpiryDuration(EXPIRY_DURATION)
                                            .setExpiryTimestampMillis(EXPIRY_TIMESTAMP_MILLIS)
                                            .build())
                                    .setRuleScope(
                                        RuleScope.newBuilder()
                                            .setEnvironmentScope(
                                                EnvironmentScope.newBuilder()
                                                    .addEnvironmentIds("dev"))
                                            .build())
                                    .build()))));
  }

  private void createChainedRule(List<CustomSignatureRule> rules, int... indices) {
    String idList =
        Arrays.stream(indices)
            .mapToObj(i -> String.valueOf(10000000 + 1 + i))
            .collect(Collectors.joining(" , "));
    rules.add(
        CustomSignatureRule.newBuilder()
            .setId(getUUID("Chained rule with ids: " + idList))
            .setName("Chained rule with ids: " + idList)
            .setDescription("Chained rule with ids: " + idList)
            .setEffect(getRuleEffect())
            .setDefinition(
                RuleDefinition.newBuilder()
                    .setClauseGroup(
                        ClauseGroup.newBuilder()
                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                            .addAllClauses(
                                Arrays.stream(indices)
                                    .mapToObj(
                                        i ->
                                            rules
                                                .get(i)
                                                .getDefinition()
                                                .getClauseGroup()
                                                .getClauses(0))
                                    .collect(Collectors.toList()))
                            .build())
                    .build())
            .build());
  }

  private RuleEffect getRuleEffect() {
    return RuleEffect.newBuilder()
        .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
        .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
        .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT)
        .build();
  }

  private RuleEffect getRuleEffect(EventType eventType) {
    return RuleEffect.newBuilder()
        .setEventType(eventType)
        .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
        .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT)
        .build();
  }

  private String getUUID(String value) {
    String ruleUuidSeed = "621c77fd-8014-41ac-b473-fa865642ef41";
    return UuidCreator.getNameBasedSha1(ruleUuidSeed, value).toString();
  }

  private static class MatchCombination {
    private final MatchKey matchKey;
    private final String matchValue;

    public MatchCombination(MatchKey matchKey, String matchValue) {
      this.matchKey = matchKey;
      this.matchValue = matchValue;
    }

    public MatchKey getMatchKey() {
      return matchKey;
    }

    public String getMatchValue() {
      return matchValue;
    }
  }

  private static class KeyValueCombination {
    private final KeyValueTag keyValueTag;
    private final String matchKey;
    private final String matchValue;

    public KeyValueCombination(KeyValueTag keyValueTag, String matchKey, String matchValue) {
      this.keyValueTag = keyValueTag;
      this.matchKey = matchKey;
      this.matchValue = matchValue;
    }

    public KeyValueTag getKeyValueTag() {
      return keyValueTag;
    }

    public String getMatchKey() {
      return matchKey;
    }

    public String getMatchValue() {
      return matchValue;
    }
  }
}
