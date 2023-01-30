package ai.traceable.customsignature.config.service.modsec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.customsignature.config.service.modsec.directives.ModsecDirectivesManager;
import ai.traceable.customsignature.config.service.modsec.registry.ModsecRuleMappings;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.EventSeverity;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.ExpiryDetails;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.KeyValueExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import com.github.f4b6a3.uuid.UuidCreator;
import com.google.common.io.Resources;
import io.grpc.Status;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.Test;

public class CustomSignatureModsecRulesManagerTest {

  private static final int EXPIRY_TIMESTAMP_MILLIS = 12345678;
  private static final String EXPIRY_DURATION = "P3M";

  @Test
  public void testConvertRulesException() {
    ModsecDirectivesManager mockDirectivesManager = mock(ModsecDirectivesManager.class);
    when(mockDirectivesManager.getModsecHeader(ModsecRuleVersion.MODSEC_RULE_VERSION_V3))
        .thenReturn("");
    ModsecRuleConversion modsecRuleConversion = mock(ModsecRuleConversion.class);
    CustomSignatureModsecRulesManager modsecRulesManager =
        new CustomSignatureModsecRulesManager(
            modsecRuleConversion, mockDirectivesManager, ModsecRuleVersion.MODSEC_RULE_VERSION_V3);

    GetCustomSignatureModsecRulesResponse response =
        modsecRulesManager.getModsecRules(List.of(CustomSignatureRule.newBuilder().build()));
    assertTrue(response.getModsecRulesBlob().isEmpty());
    assertTrue(response.getRulesList().isEmpty());

    response =
        modsecRulesManager.getModsecRules(
            List.of(
                CustomSignatureRule.newBuilder()
                    .setDefinition(
                        RuleDefinition.newBuilder()
                            .setClauseGroup(
                                ClauseGroup.newBuilder()
                                    .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                                    .build())
                            .build())
                    .build()));
    assertTrue(response.getModsecRulesBlob().isEmpty());
    assertTrue(response.getRulesList().isEmpty());

    when(modsecRuleConversion.getModsecRuleForANDClauses(any(), any()))
        .thenThrow(new UnsupportedOperationException());
    response =
        modsecRulesManager.getModsecRules(
            List.of(
                CustomSignatureRule.newBuilder()
                    .setDefinition(
                        RuleDefinition.newBuilder()
                            .setClauseGroup(
                                ClauseGroup.newBuilder()
                                    .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                                    .addClauses(Clause.getDefaultInstance())
                                    .build())
                            .build())
                    .build()));
    assertTrue(response.getModsecRulesBlob().isEmpty());
    assertTrue(response.getRulesList().isEmpty());
    verify(modsecRuleConversion, times(1)).getModsecRuleForANDClauses(any(), any());
  }

  @Test
  public void testConvertRules() throws IOException {
    ModsecDirectivesManager mockDirectivesManager = mock(ModsecDirectivesManager.class);
    when(mockDirectivesManager.getModsecHeader(ModsecRuleVersion.MODSEC_RULE_VERSION_V3))
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
                + "SecCollectionTimeout 600"
                + "\n\n");

    CustomSignatureModsecRulesManager modsecRulesManager =
        new CustomSignatureModsecRulesManager(
            new ModsecRuleConversion(new ModsecRuleMappings()),
            mockDirectivesManager,
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3);

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
            new KeyValueCombination(KeyValueTag.KEY_VALUE_TAG_HEADER, "x-real", ".*\\<script\\>.*"),
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
        List.of(MatchOperator.MATCH_OPERATOR_GREATER_THAN, MatchOperator.MATCH_OPERATOR_LESS_THAN));

    createChainedRule(rules, 0, 10, 80, 100);
    createChainedRule(rules, 15, 95);

    GetCustomSignatureModsecRulesResponse response = modsecRulesManager.getModsecRules(rules);
    // subtracting 4 unsupported NOT_CONTAIN rules
    assertEquals(
        rules.size() - 4 + 4 + 22, /* 3+1 extra chained rules and 22 lines of modsec directives */
        response.getModsecRulesBlob().split("\r\n|\n\n|\r|\n").length);
    assertEquals(rules.size() - 4, response.getRulesCount());
    assertEquals(
        EXPIRY_TIMESTAMP_MILLIS,
        response.getRulesList().get(0).getBlockingExpiryDetails().getExpiryTimestampMillis());
    assertEquals(
        "dev", response.getRules(0).getRuleScope().getEnvironmentScope().getEnvironmentIds(0));

    String fileRules =
        Resources.toString(
            CustomSignatureModsecRulesManagerTest.class
                .getClassLoader()
                .getResource("sample-modsecurity-rules.conf"),
            StandardCharsets.UTF_8);
    assertEquals(fileRules, response.getModsecRulesBlob());

    if (SystemUtils.IS_OS_LINUX) {
      assertTrue(modsecRulesManager.loadNativeLibrarySuccess);
      assertEquals(Status.OK, modsecRulesManager.validateModsecRule(fileRules, "test"));
    }
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
        .setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION)
        .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
        .build();
  }

  private RuleEffect getRuleEffect(EventType eventType) {
    return RuleEffect.newBuilder()
        .setEventType(eventType)
        .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
        .build();
  }

  private String getUUID(String value) {
    String ruleUuidSeed = "621c77fd-8014-41ac-b473-fa865642ef41";
    return UuidCreator.getNameBasedSha1(ruleUuidSeed, value).toString();
  }

  private class MatchCombination {
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

  private class KeyValueCombination {
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
