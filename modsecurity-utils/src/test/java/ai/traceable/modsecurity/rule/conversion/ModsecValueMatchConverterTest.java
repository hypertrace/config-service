package ai.traceable.modsecurity.rule.conversion;

import static ai.traceable.customsignature.config.service.v1.MatchCategory.MATCH_CATEGORY_REQUEST;
import static ai.traceable.customsignature.config.service.v1.MatchCategory.MATCH_CATEGORY_RESPONSE;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_COOKIES_COUNT;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_HEADERS_COUNT;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_QUERY_PARAMS_COUNT;
import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_CONTAINS;
import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_EQUALS;
import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_GREATER_THAN;
import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_LESS_THAN;
import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_MATCHES_REGEX;
import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_NOT_CONTAIN;
import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_NOT_EQUAL;
import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX;
import static ai.traceable.modsecurity.rule.api.v1.RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_BODY;
import static ai.traceable.modsecurity.rule.api.v1.RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_BODY_SIZE;
import static ai.traceable.modsecurity.rule.api.v1.RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_HOST;
import static ai.traceable.modsecurity.rule.api.v1.RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_HTTP_METHOD;
import static ai.traceable.modsecurity.rule.api.v1.RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_URL;
import static ai.traceable.modsecurity.rule.api.v1.RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_USER_AGENT;
import static ai.traceable.modsecurity.rule.api.v1.ResponseValueMatchMetadata.RESPONSE_VALUE_MATCH_METADATA_BODY;
import static ai.traceable.modsecurity.rule.api.v1.ResponseValueMatchMetadata.RESPONSE_VALUE_MATCH_METADATA_BODY_SIZE;
import static ai.traceable.modsecurity.rule.api.v1.ResponseValueMatchMetadata.RESPONSE_VALUE_MATCH_METADATA_STATUS_CODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.modsecurity.RuleEngine;
import ai.traceable.modsecurity.RuleMatch;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecRule;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecRuleClause;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecValueMatchClause;
import ai.traceable.modsecurity.rule.api.v1.CustomSecRuleClause;
import ai.traceable.modsecurity.rule.api.v1.RequestValueMatchMetadata;
import ai.traceable.modsecurity.rule.api.v1.ResponseValueMatchMetadata;
import ai.traceable.modsecurity.rule.conversion.clause.CustomModsecKeyValueMatchClauseConverter;
import ai.traceable.modsecurity.rule.conversion.clause.CustomModsecValueMatchClauseConverter;
import ai.traceable.modsecurity.rule.conversion.clause.ModsecOperatorConverter;
import ai.traceable.modsecurity.rule.conversion.clause.ModsecVariableConverter;
import ai.traceable.modsecurity.utils.ModsecRuleEngineUtils;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.Value;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class ModsecValueMatchConverterTest {

  private static final long RULE_ID_SEED = 100001;
  private static final UuidGenerator uuidGenerator = new UuidGenerator();

  private final ModsecVariableConverter modsecVariableConverter = new ModsecVariableConverter();
  private final ModsecOperatorConverter modsecOperatorConverter = new ModsecOperatorConverter();
  private final ModsecRuleConverter modsecRuleConverter =
      new ModsecRuleConverterImpl(
          new CustomModsecValueMatchClauseConverter(
              modsecVariableConverter, modsecOperatorConverter),
          new CustomModsecKeyValueMatchClauseConverter(
              modsecVariableConverter, modsecOperatorConverter));

  private RuleEngine ruleEngine;

  @BeforeEach
  @EnabledOnOs(OS.LINUX)
  public void setup() throws Exception {

    try (MockedStatic<ModsecRuleEngineUtils> mockModsecUtils =
        mockStatic(ModsecRuleEngineUtils.class, Mockito.CALLS_REAL_METHODS)) {
      mockModsecUtils
          .when(() -> ModsecRuleEngineUtils.corazaValidate(anyString()))
          .thenReturn(Status.OK);
      ruleEngine =
          ModsecRuleEngineUtils.createRuleEngine(
              "SecResponseBodyAccess On\n\n"
                  + modsecRuleConverter.getModsecRulesBlob(getSampleCustomModsecRules()));
    }
  }

  @AfterEach
  @EnabledOnOs(OS.LINUX)
  public void finish() {
    if (ruleEngine != null) {
      RuleEngine.destroy(ruleEngine);
    }
  }

  @Test
  @EnabledOnOs(OS.LINUX)
  public void testMatches() {
    try (MockedStatic<ModsecRuleEngineUtils> mockModsecUtils =
        mockStatic(ModsecRuleEngineUtils.class, Mockito.CALLS_REAL_METHODS)) {
      mockModsecUtils
          .when(() -> ModsecRuleEngineUtils.corazaValidate(anyString()))
          .thenReturn(Status.OK);
      Map<String, String> attributesMap =
          Map.of(
              "http.url",
              "/haha/xyz.txt?param1=test",
              "http.method",
              "get",
              "http.request.version",
              "1.0",
              "http.request.header.user-agent",
              "Chrome x/10",
              "http.request.header.host",
              "my-home-hostnames",
              "http.request.header.x-forwarded-host",
              "198.23.1.2",
              "http.request.body.login",
              "' or '1'='1",
              "http.request.body.password",
              "validpassword",
              "http.response.body",
              "21");

      Set<RuleMatchInfo> ruleMatchInfos =
          ModsecRuleEngineUtils.getModsecRuleMatches(ruleEngine, attributesMap).stream()
              .map(RuleMatchInfo::new)
              .collect(Collectors.toUnmodifiableSet());
      assertEquals(35, ruleMatchInfos.size());
      verifyRuleMatches(
          ruleMatchInfos,
          List.of(
              REQUEST_VALUE_MATCH_METADATA_URL + ":" + MATCH_OPERATOR_NOT_EQUAL,
              REQUEST_VALUE_MATCH_METADATA_URL + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX,
              REQUEST_VALUE_MATCH_METADATA_URL + ":" + MATCH_OPERATOR_NOT_CONTAIN,
              REQUEST_VALUE_MATCH_METADATA_URL + ":" + MATCH_OPERATOR_MATCHES_REGEX),
          "http.url",
          "/haha/xyz.txt?param1=test");
      verifyRuleMatches(
          ruleMatchInfos,
          List.of(
              REQUEST_VALUE_MATCH_METADATA_USER_AGENT + ":" + MATCH_OPERATOR_NOT_EQUAL,
              REQUEST_VALUE_MATCH_METADATA_USER_AGENT + ":" + MATCH_OPERATOR_MATCHES_REGEX,
              REQUEST_VALUE_MATCH_METADATA_USER_AGENT + ":" + MATCH_OPERATOR_CONTAINS,
              REQUEST_VALUE_MATCH_METADATA_USER_AGENT + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX),
          "http.request.header.user-agent",
          "Chrome x/10");
      verifyRuleMatches(
          ruleMatchInfos,
          List.of(
              REQUEST_VALUE_MATCH_METADATA_HTTP_METHOD + ":" + MATCH_OPERATOR_EQUALS,
              REQUEST_VALUE_MATCH_METADATA_HTTP_METHOD + ":" + MATCH_OPERATOR_MATCHES_REGEX,
              REQUEST_VALUE_MATCH_METADATA_HTTP_METHOD + ":" + MATCH_OPERATOR_CONTAINS,
              REQUEST_VALUE_MATCH_METADATA_HTTP_METHOD + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX),
          "", // TODO: Needs to be fixed in modsecurity
          "get");

      /*
      TODO: Debug and fix the test - Intermittently failing
      verifyRuleMatches(
          ruleMatchInfos,
          List.of(
              REQUEST_VALUE_MATCH_METADATA_BODY + ":" + MATCH_OPERATOR_NOT_EQUAL,
              REQUEST_VALUE_MATCH_METADATA_BODY + ":" + MATCH_OPERATOR_NOT_CONTAIN,
              REQUEST_VALUE_MATCH_METADATA_BODY + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX,
              REQUEST_VALUE_MATCH_METADATA_BODY + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX),
          "default.",
          "");
      */

      verifyRuleMatches(
          ruleMatchInfos,
          List.of(
              RESPONSE_VALUE_MATCH_METADATA_BODY + ":" + MATCH_OPERATOR_NOT_EQUAL,
              RESPONSE_VALUE_MATCH_METADATA_BODY + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX,
              RESPONSE_VALUE_MATCH_METADATA_BODY + ":" + MATCH_OPERATOR_NOT_CONTAIN,
              RESPONSE_VALUE_MATCH_METADATA_BODY + ":" + MATCH_OPERATOR_GREATER_THAN,
              RESPONSE_VALUE_MATCH_METADATA_BODY + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX),
          "http.response.body",
          "21");
      verifyRuleMatches(
          ruleMatchInfos,
          List.of(
              RESPONSE_VALUE_MATCH_METADATA_STATUS_CODE + ":" + MATCH_OPERATOR_NOT_EQUAL,
              RESPONSE_VALUE_MATCH_METADATA_STATUS_CODE + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX,
              RESPONSE_VALUE_MATCH_METADATA_STATUS_CODE + ":" + MATCH_OPERATOR_NOT_CONTAIN,
              RESPONSE_VALUE_MATCH_METADATA_STATUS_CODE + ":" + MATCH_OPERATOR_LESS_THAN,
              RESPONSE_VALUE_MATCH_METADATA_STATUS_CODE + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX),
          "http.response.status_code",
          "200");
      verifyRuleMatches(
          ruleMatchInfos,
          List.of(
              REQUEST_VALUE_MATCH_METADATA_HOST + ":" + MATCH_OPERATOR_NOT_EQUAL,
              REQUEST_VALUE_MATCH_METADATA_HOST + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX,
              REQUEST_VALUE_MATCH_METADATA_HOST + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX,
              REQUEST_VALUE_MATCH_METADATA_HOST + ":" + MATCH_OPERATOR_NOT_CONTAIN),
          "http.request.header.x-forwarded-host",
          "198.23.1.2");
      verifyRuleMatches(
          ruleMatchInfos,
          List.of(REQUEST_VALUE_MATCH_METADATA_HOST + ":" + MATCH_OPERATOR_MATCHES_REGEX),
          "http.request.header.host",
          "my-home-hostnames");
    }
  }

  private void verifyRuleMatches(
      Set<RuleMatchInfo> ruleMatchInfos,
      List<String> ruleMsgs,
      String matchAttribute,
      String matchAttributeValue) {
    ruleMsgs.forEach(
        ruleMsg ->
            assertTrue(
                ruleMatchInfos.contains(
                    new RuleMatchInfo(ruleMsg, matchAttribute, matchAttributeValue))));
  }

  static List<CustomModsecRule> getSampleCustomModsecRules() {
    List<CustomModsecRule> customModsecRules = new ArrayList<>();

    customModsecRules.addAll(
        createCustomModsecRequestValueMatchRules(
            Map.of(
                REQUEST_VALUE_MATCH_METADATA_URL,
                "/url",
                REQUEST_VALUE_MATCH_METADATA_HOST,
                "local",
                REQUEST_VALUE_MATCH_METADATA_HTTP_METHOD,
                "get",
                REQUEST_VALUE_MATCH_METADATA_USER_AGENT,
                "Chrome",
                REQUEST_VALUE_MATCH_METADATA_BODY,
                "big-one"),
            List.of(
                MATCH_OPERATOR_EQUALS,
                MATCH_OPERATOR_NOT_EQUAL,
                MATCH_OPERATOR_MATCHES_REGEX,
                MATCH_OPERATOR_NOT_MATCH_REGEX,
                MATCH_OPERATOR_CONTAINS,
                MATCH_OPERATOR_NOT_CONTAIN)));

    customModsecRules.addAll(
        createCustomModsecRequestValueMatchRules(
            Map.of(REQUEST_VALUE_MATCH_METADATA_BODY_SIZE, "1"),
            List.of(
                MATCH_OPERATOR_EQUALS,
                MATCH_OPERATOR_NOT_EQUAL,
                MATCH_OPERATOR_GREATER_THAN,
                MATCH_OPERATOR_LESS_THAN)));

    customModsecRules.addAll(
        createCustomModsecRequestValueMatchRules(
            Map.of(
                REQUEST_VALUE_MATCH_METADATA_URL,
                "/(abc|xyz)",
                REQUEST_VALUE_MATCH_METADATA_HOST,
                "host(s|names)",
                REQUEST_VALUE_MATCH_METADATA_HTTP_METHOD,
                "p(u|os)t",
                REQUEST_VALUE_MATCH_METADATA_USER_AGENT,
                "(f|F)irefox.*",
                REQUEST_VALUE_MATCH_METADATA_BODY,
                "(no|some)thing.*me"),
            List.of(MATCH_OPERATOR_MATCHES_REGEX, MATCH_OPERATOR_NOT_MATCH_REGEX)));

    customModsecRules.addAll(
        createCustomModsecResponseValueMatchRules(
            Map.of(
                RESPONSE_VALUE_MATCH_METADATA_STATUS_CODE,
                "400",
                RESPONSE_VALUE_MATCH_METADATA_BODY,
                "20"),
            List.of(
                MATCH_OPERATOR_EQUALS,
                MATCH_OPERATOR_NOT_EQUAL,
                MATCH_OPERATOR_MATCHES_REGEX,
                MATCH_OPERATOR_NOT_MATCH_REGEX,
                MATCH_OPERATOR_CONTAINS,
                MATCH_OPERATOR_NOT_CONTAIN,
                MATCH_OPERATOR_GREATER_THAN,
                MATCH_OPERATOR_LESS_THAN)));

    customModsecRules.addAll(
        createCustomModsecResponseValueMatchRules(
            Map.of(RESPONSE_VALUE_MATCH_METADATA_BODY_SIZE, "1"),
            List.of(
                MATCH_OPERATOR_EQUALS,
                MATCH_OPERATOR_NOT_EQUAL,
                MATCH_OPERATOR_GREATER_THAN,
                MATCH_OPERATOR_LESS_THAN)));

    customModsecRules.addAll(
        createCustomModsecResponseValueMatchRules(
            Map.of(
                RESPONSE_VALUE_MATCH_METADATA_STATUS_CODE,
                "(3|5)[0-9]+2",
                RESPONSE_VALUE_MATCH_METADATA_BODY,
                "(no|some)thing.*me"),
            List.of(MATCH_OPERATOR_MATCHES_REGEX, MATCH_OPERATOR_NOT_MATCH_REGEX)));

    customModsecRules.addAll(
        createCustomModsecValueMatchRulesForCountMatchKeys(
            MATCH_CATEGORY_REQUEST,
            Map.of(
                MATCH_KEY_QUERY_PARAMS_COUNT, "5",
                MATCH_KEY_HEADERS_COUNT, "2",
                MATCH_KEY_COOKIES_COUNT, "3"),
            List.of(
                MATCH_OPERATOR_EQUALS,
                MATCH_OPERATOR_NOT_EQUAL,
                MATCH_OPERATOR_GREATER_THAN,
                MATCH_OPERATOR_LESS_THAN)));

    customModsecRules.addAll(
        createCustomModsecValueMatchRulesForCountMatchKeys(
            MATCH_CATEGORY_RESPONSE,
            Map.of(MATCH_KEY_HEADERS_COUNT, "4"),
            List.of(
                MATCH_OPERATOR_EQUALS,
                MATCH_OPERATOR_NOT_EQUAL,
                MATCH_OPERATOR_GREATER_THAN,
                MATCH_OPERATOR_LESS_THAN)));

    customModsecRules.sort(Comparator.comparing(CustomModsecRule::toString));
    AtomicLong ruleId = new AtomicLong(RULE_ID_SEED);
    return customModsecRules.stream()
        .map(rule -> rule.toBuilder().setRuleId(ruleId.getAndIncrement()).build())
        .collect(Collectors.toList());
  }

  private static List<CustomModsecRule> createCustomModsecValueMatchRulesForCountMatchKeys(
      MatchCategory matchCategory,
      Map<MatchKey, String> matchKeyToValueMap,
      List<CustomModsecMatchExpression.MatchOperator> operators) {

    return matchKeyToValueMap.entrySet().stream()
        .flatMap(
            entry -> {
              MatchKey matchKey = entry.getKey();
              String value = entry.getValue();
              return operators.stream()
                  .map(
                      operator -> {
                        String ruleMsg = matchKey.name() + ":" + operator.name();
                        String ruleUuid = uuidGenerator.generateId(ruleMsg);
                        String modsecRule =
                            (matchCategory.equals(MATCH_CATEGORY_REQUEST))
                                ? generateModsecCountRule("", matchKey, value, operator, ruleUuid)
                                : generateModsecCountRule(
                                    "&RESPONSE_HEADERS", matchKey, value, operator, ruleUuid);
                        return CustomModsecRule.newBuilder()
                            .setRuleUuid(ruleUuid)
                            .setRuleMsg(ruleMsg)
                            .addAndClauses(
                                CustomModsecRuleClause.newBuilder()
                                    .setCustomSecRuleClause(
                                        CustomSecRuleClause.newBuilder()
                                            .setInputSecRule(modsecRule)
                                            .build())
                                    .build())
                            .build();
                      });
            })
        .collect(Collectors.toList());
  }

  private static String generateModsecCountRule(
      String modsecVariable,
      MatchKey matchKey,
      String value,
      CustomModsecMatchExpression.MatchOperator operator,
      String ruleUuid) {
    String messageForMatchKeyType;
    if (modsecVariable.isEmpty()) {
      modsecVariable = getModsecVariableForMatchKeyType(matchKey);
      messageForMatchKeyType = getMessageForMatchKeyType(matchKey);
    } else {
      messageForMatchKeyType = "Response contains %{tx.headers_count} headers";
    }
    String modsecOperator = getModsecOperator(operator);
    return String.format(
        "SecRule %s \"%s %s\" \\\n"
            + "    \"id:9000000,\\\n"
            + "    phase:%d,\\\n"
            + "    pass,\\\n"
            + "    t:none,\\\n"
            + "    setvar:'tx.%s=%%{MATCHED_VAR}',\\\n"
            + "    msg:'%s',\\\n"
            + "    tag:'%s',\\\n"
            + "    tag:'paranoia-level/1',\\\n"
            + "    tag:'rule-uuid/%s',\\\n"
            + "    severity:'%s'\"",
        modsecVariable,
        modsecOperator,
        value,
        getPhaseForMatchKeyType(matchKey),
        matchKey.name().toLowerCase(),
        messageForMatchKeyType,
        getTagForMatchKeyType(matchKey),
        ruleUuid,
        "INFO");
  }

  private static String getModsecOperator(CustomModsecMatchExpression.MatchOperator operator) {
    switch (operator) {
      case MATCH_OPERATOR_EQUALS:
        return "eq";
      case MATCH_OPERATOR_NOT_EQUAL:
        return "!eq";
      case MATCH_OPERATOR_GREATER_THAN:
        return "gt";
      case MATCH_OPERATOR_LESS_THAN:
        return "lt";
      default:
        throw new IllegalArgumentException("Unsupported operator: " + operator);
    }
  }

  private static String getMessageForMatchKeyType(MatchKey matchKey) {
    switch (matchKey) {
      case MATCH_KEY_QUERY_PARAMS_COUNT:
        return "Request contains %{tx.query_params_count} query parameters";
      case MATCH_KEY_HEADERS_COUNT:
        return "Request contains %{tx.headers_count} headers";
      case MATCH_KEY_COOKIES_COUNT:
        return "Request contains %{tx.cookies_count} cookies";
      default:
        throw new IllegalArgumentException("Unsupported matchKey: " + matchKey);
    }
  }

  private static String getTagForMatchKeyType(MatchKey matchKey) {
    switch (matchKey) {
      case MATCH_KEY_QUERY_PARAMS_COUNT:
        return "QUERY_PARAM_COUNTER";
      case MATCH_KEY_HEADERS_COUNT:
        return "HEADER_COUNTER";
      case MATCH_KEY_COOKIES_COUNT:
        return "COOKIE_COUNTER";
      default:
        throw new IllegalArgumentException("Unsupported matchKey: " + matchKey);
    }
  }

  private static String getModsecVariableForMatchKeyType(MatchKey matchKey) {
    switch (matchKey) {
      case MATCH_KEY_QUERY_PARAMS_COUNT:
        return "&ARGS";
      case MATCH_KEY_HEADERS_COUNT:
        return "&REQUEST_HEADERS";
      case MATCH_KEY_COOKIES_COUNT:
        return "&REQUEST_COOKIES";
      default:
        throw new IllegalArgumentException("Unsupported matchKey: " + matchKey);
    }
  }

  private static int getPhaseForMatchKeyType(MatchKey matchKey) {
    switch (matchKey) {
      case MATCH_KEY_QUERY_PARAMS_COUNT:
      case MATCH_KEY_HEADERS_COUNT:
      case MATCH_KEY_COOKIES_COUNT:
        return 1;
      default:
        throw new IllegalArgumentException("Unsupported matchKey: " + matchKey);
    }
  }

  private static List<CustomModsecRule> createCustomModsecRequestValueMatchRules(
      Map<RequestValueMatchMetadata, String> metadataValuesMap,
      List<CustomModsecMatchExpression.MatchOperator> operators) {
    return metadataValuesMap.entrySet().stream()
        .flatMap(
            entry ->
                operators.stream()
                    .map(
                        operator ->
                            createCustomModsecRule(entry.getKey(), operator, entry.getValue())))
        .collect(Collectors.toList());
  }

  private static List<CustomModsecRule> createCustomModsecResponseValueMatchRules(
      Map<ResponseValueMatchMetadata, String> metadataValuesMap,
      List<CustomModsecMatchExpression.MatchOperator> operators) {
    return metadataValuesMap.entrySet().stream()
        .flatMap(
            entry ->
                operators.stream()
                    .map(
                        operator ->
                            createCustomModsecRule(entry.getKey(), operator, entry.getValue())))
        .collect(Collectors.toList());
  }

  private static CustomModsecRule createCustomModsecRule(
      RequestValueMatchMetadata requestMetadata,
      CustomModsecMatchExpression.MatchOperator operator,
      String value) {
    String ruleMsg = requestMetadata.name() + ":" + operator.name();
    String ruleUuid = uuidGenerator.generateId(ruleMsg);
    return CustomModsecRule.newBuilder()
        .setRuleMsg(ruleMsg)
        .setRuleUuid(ruleUuid)
        .addAndClauses(
            CustomModsecRuleClause.newBuilder()
                .setValueMatchClause(
                    CustomModsecValueMatchClause.newBuilder()
                        .setRequestValueMetadata(requestMetadata)
                        .setValueMatchExpression(
                            CustomModsecMatchExpression.newBuilder()
                                .setValueMatchOperator(operator)
                                .setMatchValue(value))))
        .build();
  }

  private static CustomModsecRule createCustomModsecRule(
      ResponseValueMatchMetadata responseMetadata,
      CustomModsecMatchExpression.MatchOperator operator,
      String value) {
    String ruleMsg = responseMetadata.name() + ":" + operator.name();
    String ruleUuid = uuidGenerator.generateId(ruleMsg);
    return CustomModsecRule.newBuilder()
        .setRuleMsg(ruleMsg)
        .setRuleUuid(ruleUuid)
        .addAndClauses(
            CustomModsecRuleClause.newBuilder()
                .setValueMatchClause(
                    CustomModsecValueMatchClause.newBuilder()
                        .setResponseValueMetadata(responseMetadata)
                        .setValueMatchExpression(
                            CustomModsecMatchExpression.newBuilder()
                                .setValueMatchOperator(operator)
                                .setMatchValue(value))))
        .build();
  }

  @Value
  static class RuleMatchInfo {
    String ruleMsg;
    String matchAttribute;
    String matchAttributeValue;

    RuleMatchInfo(RuleMatch ruleMatch) {
      this.ruleMsg = ruleMatch.getRuleMessage();
      this.matchAttribute = ruleMatch.getMatchAttribute();
      this.matchAttributeValue = ruleMatch.getMatchAttributeValue();
    }

    public RuleMatchInfo(String ruleMsg, String matchAttribute, String matchAttributeValue) {
      this.ruleMsg = ruleMsg;
      this.matchAttribute = matchAttribute;
      this.matchAttributeValue = matchAttributeValue;
    }
  }
}
