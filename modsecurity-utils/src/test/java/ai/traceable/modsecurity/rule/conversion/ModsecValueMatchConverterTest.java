package ai.traceable.modsecurity.rule.conversion;

import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_CONTAINS;
import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_EQUALS;
import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_GREATER_THAN;
import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_LESS_THAN;
import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_MATCHES_REGEX;
import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_NOT_CONTAIN;
import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_NOT_EQUAL;
import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX;
import static ai.traceable.modsecurity.rule.api.v1.RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_BODY;
import static ai.traceable.modsecurity.rule.api.v1.RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_HOST;
import static ai.traceable.modsecurity.rule.api.v1.RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_HTTP_METHOD;
import static ai.traceable.modsecurity.rule.api.v1.RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_URL;
import static ai.traceable.modsecurity.rule.api.v1.RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_USER_AGENT;
import static ai.traceable.modsecurity.rule.api.v1.ResponseValueMatchMetadata.RESPONSE_VALUE_MATCH_METADATA_BODY;
import static ai.traceable.modsecurity.rule.api.v1.ResponseValueMatchMetadata.RESPONSE_VALUE_MATCH_METADATA_STATUS_CODE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.modsecurity.RuleEngine;
import ai.traceable.modsecurity.RuleMatch;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecRule;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecRuleClause;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecValueMatchClause;
import ai.traceable.modsecurity.rule.api.v1.RequestValueMatchMetadata;
import ai.traceable.modsecurity.rule.api.v1.ResponseValueMatchMetadata;
import ai.traceable.modsecurity.rule.conversion.clause.CustomModsecKeyValueMatchClauseConverter;
import ai.traceable.modsecurity.rule.conversion.clause.CustomModsecValueMatchClauseConverter;
import ai.traceable.modsecurity.rule.conversion.clause.ModsecOperatorConverter;
import ai.traceable.modsecurity.rule.conversion.clause.ModsecVariableConverter;
import ai.traceable.modsecurity.utils.ModsecRuleEngineUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

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
    ruleEngine =
        ModsecRuleEngineUtils.createRuleEngine(
            "SecResponseBodyAccess On\n\n"
                + modsecRuleConverter.getModsecRulesBlob(getSampleCustomModsecRules()));
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

    Map<String, RuleMatch> ruleMatches =
        ModsecRuleEngineUtils.getModsecRuleMatches(ruleEngine, attributesMap).stream()
            .collect(Collectors.toMap(RuleMatch::getRuleId, Function.identity()));
    assertEquals(31, ruleMatches.size());
    verifyRuleMatches(
        Map.of(
            ruleMatches.get("100053"),
            REQUEST_VALUE_MATCH_METADATA_URL + ":" + MATCH_OPERATOR_NOT_EQUAL,
            ruleMatches.get("100055"),
            REQUEST_VALUE_MATCH_METADATA_URL + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX,
            ruleMatches.get("100005"),
            REQUEST_VALUE_MATCH_METADATA_URL + ":" + MATCH_OPERATOR_NOT_CONTAIN,
            ruleMatches.get("100020"),
            REQUEST_VALUE_MATCH_METADATA_URL + ":" + MATCH_OPERATOR_MATCHES_REGEX),
        "http.url",
        "/haha/xyz.txt?param1=test");
    verifyRuleMatches(
        Map.of(
            ruleMatches.get("100058"),
            REQUEST_VALUE_MATCH_METADATA_USER_AGENT + ":" + MATCH_OPERATOR_NOT_EQUAL,
            ruleMatches.get("100039"),
            REQUEST_VALUE_MATCH_METADATA_USER_AGENT + ":" + MATCH_OPERATOR_MATCHES_REGEX,
            ruleMatches.get("100019"),
            REQUEST_VALUE_MATCH_METADATA_USER_AGENT + ":" + MATCH_OPERATOR_CONTAINS,
            ruleMatches.get("100025"),
            REQUEST_VALUE_MATCH_METADATA_USER_AGENT + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX),
        "http.request.header.user-agent",
        "Chrome x/10");
    verifyRuleMatches(
        Map.of(
            ruleMatches.get("100004"),
            REQUEST_VALUE_MATCH_METADATA_HTTP_METHOD + ":" + MATCH_OPERATOR_EQUALS,
            ruleMatches.get("100002"),
            REQUEST_VALUE_MATCH_METADATA_HTTP_METHOD + ":" + MATCH_OPERATOR_MATCHES_REGEX,
            ruleMatches.get("100023"),
            REQUEST_VALUE_MATCH_METADATA_HTTP_METHOD + ":" + MATCH_OPERATOR_CONTAINS,
            ruleMatches.get("100033"),
            REQUEST_VALUE_MATCH_METADATA_HTTP_METHOD + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX),
        "", // TODO: Needs to be fixed in modsecurity
        "get");
    /*
    TODO: Debug and fix the test - Intermittently failing
    verifyRuleMatches(
        Map.of(
            ruleMatches.get("100029"),
            REQUEST_VALUE_MATCH_METADATA_BODY + ":" + MATCH_OPERATOR_NOT_EQUAL,
            ruleMatches.get("100011"),
            REQUEST_VALUE_MATCH_METADATA_BODY + ":" + MATCH_OPERATOR_NOT_CONTAIN,
            ruleMatches.get("100049"),
            REQUEST_VALUE_MATCH_METADATA_BODY + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX,
            ruleMatches.get("100050"),
            REQUEST_VALUE_MATCH_METADATA_BODY + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX),
        "default.",
        "");
     */
    verifyRuleMatches(
        Map.of(
            ruleMatches.get("100048"),
            RESPONSE_VALUE_MATCH_METADATA_BODY + ":" + MATCH_OPERATOR_NOT_EQUAL,
            ruleMatches.get("100013"),
            RESPONSE_VALUE_MATCH_METADATA_BODY + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX,
            ruleMatches.get("100022"),
            RESPONSE_VALUE_MATCH_METADATA_BODY + ":" + MATCH_OPERATOR_NOT_CONTAIN,
            ruleMatches.get("100047"),
            RESPONSE_VALUE_MATCH_METADATA_BODY + ":" + MATCH_OPERATOR_GREATER_THAN,
            ruleMatches.get("100014"),
            RESPONSE_VALUE_MATCH_METADATA_BODY + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX),
        "http.response.body",
        "21");
    verifyRuleMatches(
        Map.of(
            ruleMatches.get("100007"),
            RESPONSE_VALUE_MATCH_METADATA_STATUS_CODE + ":" + MATCH_OPERATOR_NOT_EQUAL,
            ruleMatches.get("100042"),
            RESPONSE_VALUE_MATCH_METADATA_STATUS_CODE + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX,
            ruleMatches.get("100018"),
            RESPONSE_VALUE_MATCH_METADATA_STATUS_CODE + ":" + MATCH_OPERATOR_NOT_CONTAIN,
            ruleMatches.get("100008"),
            RESPONSE_VALUE_MATCH_METADATA_STATUS_CODE + ":" + MATCH_OPERATOR_LESS_THAN,
            ruleMatches.get("100043"),
            RESPONSE_VALUE_MATCH_METADATA_STATUS_CODE + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX),
        "http.response.status_code",
        "200");
    verifyRuleMatches(
        Map.of(
            ruleMatches.get("100040"),
                REQUEST_VALUE_MATCH_METADATA_HOST + ":" + MATCH_OPERATOR_NOT_EQUAL,
            ruleMatches.get("100034"),
                REQUEST_VALUE_MATCH_METADATA_HOST + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX,
            ruleMatches.get("100035"),
                REQUEST_VALUE_MATCH_METADATA_HOST + ":" + MATCH_OPERATOR_NOT_MATCH_REGEX,
            ruleMatches.get("100015"),
                REQUEST_VALUE_MATCH_METADATA_HOST + ":" + MATCH_OPERATOR_NOT_CONTAIN),
        "http.request.header.x-forwarded-host",
        "198.23.1.2");
    verifyRuleMatches(
        Map.of(
            ruleMatches.get("100056"),
            REQUEST_VALUE_MATCH_METADATA_HOST + ":" + MATCH_OPERATOR_MATCHES_REGEX),
        "http.request.header.host",
        "my-home-hostnames");
  }

  private void verifyRuleMatches(
      Map<RuleMatch, String> ruleMatches, String matchAttribute, String matchAttributeValue) {
    ruleMatches.forEach(
        (ruleMatch, ruleMsg) -> {
          assertEquals(ruleMsg, ruleMatch.getRuleMessage());
          assertEquals(matchAttribute, ruleMatch.getMatchAttribute());
          assertEquals(matchAttributeValue, ruleMatch.getMatchAttributeValue());
        });
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
            Map.of(
                RESPONSE_VALUE_MATCH_METADATA_STATUS_CODE,
                "(3|5)[0-9]+2",
                RESPONSE_VALUE_MATCH_METADATA_BODY,
                "(no|some)thing.*me"),
            List.of(MATCH_OPERATOR_MATCHES_REGEX, MATCH_OPERATOR_NOT_MATCH_REGEX)));

    customModsecRules.sort(Comparator.comparing(CustomModsecRule::toString));
    AtomicLong ruleId = new AtomicLong(RULE_ID_SEED);
    return customModsecRules.stream()
        .map(rule -> rule.toBuilder().setRuleId(ruleId.getAndIncrement()).build())
        .collect(Collectors.toList());
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

  private static CustomModsecRule createCustomModsecRule(
      RequestValueMatchMetadata requestMetadata,
      CustomModsecMatchExpression.MatchOperator operator,
      String value,
      long ruleId) {
    return createCustomModsecRule(requestMetadata, operator, value).toBuilder()
        .setRuleId(ruleId)
        .build();
  }

  private static CustomModsecRule createCustomModsecRule(
      ResponseValueMatchMetadata responseMetadata,
      CustomModsecMatchExpression.MatchOperator operator,
      String value,
      long ruleId) {
    return createCustomModsecRule(responseMetadata, operator, value).toBuilder()
        .setRuleId(ruleId)
        .build();
  }
}
