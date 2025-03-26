package ai.traceable.modsecurity.rule.secrule.actions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ModsecActionsTest {
  private static final String INPUT_SEC_RULE_1 =
      "SecRule REQUEST_HEADERS:secrulealert \"@rx secrulealert\" \\\n"
          + "    \"id:92100120,\\\n"
          + "    phase:2,\\\n"
          + "    block,\\\n"
          + "    msg:'Test Sec Rule',\\\n"
          + "    logdata:'Matched Data: %{TX.0} found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}',\\\n"
          + "    tag:'attack-protocol',\\\n"
          + "    tag:'traceable/labels/OWASP_2021:A4,CWE:444,OWASP_API_2019:API8',\\\n"
          + "    tag:'traceable/severity/HIGH',\\\n"
          + "    tag:'traceable/type/safe,block',\\\n"
          + "    severity:'CRITICAL',\\\n"
          + "    setvar:'tx.anomaly_score_pl1=+%{tx.critical_anomaly_score}'\"";

  private static final String INPUT_SEC_RULE_2 =
      "SecRule REQUEST_HEADERS:secruleblock \"@rx secruleblock\" \\\n"
          + "    \"id:92100120,\\\n"
          + "    phase:2,\\\n"
          + "    block,\\\n"
          + "    msg:'Test Sec Rule',\\\n"
          + "    logdata:'Matched Data: %{TX.0} found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}',\\\n"
          + "    tag:'attack-protocol',\\\n"
          + "    tag:'traceable/labels/OWASP_2021:A4,CWE:444,OWASP_API_2019:API8',\\\n"
          + "    tag:'traceable/severity/HIGH',\\\n"
          + "    tag:'traceable/type/safe,pass',\\\n"
          + "    severity:'CRITICAL',\\\n"
          + "    setvar:'tx.anomaly_score_pl1=+%{tx.critical_anomaly_score}'\"";

  private static final String EXPECTED_OUTPUT_1 =
      "SecRule REQUEST_HEADERS:secrulealert \"@rx secrulealert\" \\\n"
          + "    \"tag:'rule-uuid/rule-uuid-1',id:10000000,\\\n"
          + "    phase:2,\\\n"
          + "    block,\\\n"
          + "    msg:'TestModifiedActionsStringInSecrule-CreateAlertRule',\\\n"
          + "    logdata:'Matched Data: %{TX.0} found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}',\\\n"
          + "    tag:'attack-protocol',\\\n"
          + "    tag:'traceable/labels/OWASP_2021:A4,CWE:444,OWASP_API_2019:API8',\\\n"
          + "    tag:'traceable/severity/HIGH',\\\n"
          + "    tag:'traceable/type/safe,block',\\\n"
          + "    severity:'CRITICAL',\\\n"
          + "    setvar:'tx.anomaly_score_pl1=+%{tx.critical_anomaly_score}'\"";

  private static final String EXPECTED_OUTPUT_2 =
      "SecRule REQUEST_HEADERS:secruleblock \"@rx secruleblock\" \\\n"
          + "    \"tag:'rule-uuid/rule-uuid-2',id:10000000,\\\n"
          + "    phase:2,\\\n"
          + "    block,\\\n"
          + "    msg:'TestModifiedActionsStringInSecrule-CreateBlockRule',\\\n"
          + "    logdata:'Matched Data: %{TX.0} found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}',\\\n"
          + "    tag:'attack-protocol',\\\n"
          + "    tag:'traceable/labels/OWASP_2021:A4,CWE:444,OWASP_API_2019:API8',\\\n"
          + "    tag:'traceable/severity/HIGH',\\\n"
          + "    tag:'traceable/type/safe,pass',\\\n"
          + "    severity:'CRITICAL',\\\n"
          + "    setvar:'tx.anomaly_score_pl1=+%{tx.critical_anomaly_score}'\"";

  private static Stream<Arguments> provideTestCases() {
    return Stream.of(
        Arguments.of(
            INPUT_SEC_RULE_1,
            EXPECTED_OUTPUT_1,
            "rule-uuid-1",
            "TestModifiedActionsStringInSecrule-CreateAlertRule"),
        Arguments.of(
            INPUT_SEC_RULE_2,
            EXPECTED_OUTPUT_2,
            "rule-uuid-2",
            "TestModifiedActionsStringInSecrule-CreateBlockRule"));
  }

  @ParameterizedTest
  @MethodSource("provideTestCases")
  void testModifyActionsStringInSecRule(
      String inputSecrule, String expectedOutput, String ruleUuid, String msg) {
    ModsecActions modsecActions =
        new ModsecActions(10000000L, ruleUuid, msg, Optional.empty(), false);
    String result =
        modsecActions.modifyActionsStringInSecRule(inputSecrule, ModsecActionsType.SINGULAR);
    assertEquals(expectedOutput, result);
  }
}
