package ai.traceable.modsecurity.rule.conversion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecRule;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecRuleClause;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecValueMatchClause;
import ai.traceable.modsecurity.rule.api.v1.CustomSecRuleClause;
import ai.traceable.modsecurity.rule.api.v1.RequestValueMatchMetadata;
import ai.traceable.modsecurity.rule.conversion.clause.CustomModsecKeyValueMatchClauseConverter;
import ai.traceable.modsecurity.rule.conversion.clause.CustomModsecValueMatchClauseConverter;
import ai.traceable.modsecurity.rule.conversion.clause.ModsecOperatorConverter;
import ai.traceable.modsecurity.rule.conversion.clause.ModsecVariableConverter;
import ai.traceable.modsecurity.utils.ModsecRuleEngineUtils;
import com.google.common.io.Resources;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.Test;

public class ModsecRuleConverterTest {

  private final ModsecVariableConverter variableConverter = new ModsecVariableConverter();
  private final ModsecOperatorConverter operatorConverter = new ModsecOperatorConverter();
  private ModsecRuleConverter modsecRuleConverter =
      new ModsecRuleConverterImpl(
          new CustomModsecValueMatchClauseConverter(variableConverter, operatorConverter),
          new CustomModsecKeyValueMatchClauseConverter(variableConverter, operatorConverter));

  @Test
  public void testConvertedModsecRules() throws Exception {
    List<CustomModsecRule> customModsecRules = new ArrayList<>();
    customModsecRules.addAll(ModsecValueMatchConverterTest.getSampleCustomModsecRules());
    customModsecRules.addAll(ModsecKeyValueMatchConverterTest.getSampleCustomModsecRules());

    String fileRulesBlob =
        Resources.toString(
            this.getClass()
                .getClassLoader()
                .getResource("conversion/sample-custom-modsec-rules.conf"),
            StandardCharsets.UTF_8);
    customModsecRules.sort(Comparator.comparing(CustomModsecRule::toString));
    String convertedModsecRulesBlob = modsecRuleConverter.getModsecRulesBlob(customModsecRules);

    assertEquals(fileRulesBlob, convertedModsecRulesBlob);
    if (SystemUtils.IS_OS_LINUX) {
      assertEquals(Status.OK, ModsecRuleEngineUtils.validate(convertedModsecRulesBlob));
    }
  }

  @Test
  void testCustomSecRules() throws Exception {
    List<CustomModsecRule> customModsecRules = new ArrayList<>();
    customModsecRules.add(
        CustomModsecRule.newBuilder()
            .setRuleMsg("msg1")
            .setRuleUuid("uuid1")
            .setLogMessage("log1")
            .setRuleId(12345)
            .addAndClauses(
                CustomModsecRuleClause.newBuilder()
                    .setCustomSecRuleClause(
                        CustomSecRuleClause.newBuilder()
                            .setInputSecRule(
                                "SecRule REQUEST_HEADERS \"@rx (\\\\u[0-9a-fA-F]{4}){4,}\" \\\n"
                                    + "    \"id:9210104,\\\n"
                                    + "    phase:1,\\\n"
                                    + "    nolog,\\\n"
                                    + "    noauditlog,\\\n"
                                    + "    capture,\\\n"
                                    + "    tag:'traceable/rank/1',\\\n"
                                    + "    tag:'traceable/severity/HIGH',\\\n"
                                    + "    setvar:TX.MATCH_NAME=%{MATCHED_VAR_NAME},\\\n"
                                    + "    chain\"\n"
                                    + "    SecRule TX:0 \"@rx .*\" \\\n"
                                    + "        \"t:none,t:urlDecodeUni,t:htmlEntityDecode,t:jsDecode,\\\n"
                                    + "        setvar:'tx.unicodeencoded_%{TX.MATCH_NAME}=%{MATCHED_VAR}'\"")))
            .build());

    String convertedModsecRulesBlob = modsecRuleConverter.getModsecRulesBlob(customModsecRules);
    String expectedModsecRulesBlob =
        "SecRule REQUEST_HEADERS \"@rx (\\\\u[0-9a-fA-F]{4}){4,}\" \\\n"
            + "    \"tag:'rule-uuid/uuid1',logdata:'log1',msg:'msg1',id:12345,\\\n"
            + "    phase:1,\\\n"
            + "    nolog,\\\n"
            + "    noauditlog,\\\n"
            + "    capture,\\\n"
            + "    tag:'traceable/rank/1',\\\n"
            + "    tag:'traceable/severity/HIGH',\\\n"
            + "    setvar:TX.MATCH_NAME=%{MATCHED_VAR_NAME},\\\n"
            + "    chain\"\n"
            + "    SecRule TX:0 \"@rx .*\" \\\n"
            + "        \"t:none,t:urlDecodeUni,t:htmlEntityDecode,t:jsDecode,\\\n"
            + "        setvar:'tx.unicodeencoded_%{TX.MATCH_NAME}=%{MATCHED_VAR}'\"";

    assertEquals(expectedModsecRulesBlob, convertedModsecRulesBlob);

    customModsecRules.clear();
    customModsecRules.add(
        CustomModsecRule.newBuilder()
            .setRuleMsg("msg1")
            .setRuleUuid("uuid1")
            .setLogMessage("log1")
            .setRuleId(12345)
            .addAndClauses(
                CustomModsecRuleClause.newBuilder()
                    .setValueMatchClause(
                        CustomModsecValueMatchClause.newBuilder()
                            .setRequestValueMetadata(
                                RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_HTTP_METHOD)
                            .setValueMatchExpression(
                                CustomModsecMatchExpression.newBuilder()
                                    .setMatchValue("abc")
                                    .setValueMatchOperator(
                                        CustomModsecMatchExpression.MatchOperator
                                            .MATCH_OPERATOR_EQUALS)
                                    .build())
                            .build()))
            .addAndClauses(
                CustomModsecRuleClause.newBuilder()
                    .setCustomSecRuleClause(
                        CustomSecRuleClause.newBuilder()
                            .setInputSecRule(
                                "SecRule REQUEST_HEADERS \"@rx (\\\\u[0-9a-fA-F]{4}){4,}\" \\\n"
                                    + "    \"id:9210104,\\\n"
                                    + "    phase:1,\\\n"
                                    + "    nolog,\\\n"
                                    + "    noauditlog,\\\n"
                                    + "    capture,\\\n"
                                    + "    tag:'traceable/rank/1',\\\n"
                                    + "    tag:'traceable/severity/HIGH',\\\n"
                                    + "    setvar:TX.MATCH_NAME=%{MATCHED_VAR_NAME},\\\n"
                                    + "    chain\"\n"
                                    + "    SecRule TX:0 \"@rx .*\" \\\n"
                                    + "        \"t:none,t:urlDecodeUni,t:htmlEntityDecode,t:jsDecode,\\\n"
                                    + "        setvar:'tx.unicodeencoded_%{TX.MATCH_NAME}=%{MATCHED_VAR}'\"")))
            .build());
    convertedModsecRulesBlob = modsecRuleConverter.getModsecRulesBlob(customModsecRules);
    expectedModsecRulesBlob =
        "SecRule REQUEST_METHOD \"@streq abc\" \"id:12345,phase:2,capture,t:none,msg:'msg1',logdata:'log1',tag:'CUSTOM_SIGNATURE',tag:'paranoia-level/1',tag:'rule-uuid/uuid1',severity:'CRITICAL',chain\"\n"
            + "SecRule REQUEST_HEADERS \"@rx (\\\\u[0-9a-fA-F]{4}){4,}\" \\\n"
            + "    \"\\\n"
            + "    phase:1,\\\n"
            + "    nolog,\\\n"
            + "    noauditlog,\\\n"
            + "    capture,\\\n"
            + "    tag:'traceable/rank/1',\\\n"
            + "    tag:'traceable/severity/HIGH',\\\n"
            + "    setvar:TX.MATCH_NAME=%{MATCHED_VAR_NAME},\\\n"
            + "    chain\"\n"
            + "    SecRule TX:0 \"@rx .*\" \\\n"
            + "        \"t:none,t:urlDecodeUni,t:htmlEntityDecode,t:jsDecode,\\\n"
            + "        setvar:'tx.unicodeencoded_%{TX.MATCH_NAME}=%{MATCHED_VAR}'\"";
    assertEquals(expectedModsecRulesBlob, convertedModsecRulesBlob);

    customModsecRules.clear();
    customModsecRules.add(
        CustomModsecRule.newBuilder()
            .setRuleMsg("msg1")
            .setRuleUuid("uuid1")
            .setLogMessage("log1")
            .setRuleId(12345)
            .addAndClauses(
                CustomModsecRuleClause.newBuilder()
                    .setCustomSecRuleClause(
                        CustomSecRuleClause.newBuilder()
                            .setInputSecRule(
                                "SecRule REQUEST_HEADERS \"@rx (\\\\u[0-9a-fA-F]{4}){4,}\" \\\n"
                                    + "    \"id:9210104,\\\n"
                                    + "    phase:1,\\\n"
                                    + "    nolog,\\\n"
                                    + "    noauditlog,\\\n"
                                    + "    capture,\\\n"
                                    + "    msg:'rule-msg',\\\n"
                                    + "    tag:'traceable/rank/1',\\\n"
                                    + "    tag:'traceable/severity/HIGH',\\\n"
                                    + "    setvar:TX.MATCH_NAME=%{MATCHED_VAR_NAME},\\\n"
                                    + "    chain\"\n"
                                    + "    SecRule TX:0 \"@rx .*\" \\\n"
                                    + "        \"t:none,t:urlDecodeUni,t:htmlEntityDecode,t:jsDecode,\\\n"
                                    + "        setvar:'tx.unicodeencoded_%{TX.MATCH_NAME}=%{MATCHED_VAR}'\"")))
            .build());
    convertedModsecRulesBlob = modsecRuleConverter.getModsecRulesBlob(customModsecRules);
    expectedModsecRulesBlob =
        "SecRule REQUEST_HEADERS \"@rx (\\\\u[0-9a-fA-F]{4}){4,}\" \\\n"
            + "    \"tag:'rule-uuid/uuid1',logdata:'log1',id:12345,\\\n"
            + "    phase:1,\\\n"
            + "    nolog,\\\n"
            + "    noauditlog,\\\n"
            + "    capture,\\\n"
            + "    msg:'msg1',\\\n"
            + "    tag:'traceable/rank/1',\\\n"
            + "    tag:'traceable/severity/HIGH',\\\n"
            + "    setvar:TX.MATCH_NAME=%{MATCHED_VAR_NAME},\\\n"
            + "    chain\"\n"
            + "    SecRule TX:0 \"@rx .*\" \\\n"
            + "        \"t:none,t:urlDecodeUni,t:htmlEntityDecode,t:jsDecode,\\\n"
            + "        setvar:'tx.unicodeencoded_%{TX.MATCH_NAME}=%{MATCHED_VAR}'\"";
    assertEquals(expectedModsecRulesBlob, convertedModsecRulesBlob);

    customModsecRules.clear();
    customModsecRules.add(
        CustomModsecRule.newBuilder()
            .setRuleMsg("msg1")
            .setRuleUuid("uuid1")
            .setLogMessage("log1")
            .setRuleId(12345)
            .addAndClauses(
                CustomModsecRuleClause.newBuilder()
                    .setCustomSecRuleClause(
                        CustomSecRuleClause.newBuilder()
                            .setInputSecRule(
                                "SecRule REQUEST_HEADERS \"@rx (\\\\u[0-9a-fA-F]{4}){4,}\" \\\n"
                                    + "    \"id:9210104,\\\n"
                                    + "    phase:1,\\\n"
                                    + "    nolog,\\\n"
                                    + "    noauditlog,\\\n"
                                    + "    capture,\\\n"
                                    + "    tag:'traceable/rank/1',\\\n"
                                    + "    tag:'traceable/severity/HIGH',\\\n"
                                    + "    setvar:TX.MATCH_NAME=%{MATCHED_VAR_NAME},\\\n"
                                    + "    chain\"\n"
                                    + "    SecRule TX:0 \"@rx .*\" \\\n"
                                    + "        \"t:none,t:urlDecodeUni,t:htmlEntityDecode,t:jsDecode,\\\n"
                                    + "        setvar:'tx.unicodeencoded_%{TX.MATCH_NAME}=%{MATCHED_VAR}'\"")))
            .addAndClauses(
                CustomModsecRuleClause.newBuilder()
                    .setCustomSecRuleClause(
                        CustomSecRuleClause.newBuilder()
                            .setInputSecRule(
                                "SecRule REQUEST_URI|REQUEST_FILENAME|ARGS|REQUEST_BODY \"@rx (\\\\u[0-9a-fA-F]{4}){4,}\" \\\n"
                                    + "    \"id:9210105,\\\n"
                                    + "    phase:2,\\\n"
                                    + "    nolog,\\\n"
                                    + "    noauditlog,\\\n"
                                    + "    capture,\\\n"
                                    + "    tag:'traceable/rank/2',\\\n"
                                    + "    tag:'traceable/severity/HIGH',\\\n"
                                    + "    setvar:TX.MATCH_NAME=%{MATCHED_VAR_NAME},\\\n"
                                    + "    chain\"\n"
                                    + "    SecRule TX:0 \"@rx .*\" \\\n"
                                    + "        \"t:none,t:urlDecodeUni,t:htmlEntityDecode,t:jsDecode,\\\n"
                                    + "        setvar:'tx.unicodeencoded_%{TX.MATCH_NAME}=%{MATCHED_VAR}'\"")))
            .addAndClauses(
                CustomModsecRuleClause.newBuilder()
                    .setValueMatchClause(
                        CustomModsecValueMatchClause.newBuilder()
                            .setRequestValueMetadata(
                                RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_HTTP_METHOD)
                            .setValueMatchExpression(
                                CustomModsecMatchExpression.newBuilder()
                                    .setMatchValue("abc")
                                    .setValueMatchOperator(
                                        CustomModsecMatchExpression.MatchOperator
                                            .MATCH_OPERATOR_EQUALS)
                                    .build())
                            .build()))
            .build());
    convertedModsecRulesBlob = modsecRuleConverter.getModsecRulesBlob(customModsecRules);
    expectedModsecRulesBlob =
        "SecRule REQUEST_HEADERS \"@rx (\\\\u[0-9a-fA-F]{4}){4,}\" \\\n"
            + "    \"tag:'rule-uuid/uuid1',logdata:'log1',msg:'msg1',id:12345,\\\n"
            + "    phase:1,\\\n"
            + "    nolog,\\\n"
            + "    noauditlog,\\\n"
            + "    capture,\\\n"
            + "    tag:'traceable/rank/1',\\\n"
            + "    tag:'traceable/severity/HIGH',\\\n"
            + "    setvar:TX.MATCH_NAME=%{MATCHED_VAR_NAME},\\\n"
            + "    chain\"\n"
            + "    SecRule TX:0 \"@rx .*\" \\\n"
            + "        \"t:none,t:urlDecodeUni,t:htmlEntityDecode,t:jsDecode,\\\n"
            + "        setvar:'tx.unicodeencoded_%{TX.MATCH_NAME}=%{MATCHED_VAR}',chain\"\n"
            + "SecRule REQUEST_URI|REQUEST_FILENAME|ARGS|REQUEST_BODY \"@rx (\\\\u[0-9a-fA-F]{4}){4,}\" \\\n"
            + "    \"\\\n"
            + "    phase:2,\\\n"
            + "    nolog,\\\n"
            + "    noauditlog,\\\n"
            + "    capture,\\\n"
            + "    tag:'traceable/rank/2',\\\n"
            + "    tag:'traceable/severity/HIGH',\\\n"
            + "    setvar:TX.MATCH_NAME=%{MATCHED_VAR_NAME},\\\n"
            + "    chain\"\n"
            + "    SecRule TX:0 \"@rx .*\" \\\n"
            + "        \"t:none,t:urlDecodeUni,t:htmlEntityDecode,t:jsDecode,\\\n"
            + "        setvar:'tx.unicodeencoded_%{TX.MATCH_NAME}=%{MATCHED_VAR}',chain\"\n"
            + "SecRule REQUEST_METHOD \"@streq abc\" \"capture,block,t:none\"";
    assertEquals(expectedModsecRulesBlob, convertedModsecRulesBlob);

    List<CustomModsecRule> modsecRules =
        List.of(
            CustomModsecRule.newBuilder()
                .setRuleMsg("msg1")
                .setRuleUuid("uuid1")
                .setLogMessage("log1")
                .setRuleId(12345)
                .addAndClauses(
                    CustomModsecRuleClause.newBuilder()
                        .setCustomSecRuleClause(
                            CustomSecRuleClause.newBuilder()
                                .setInputSecRule(
                                    "SecRule REQUEST_HEADERS \"@rx (\\\\u[0-9a-fA-F]{4}){4,}\" \\\n"
                                        + "    \"id:9210104,\\\n"
                                        + "    phase:1,\\\n"
                                        + "    nolog,\\\n"
                                        + "    noauditlog,\\\n"
                                        + "    capture,\\\n"
                                        + "    tag:'traceable/rank/1',\\\n"
                                        + "    tag:'traceable/severity/HIGH',\\\n"
                                        + "    setvar:TX.MATCH_NAME=%{MATCHED_VAR_NAME},\\\n"
                                        + "    chain\"\n"
                                        + "    SecRule TX:0 \"@rx .*\" \\\n"
                                        + "        \"t:none,t:urlDecodeUni,t:htmlEntityDecode,t:jsDecode,\\\n"
                                        + "logdata:'Found rds_data in encoded sign parameter with value - %{TX.0}'\\\n"
                                        + "        setvar:'tx.unicodeencoded_%{TX.MATCH_NAME}=%{MATCHED_VAR}'\"")))
                .build());
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          modsecRuleConverter.getModsecRulesBlob(modsecRules);
        });
  }
}
