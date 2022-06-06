package ai.traceable.customsignature.config.service.modsec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import ai.traceable.customsignature.config.service.modsec.registry.ModsecActions;
import ai.traceable.customsignature.config.service.modsec.registry.ModsecRuleMappings;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.KeyValueExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ModsecRuleConversionTest {

  @Test
  public void testGetModsecRuleExceptions() {
    ModsecRuleMappings modsecRuleMappings = mock(ModsecRuleMappings.class);
    ModsecRuleConversion modsecRuleConversion = new ModsecRuleConversion(modsecRuleMappings);

    {
      assertThrows(
          UnsupportedOperationException.class,
          () ->
              modsecRuleConversion
                  .getModsecRuleForANDClauses(
                      Collections.singletonList(Clause.newBuilder().build()),
                      new ModsecActions(100001, "ruleId", "msg"))
                  .isEmpty());
      verifyNoInteractions(modsecRuleMappings);
    }
    {
      doThrow(new UnsupportedOperationException())
          .when(modsecRuleMappings)
          .getVariableString(any(), any());
      doReturn("xyz").when(modsecRuleMappings).getOperatorString(any(), any());

      assertThrows(
          UnsupportedOperationException.class,
          () ->
              modsecRuleConversion
                  .getModsecRuleForANDClauses(
                      Collections.singletonList(
                          Clause.newBuilder()
                              .setMatchExpression(MatchExpression.newBuilder().build())
                              .build()),
                      new ModsecActions(100001, "ruleId", "msg"))
                  .isEmpty());
      verify(modsecRuleMappings, times(1)).getVariableString(any(), any());
      verify(modsecRuleMappings, times(0)).getOperatorString(any(), any());
    }
    {
      reset(modsecRuleMappings);
      doReturn("xyz").when(modsecRuleMappings).getVariableString(any(), any());
      doThrow(new UnsupportedOperationException())
          .when(modsecRuleMappings)
          .getOperatorString(any(), any());
      ;

      assertThrows(
          UnsupportedOperationException.class,
          () ->
              modsecRuleConversion
                  .getModsecRuleForANDClauses(
                      Collections.singletonList(
                          Clause.newBuilder()
                              .setMatchExpression(MatchExpression.newBuilder().build())
                              .build()),
                      new ModsecActions(100001, "ruleId", "msg"))
                  .isEmpty());
      verify(modsecRuleMappings, times(1)).getVariableString(any(), any());
      verify(modsecRuleMappings, times(1)).getOperatorString(any(), any());
    }
  }

  @Test
  public void testGetModsecRule() {
    ModsecRuleConversion modsecRuleConversion = new ModsecRuleConversion(new ModsecRuleMappings());

    assertEquals(
        "SecRule "
            + "REQUEST_HEADERS:Host|REQUEST_HEADERS:x-forwarded-host|REQUEST_HEADERS:forwarded "
            + "\"@streq 127.0.0.1\" "
            + "\"id:10000005,phase:2,capture,block,t:none,"
            + "msg:'MATCH_KEY_HOST : MATCH_OPERATOR_EQUALS',"
            + "logdata:'Matched Data: %{TX.0} found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}',"
            + "tag:'CUSTOM_SIGNATURE',tag:'paranoia-level/1',"
            + "tag:'rule-uuid/21d38251-5986-5a04-b7a0-b32068437ccb',"
            + "severity:'CRITICAL'\"",
        modsecRuleConversion.getModsecRuleForANDClauses(
            List.of(
                Clause.newBuilder()
                    .setMatchExpression(
                        MatchExpression.newBuilder()
                            .setMatchKey(MatchKey.MATCH_KEY_HOST)
                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                            .setMatchValue("127.0.0.1")
                            .build())
                    .build()),
            new ModsecActions(
                10000005,
                "21d38251-5986-5a04-b7a0-b32068437ccb",
                "MATCH_KEY_HOST : MATCH_OPERATOR_EQUALS")));

    assertEquals(
        "SecRule REQUEST_URI_RAW \"@streq /foo\" "
            + "\"id:10000105,phase:2,capture,t:none,"
            + "msg:'Chained rule with ids: 10000001 , 10000011 , 10000081 , 10000101',"
            + "logdata:'Matched Data: %{TX.0} found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}',"
            + "tag:'CUSTOM_SIGNATURE',tag:'paranoia-level/1',"
            + "tag:'rule-uuid/4d1e7177-4e54-5a8e-a985-ad1308304605',severity:'CRITICAL',chain\"\n"
            + "SecRule REQUEST_METHOD \"@contains post\" \"capture,t:none,chain\"\n"
            + "SecRule REQUEST_HEADERS:x-real \"@rx .*\\<script\\>.*\" \"capture,t:none,chain\"\n"
            + "SecRule ARGS:/paramLevel/ \"@gt 5\" \"capture,block,t:none\"",
        modsecRuleConversion.getModsecRuleForANDClauses(
            List.of(
                Clause.newBuilder()
                    .setMatchExpression(
                        MatchExpression.newBuilder()
                            .setMatchKey(MatchKey.MATCH_KEY_URL)
                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                            .setMatchValue("/foo")
                            .build())
                    .build(),
                Clause.newBuilder()
                    .setMatchExpression(
                        MatchExpression.newBuilder()
                            .setMatchKey(MatchKey.MATCH_KEY_HTTP_METHOD)
                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_CONTAINS)
                            .setMatchValue("post")
                            .build())
                    .build(),
                Clause.newBuilder()
                    .setKeyValueExpression(
                        KeyValueExpression.newBuilder()
                            .setTag(KeyValueTag.KEY_VALUE_TAG_HEADER)
                            .setKeyMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                            .setMatchKey("x-real")
                            .setValueMatchOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                            .setMatchValue(".*\\<script\\>.*")
                            .build())
                    .build(),
                Clause.newBuilder()
                    .setKeyValueExpression(
                        KeyValueExpression.newBuilder()
                            .setTag(KeyValueTag.KEY_VALUE_TAG_PARAMETER)
                            .setKeyMatchOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                            .setMatchKey("paramLevel")
                            .setValueMatchOperator(MatchOperator.MATCH_OPERATOR_GREATER_THAN)
                            .setMatchValue("5")
                            .build())
                    .build()),
            new ModsecActions(
                10000105,
                "4d1e7177-4e54-5a8e-a985-ad1308304605",
                "Chained rule with ids: 10000001 , 10000011 , 10000081 , 10000101")));

    assertEquals(
        "SecRule "
            + "REQUEST_HEADERS:User-Agent \"!@contains Chrome\" "
            + "\"id:10000106,phase:2,capture,t:none,"
            + "msg:'Chained rule with ids: 10000016 , 10000096',"
            + "logdata:'Matched Data: %{TX.0} found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}',"
            + "tag:'CUSTOM_SIGNATURE',tag:'paranoia-level/1',"
            + "tag:'rule-uuid/3c7f7064-013a-5b33-ae02-4ae202acea60',"
            + "severity:'CRITICAL',chain\"\n"
            + "SecRule ARGS|!ARGS:/^(param)[a-s1-9_-]{3,16}$/ \"!@rx ^\\d+$\" "
            + "\"capture,block,t:none\"",
        modsecRuleConversion.getModsecRuleForANDClauses(
            List.of(
                Clause.newBuilder()
                    .setMatchExpression(
                        MatchExpression.newBuilder()
                            .setMatchKey(MatchKey.MATCH_KEY_USER_AGENT)
                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_NOT_CONTAIN)
                            .setMatchValue("Chrome")
                            .build())
                    .build(),
                Clause.newBuilder()
                    .setKeyValueExpression(
                        KeyValueExpression.newBuilder()
                            .setTag(KeyValueTag.KEY_VALUE_TAG_PARAMETER)
                            .setKeyMatchOperator(MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX)
                            .setMatchKey("^(param)[a-s1-9_-]{3,16}$")
                            .setValueMatchOperator(MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX)
                            .setMatchValue("^\\d+$")
                            .build())
                    .build()),
            new ModsecActions(
                10000106,
                "3c7f7064-013a-5b33-ae02-4ae202acea60",
                "Chained rule with ids: 10000016 , 10000096")));
  }

  @Test
  void testModsecResponseRule() {
    ModsecRuleConversion modsecRuleConversion = new ModsecRuleConversion(new ModsecRuleMappings());
    assertEquals(
        "SecRule "
            + "RESPONSE_STATUS "
            + "\"@streq 200\" "
            + "\"id:10000005,phase:4,capture,block,t:none,"
            + "msg:'MATCH_KEY_STATUS_CODE : MATCH_OPERATOR_EQUALS',"
            + "logdata:'Matched Data: %{TX.0} found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}',"
            + "tag:'CUSTOM_SIGNATURE',tag:'paranoia-level/1',"
            + "tag:'rule-uuid/21d38251-5986-5a04-b7a0-b32068437ccb',"
            + "severity:'CRITICAL'\"",
        modsecRuleConversion.getModsecRuleForANDClauses(
            List.of(
                Clause.newBuilder()
                    .setMatchExpression(
                        MatchExpression.newBuilder()
                            .setMatchKey(MatchKey.MATCH_KEY_STATUS_CODE)
                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                            .setMatchValue("200")
                            .setMatchCategory(MatchCategory.MATCH_CATEGORY_RESPONSE)
                            .build())
                    .build()),
            new ModsecActions(
                10000005,
                "21d38251-5986-5a04-b7a0-b32068437ccb",
                "MATCH_KEY_STATUS_CODE : MATCH_OPERATOR_EQUALS")));

    assertEquals(
        "SecRule RESPONSE_HEADERS_NAMES \"@streq cookie\" "
            + "\"id:10000105,phase:4,capture,t:none,"
            + "msg:'Chained rule with ids: 10000001 , 10000011 , 10000081',"
            + "logdata:'Matched Data: %{TX.0} found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}',"
            + "tag:'CUSTOM_SIGNATURE',tag:'paranoia-level/1',"
            + "tag:'rule-uuid/4d1e7177-4e54-5a8e-a985-ad1308304605',severity:'CRITICAL',chain\"\n"
            + "SecRule REQUEST_METHOD \"@contains post\" \"capture,t:none,chain\"\n"
            + "SecRule RESPONSE_HEADERS:content-type \"@streq text/html\" \"capture,block,t:none\"",
        modsecRuleConversion.getModsecRuleForANDClauses(
            List.of(
                Clause.newBuilder()
                    .setMatchExpression(
                        MatchExpression.newBuilder()
                            .setMatchKey(MatchKey.MATCH_KEY_HEADER_NAME)
                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                            .setMatchValue("cookie")
                            .setMatchCategory(MatchCategory.MATCH_CATEGORY_RESPONSE)
                            .build())
                    .build(),
                Clause.newBuilder()
                    .setMatchExpression(
                        MatchExpression.newBuilder()
                            .setMatchKey(MatchKey.MATCH_KEY_HTTP_METHOD)
                            .setMatchOperator(MatchOperator.MATCH_OPERATOR_CONTAINS)
                            .setMatchValue("post")
                            .build())
                    .build(),
                Clause.newBuilder()
                    .setKeyValueExpression(
                        KeyValueExpression.newBuilder()
                            .setTag(KeyValueTag.KEY_VALUE_TAG_HEADER)
                            .setKeyMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                            .setMatchKey("content-type")
                            .setValueMatchOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                            .setMatchValue("text/html")
                            .setMatchCategory(MatchCategory.MATCH_CATEGORY_RESPONSE)
                            .build())
                    .build()),
            new ModsecActions(
                10000105,
                "4d1e7177-4e54-5a8e-a985-ad1308304605",
                "Chained rule with ids: 10000001 , 10000011 , 10000081")));
  }
}
