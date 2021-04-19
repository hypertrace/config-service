package ai.traceable.customsignature.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EventSeverity;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.KeyValueExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

public class CustomSignatureRuleConverterTest {

  @Test
  public void testConvert() throws InvalidProtocolBufferException {
    CustomSignatureRuleConverter ruleConverter = new CustomSignatureRuleConverter();
    RuleEffect ruleEffect =
        RuleEffect.newBuilder()
            .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
            .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
            .build();
    RuleDefinition ruleDefinition =
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
    CustomSignatureRule rule =
        CustomSignatureRule.newBuilder()
            .setId("id")
            .setName("name")
            .setEffect(ruleEffect)
            .setDefinition(ruleDefinition)
            .build();

    Value config = ruleConverter.convert(rule);
    assertTrue(config.hasStructValue());
    assertEquals(rule, ruleConverter.convert(config));
  }
}
