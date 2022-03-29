package ai.traceable.external.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTransformation;
import ai.traceable.sensitivedata.config.service.v1.Condition;
import ai.traceable.sensitivedata.config.service.v1.Condition.AttributeRegexMatch;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import java.util.List;
import org.junit.jupiter.api.Test;

public class RedactionRulesTranslatorTest {
  private final RedactionRulesTranslator redactionRulesTranslator = new RedactionRulesTranslator();

  @Test
  void translateRedactionRulesTest() {
    RedactionRule rule1 =
        RedactionRule.newBuilder()
            .setId("id-1")
            .setSessionIdentifier(true)
            .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_REDACT)
            .setRegex("regex-1")
            .setFqn(true)
            .addConditions(
                Condition.newBuilder()
                    .setAttributeRegexMatch(
                        AttributeRegexMatch.newBuilder().setKey("key-1").setRegex("reg-1")))
            .build();

    RedactionRule rule2 =
        RedactionRule.newBuilder()
            .setId("id-2")
            .setSessionIdentifier(false)
            .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_HASH)
            .setRegex("regex-2")
            .setFqn(false)
            .addConditions(
                Condition.newBuilder()
                    .setAttributeRegexMatch(
                        AttributeRegexMatch.newBuilder().setKey("prefix").setRegex("reg-2")))
            .build();

    RedactionRule rule3 =
        RedactionRule.newBuilder()
            .setId("id-3")
            .setSessionIdentifier(true)
            .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_RAW)
            .setFqn(true)
            .setRegex("regex-3")
            .addConditions(
                Condition.newBuilder()
                    .setAttributeRegexMatch(
                        AttributeRegexMatch.newBuilder().setKey("key-3").setRegex("reg-3")))
            .build();

    List<DataType> translatedDataTypes =
        this.redactionRulesTranslator.translateRedactionRules(List.of(rule1, rule2, rule3));
    assertEquals(
        DataTransformation.DATA_TRANSFORMATION_REDACT,
        translatedDataTypes.get(0).getTransformation());
    assertEquals(
        DataTransformation.DATA_TRANSFORMATION_OBFUSCATE,
        translatedDataTypes.get(1).getTransformation());
    assertEquals(
        DataTransformation.DATA_TRANSFORMATION_UNSPECIFIED,
        translatedDataTypes.get(2).getTransformation());

    assertEquals(
        "regex-1", translatedDataTypes.get(0).getMatchRules(0).getKeyPredicate().getValue());

    assertEquals(
        "key-1",
        translatedDataTypes
            .get(0)
            .getMatchRules(0)
            .getSpanFilter()
            .getRequiredMatchingAttributes(0)
            .getKeyPredicate()
            .getValue());
    assertEquals(
        "reg-1",
        translatedDataTypes
            .get(0)
            .getMatchRules(0)
            .getSpanFilter()
            .getRequiredMatchingAttributes(0)
            .getValuePredicate()
            .getValue());
    assertEquals(
        "prefix", translatedDataTypes.get(1).getMatchRules(0).getAttributeFilter().getPrefixes(0));
  }
}
