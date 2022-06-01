package ai.traceable.external.data.classification.config.service;

import static ai.traceable.external.data.classification.config.service.v1.Operator.OPERATOR_EQUALS;
import static ai.traceable.external.data.classification.config.service.v1.Operator.OPERATOR_MATCHES_REGEX;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_HASH;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_REDACT;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.external.data.classification.config.service.v1.AttributeFilter;
import ai.traceable.external.data.classification.config.service.v1.AttributePredicate;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTransformation;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTypeMatchRule;
import ai.traceable.external.data.classification.config.service.v1.DataType.Result;
import ai.traceable.external.data.classification.config.service.v1.PathPredicate;
import ai.traceable.external.data.classification.config.service.v1.SpanFilter;
import ai.traceable.external.data.classification.config.service.v1.StringPredicate;
import ai.traceable.sensitivedata.config.service.v1.Condition;
import ai.traceable.sensitivedata.config.service.v1.Condition.AttributeRegexMatch;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.Parameter;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class RedactionRulesTranslatorTest {
  private final RedactionRulesTranslator redactionRulesTranslator = new RedactionRulesTranslator();
  private static final String LEGACY_SENSITIVE_HEADERS_DATA_TYPE_ID =
      "legacy-datatype-sensitive-headers-id";

  @Test
  void translateRedactionRulesTest() {
    RedactionRule rule1 =
        RedactionRule.newBuilder()
            .setId("id-1")
            .setSessionIdentifier(false)
            .setRedactionStrategy(REDACTION_STRATEGY_REDACT)
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
            .setRedactionStrategy(REDACTION_STRATEGY_HASH)
            .setRegex("regex-2")
            .setFqn(false)
            .addConditions(
                Condition.newBuilder()
                    .setAttributeRegexMatch(
                        AttributeRegexMatch.newBuilder().setKey("key-2").setRegex("reg-2")))
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
        this.redactionRulesTranslator.translateRedactionRules(
            List.of(rule1, rule2, rule3),
            Set.of(REDACTION_STRATEGY_HASH, REDACTION_STRATEGY_REDACT));

    assertEquals(3, translatedDataTypes.size());

    DataType expectedRule1 =
        DataType.newBuilder()
            .setDataTypeId("id-1")
            .setSessionIdentifier(false)
            .setTransformation(DataTransformation.DATA_TRANSFORMATION_REDACT)
            .addMatchRules(
                DataTypeMatchRule.newBuilder()
                    .setSpanFilter(
                        SpanFilter.newBuilder()
                            .addRequiredMatchingAttributes(
                                AttributePredicate.newBuilder()
                                    .setNamePredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(OPERATOR_EQUALS)
                                            .setValue("key-1"))
                                    .setValuePredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(OPERATOR_MATCHES_REGEX)
                                            .setValue("reg-1"))))
                    .setResult(Result.RESULT_MATCH)
                    .setAttributeFilter(AttributeFilter.getDefaultInstance())
                    .setPathPredicate(
                        PathPredicate.newBuilder()
                            .setPathSegmentPredicate(
                                StringPredicate.newBuilder()
                                    .setOperator(OPERATOR_MATCHES_REGEX)
                                    .setValue("regex-1"))))
            .build();

    DataType expectedRule2 =
        DataType.newBuilder()
            .setDataTypeId("id-2")
            .setSessionIdentifier(false)
            .setTransformation(DataTransformation.DATA_TRANSFORMATION_OBFUSCATE)
            .addMatchRules(
                DataTypeMatchRule.newBuilder()
                    .setSpanFilter(
                        SpanFilter.newBuilder()
                            .addRequiredMatchingAttributes(
                                AttributePredicate.newBuilder()
                                    .setNamePredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(OPERATOR_EQUALS)
                                            .setValue("key-2"))
                                    .setValuePredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(OPERATOR_MATCHES_REGEX)
                                            .setValue("reg-2"))))
                    .setResult(Result.RESULT_MATCH)
                    .setAttributeFilter(AttributeFilter.newBuilder().addPrefixes("key-2"))
                    .setPathPredicate(
                        PathPredicate.newBuilder()
                            .setPathSegmentPredicate(
                                StringPredicate.newBuilder()
                                    .setOperator(OPERATOR_MATCHES_REGEX)
                                    .setValue("regex-2"))))
            .build();

    DataType expectedRule3 =
        DataType.newBuilder()
            .setDataTypeId("id-3")
            .setSessionIdentifier(true)
            .addMatchRules(
                DataTypeMatchRule.newBuilder()
                    .setSpanFilter(
                        SpanFilter.newBuilder()
                            .addRequiredMatchingAttributes(
                                AttributePredicate.newBuilder()
                                    .setNamePredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(OPERATOR_EQUALS)
                                            .setValue("key-3"))
                                    .setValuePredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(OPERATOR_MATCHES_REGEX)
                                            .setValue("reg-3"))))
                    .setResult(Result.RESULT_MATCH)
                    .setAttributeFilter(AttributeFilter.getDefaultInstance())
                    .setPathPredicate(
                        PathPredicate.newBuilder()
                            .setPathSegmentPredicate(
                                StringPredicate.newBuilder()
                                    .setOperator(OPERATOR_MATCHES_REGEX)
                                    .setValue("regex-3"))))
            .build();

    assertEquals(List.of(expectedRule1, expectedRule2, expectedRule3), translatedDataTypes);
  }

  @Test
  void translateRedactionRulesTestOnlyRedactionAllowed() {
    RedactionRule rule1 =
        RedactionRule.newBuilder()
            .setId("id-1")
            .setSessionIdentifier(false)
            .setRedactionStrategy(REDACTION_STRATEGY_REDACT)
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
            .setRedactionStrategy(REDACTION_STRATEGY_HASH)
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
        this.redactionRulesTranslator.translateRedactionRules(
            List.of(rule1, rule2, rule3), Set.of(REDACTION_STRATEGY_REDACT));
    assertEquals(2, translatedDataTypes.size());

    DataType expectedRule1 =
        DataType.newBuilder()
            .setDataTypeId("id-1")
            .setSessionIdentifier(false)
            .setTransformation(DataTransformation.DATA_TRANSFORMATION_REDACT)
            .addMatchRules(
                DataTypeMatchRule.newBuilder()
                    .setSpanFilter(
                        SpanFilter.newBuilder()
                            .addRequiredMatchingAttributes(
                                AttributePredicate.newBuilder()
                                    .setNamePredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(OPERATOR_EQUALS)
                                            .setValue("key-1"))
                                    .setValuePredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(OPERATOR_MATCHES_REGEX)
                                            .setValue("reg-1"))))
                    .setResult(Result.RESULT_MATCH)
                    .setAttributeFilter(AttributeFilter.getDefaultInstance())
                    .setPathPredicate(
                        PathPredicate.newBuilder()
                            .setPathSegmentPredicate(
                                StringPredicate.newBuilder()
                                    .setOperator(OPERATOR_MATCHES_REGEX)
                                    .setValue("regex-1"))))
            .build();

    DataType expectedRule2 =
        DataType.newBuilder()
            .setDataTypeId("id-3")
            .setSessionIdentifier(true)
            .addMatchRules(
                DataTypeMatchRule.newBuilder()
                    .setSpanFilter(
                        SpanFilter.newBuilder()
                            .addRequiredMatchingAttributes(
                                AttributePredicate.newBuilder()
                                    .setNamePredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(OPERATOR_EQUALS)
                                            .setValue("key-3"))
                                    .setValuePredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(OPERATOR_MATCHES_REGEX)
                                            .setValue("reg-3"))))
                    .setResult(Result.RESULT_MATCH)
                    .setAttributeFilter(AttributeFilter.getDefaultInstance())
                    .setPathPredicate(
                        PathPredicate.newBuilder()
                            .setPathSegmentPredicate(
                                StringPredicate.newBuilder()
                                    .setOperator(OPERATOR_MATCHES_REGEX)
                                    .setValue("regex-3"))))
            .build();

    assertEquals(List.of(expectedRule1, expectedRule2), translatedDataTypes);
  }

  @Test
  void translateDataTypesForSensitiveHeadersTest() {
    Parameter parameter1 =
        Parameter.newBuilder().setName("name-1").setParamType(ParamType.PARAM_TYPE_HEADER).build();
    Parameter parameter2 =
        Parameter.newBuilder().setName("name-2").setParamType(ParamType.PARAM_TYPE_HEADER).build();
    Parameter parameter3 =
        Parameter.newBuilder().setName("name-3").setParamType(ParamType.PARAM_TYPE_HEADER).build();
    DataType expectedDataType =
        DataType.newBuilder()
            .setDataTypeId(LEGACY_SENSITIVE_HEADERS_DATA_TYPE_ID)
            .setTransformation(DataTransformation.DATA_TRANSFORMATION_REDACT)
            .addMatchRules(buildMatchRule("name-1"))
            .addMatchRules(buildMatchRule("name-2"))
            .addMatchRules(buildMatchRule("name-3"))
            .build();

    Optional<DataType> actualDataType =
        redactionRulesTranslator.translateDataTypeForSensitiveHeaders(
            List.of(parameter1, parameter2, parameter3), REDACTION_STRATEGY_REDACT);
    assertEquals(Optional.of(expectedDataType), actualDataType);

    actualDataType =
        redactionRulesTranslator.translateDataTypeForSensitiveHeaders(
            List.of(parameter3, parameter2, parameter1), RedactionStrategy.REDACTION_STRATEGY_RAW);
    assertEquals(Optional.empty(), actualDataType);
  }

  private DataTypeMatchRule buildMatchRule(String name) {
    return DataTypeMatchRule.newBuilder()
        .setResult(Result.RESULT_MATCH)
        .setPathPredicate(
            PathPredicate.newBuilder()
                .setPathSegmentPredicate(
                    StringPredicate.newBuilder().setValue(name).setOperator(OPERATOR_EQUALS)))
        .setAttributeFilter(
            AttributeFilter.newBuilder()
                .addAllPrefixes(
                    List.of(
                        "rpc.response.metadata",
                        "http.response.header",
                        "rpc.request.metadata",
                        "http.request.header")))
        .build();
  }
}
