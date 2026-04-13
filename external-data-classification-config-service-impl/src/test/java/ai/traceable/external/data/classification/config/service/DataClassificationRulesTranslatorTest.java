package ai.traceable.external.data.classification.config.service;

import static ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.PredicateSupportLevel.PREDICATE_SUPPORT_LEVEL_LEAF_PATH_SEGMENT;
import static ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.PredicateSupportLevel.PREDICATE_SUPPORT_LEVEL_UNSPECIFIED;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Action;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.EnvironmentScope;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.KeyValuePattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Location;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Operator;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.StringPattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.UrlMatchScope;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ValuePattern;
import ai.traceable.external.data.classification.config.service.v1.AttributeFilter;
import ai.traceable.external.data.classification.config.service.v1.AttributePredicate;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTransformation;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTypeMatchRule;
import ai.traceable.external.data.classification.config.service.v1.DataType.Result;
import ai.traceable.external.data.classification.config.service.v1.FullValueRegexDataType;
import ai.traceable.external.data.classification.config.service.v1.PathPredicate;
import ai.traceable.external.data.classification.config.service.v1.PathValuePredicate;
import ai.traceable.external.data.classification.config.service.v1.SpanFilter;
import ai.traceable.external.data.classification.config.service.v1.StringPredicate;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class DataClassificationRulesTranslatorTest {
  private final DataClassificationRulesTranslator dataClassificationRulesTranslator =
      new DataClassificationRulesTranslator();

  @Test
  void translateDataTypesTest() {
    DataType dataType1 =
        DataType.newBuilder()
            .setId("id-1")
            .setRule(
                DataTypeRule.newBuilder()
                    .setName("datatype-1")
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_REDACT)
                    .addScopedPatterns(
                        ScopedPattern.newBuilder()
                            .setEnvironmentScope(
                                EnvironmentScope.newBuilder()
                                    .addAllEnvironmentIds(List.of("env-1", "env-2")))
                            .addAllLocations(
                                List.of(Location.LOCATION_REQUEST_HEADER, Location.LOCATION_QUERY))
                            .setKeyPattern(
                                StringPattern.newBuilder()
                                    .setValue("value-1")
                                    .setOperator(Operator.OPERATOR_EQUALS))
                            .setAction(Action.ACTION_MATCH)))
            .build();

    DataType dataType2 =
        DataType.newBuilder()
            .setId("id-2")
            .setRule(
                DataTypeRule.newBuilder()
                    .setName("datatype-2")
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_REDACT)
                    .addScopedPatterns(
                        ScopedPattern.newBuilder()
                            .setGlobalScope(DataTypeRule.GlobalScope.getDefaultInstance())
                            .addLocations(Location.LOCATION_ANY)
                            .setKeyPattern(
                                StringPattern.newBuilder()
                                    .setValue("value-2")
                                    .setOperator(Operator.OPERATOR_EQUALS))
                            .setAction(Action.ACTION_MATCH)))
            .build();

    DataType dataType3 =
        DataType.newBuilder()
            .setId("id-3")
            .setRule(
                DataTypeRule.newBuilder()
                    .setName("datatype-3")
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_OBFUSCATE)
                    .addScopedPatterns(
                        ScopedPattern.newBuilder()
                            .setEnvironmentScope(
                                EnvironmentScope.newBuilder().addEnvironmentIds("env-1"))
                            .addLocations(Location.LOCATION_REQUEST_HEADER)
                            .setKeyValuePattern(
                                KeyValuePattern.newBuilder()
                                    .setKeyPattern(
                                        StringPattern.newBuilder()
                                            .setValue("key-3")
                                            .setOperator(Operator.OPERATOR_EQUALS))
                                    .setValuePattern(
                                        StringPattern.newBuilder()
                                            .setValue("value-3")
                                            .setOperator(Operator.OPERATOR_EQUALS)))
                            .setAction(Action.ACTION_MATCH)))
            .build();

    DataType dataType4 =
        DataType.newBuilder()
            .setId("id-4")
            .setRule(
                DataTypeRule.newBuilder()
                    .setName("datatype-4")
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_RAW)
                    .addScopedPatterns(
                        ScopedPattern.newBuilder()
                            .setEnvironmentScope(
                                EnvironmentScope.newBuilder().addEnvironmentIds("env-1"))
                            .addLocations(Location.LOCATION_REQUEST_HEADER)
                            .addLocations(Location.LOCATION_ANY)
                            .setKeyPattern(
                                StringPattern.newBuilder()
                                    .setValue("value-4")
                                    .setOperator(Operator.OPERATOR_MATCHES_REGEX))
                            .setAction(Action.ACTION_MATCH)))
            .build();

    DataType dataType5 =
        DataType.newBuilder()
            .setId("id-5")
            .setRule(
                DataTypeRule.newBuilder()
                    .setName("datatype-5")
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_OBFUSCATE)
                    .addScopedPatterns(
                        ScopedPattern.newBuilder()
                            .setGlobalScope(DataTypeRule.GlobalScope.getDefaultInstance())
                            .addLocations(Location.LOCATION_REQUEST_HEADER)
                            .setLeafKeyValuePattern(
                                KeyValuePattern.newBuilder()
                                    .setKeyPattern(
                                        StringPattern.newBuilder()
                                            .setValue("key-5")
                                            .setOperator(Operator.OPERATOR_EQUALS))
                                    .setValuePattern(
                                        StringPattern.newBuilder()
                                            .setValue("value-5")
                                            .setOperator(Operator.OPERATOR_EQUALS)))
                            .setAction(Action.ACTION_MATCH)))
            .build();

    ai.traceable.external.data.classification.config.service.v1.DataType expectedDataType1 =
        ai.traceable.external.data.classification.config.service.v1.DataType.newBuilder()
            .setDataTypeId("id-1")
            .setTransformation(DataTransformation.DATA_TRANSFORMATION_REDACT)
            .addMatchRules(
                DataTypeMatchRule.newBuilder()
                    .setAttributeFilter(
                        AttributeFilter.newBuilder()
                            .addAllPrefixes(
                                List.of(
                                    "http.request.header",
                                    "rpc.request.metadata",
                                    "http.url",
                                    "http.target",
                                    "http.path",
                                    "url.full",
                                    "url.query")))
                    .setResult(Result.RESULT_MATCH)
                    .setPathPredicate(
                        PathPredicate.newBuilder()
                            .setPathSegmentPredicate(
                                StringPredicate.newBuilder()
                                    .setValue("value-1")
                                    .setOperator(
                                        ai.traceable.external.data.classification.config.service.v1
                                            .Operator.OPERATOR_EQUALS))))
            .build();

    ai.traceable.external.data.classification.config.service.v1.DataType expectedDataType2 =
        ai.traceable.external.data.classification.config.service.v1.DataType.newBuilder()
            .setDataTypeId("id-2")
            .setTransformation(DataTransformation.DATA_TRANSFORMATION_REDACT)
            .addMatchRules(
                DataTypeMatchRule.newBuilder()
                    .setAttributeFilter(
                        AttributeFilter.newBuilder()
                            .addAllPrefixes(
                                List.of(
                                    "http.request.header",
                                    "rpc.request.metadata",
                                    "http.response.header",
                                    "rpc.response.metadata",
                                    "http.request.header.cookie",
                                    "http.response.header.set-cookie",
                                    "http.url",
                                    "http.target",
                                    "http.path",
                                    "url.full",
                                    "url.query",
                                    "http.request.body",
                                    "rpc.request.body",
                                    "http.response.body",
                                    "rpc.response.body")))
                    .setResult(Result.RESULT_MATCH)
                    .setPathPredicate(
                        PathPredicate.newBuilder()
                            .setPathSegmentPredicate(
                                StringPredicate.newBuilder()
                                    .setValue("value-2")
                                    .setOperator(
                                        ai.traceable.external.data.classification.config.service.v1
                                            .Operator.OPERATOR_EQUALS))))
            .build();

    ai.traceable.external.data.classification.config.service.v1.DataType expectedDataType3 =
        ai.traceable.external.data.classification.config.service.v1.DataType.newBuilder()
            .setDataTypeId("id-3")
            .setTransformation(DataTransformation.DATA_TRANSFORMATION_OBFUSCATE)
            .addMatchRules(
                DataTypeMatchRule.newBuilder()
                    .setAttributeFilter(
                        AttributeFilter.newBuilder()
                            .addAllPrefixes(List.of("http.request.header", "rpc.request.metadata")))
                    .setResult(Result.RESULT_MATCH)
                    .setPathValuePredicate(
                        PathValuePredicate.newBuilder()
                            .setPathPredicate(
                                PathPredicate.newBuilder()
                                    .setPathSegmentPredicate(
                                        StringPredicate.newBuilder()
                                            .setValue("key-3")
                                            .setOperator(
                                                ai.traceable.external.data.classification.config
                                                    .service.v1.Operator.OPERATOR_EQUALS)))
                            .setValuePredicate(
                                StringPredicate.newBuilder()
                                    .setValue("value-3")
                                    .setOperator(
                                        ai.traceable.external.data.classification.config.service.v1
                                            .Operator.OPERATOR_EQUALS))))
            .build();

    ai.traceable.external.data.classification.config.service.v1.DataType expectedDataType4 =
        ai.traceable.external.data.classification.config.service.v1.DataType.newBuilder()
            .setDataTypeId("id-4")
            .addMatchRules(
                DataTypeMatchRule.newBuilder()
                    .setAttributeFilter(
                        AttributeFilter.newBuilder()
                            .addAllPrefixes(
                                List.of(
                                    "http.request.header",
                                    "rpc.request.metadata",
                                    "http.response.header",
                                    "rpc.response.metadata",
                                    "http.request.header.cookie",
                                    "http.response.header.set-cookie",
                                    "http.url",
                                    "http.target",
                                    "http.path",
                                    "url.full",
                                    "url.query",
                                    "http.request.body",
                                    "rpc.request.body",
                                    "http.response.body",
                                    "rpc.response.body")))
                    .setResult(Result.RESULT_MATCH)
                    .setPathPredicate(
                        PathPredicate.newBuilder()
                            .setPathSegmentPredicate(
                                StringPredicate.newBuilder()
                                    .setValue("value-4")
                                    .setOperator(
                                        ai.traceable.external.data.classification.config.service.v1
                                            .Operator.OPERATOR_MATCHES_REGEX))))
            .build();

    ai.traceable.external.data.classification.config.service.v1.DataType expectedDataType5 =
        ai.traceable.external.data.classification.config.service.v1.DataType.newBuilder()
            .setDataTypeId("id-5")
            .setTransformation(DataTransformation.DATA_TRANSFORMATION_OBFUSCATE)
            .addMatchRules(
                DataTypeMatchRule.newBuilder()
                    .setAttributeFilter(
                        AttributeFilter.newBuilder()
                            .addAllPrefixes(List.of("http.request.header", "rpc.request.metadata")))
                    .setResult(Result.RESULT_MATCH)
                    .setPathValuePredicate(
                        PathValuePredicate.newBuilder()
                            .setPathPredicate(
                                PathPredicate.newBuilder()
                                    .setLeafPathSegmentPredicate(
                                        StringPredicate.newBuilder()
                                            .setValue("key-5")
                                            .setOperator(
                                                ai.traceable.external.data.classification.config
                                                    .service.v1.Operator.OPERATOR_EQUALS)))
                            .setValuePredicate(
                                StringPredicate.newBuilder()
                                    .setValue("value-5")
                                    .setOperator(
                                        ai.traceable.external.data.classification.config.service.v1
                                            .Operator.OPERATOR_EQUALS))))
            .build();

    ai.traceable.external.data.classification.config.service.v1.DataType expectedDataType5_1 =
        ai.traceable.external.data.classification.config.service.v1.DataType.newBuilder()
            .setDataTypeId("id-5")
            .setTransformation(DataTransformation.DATA_TRANSFORMATION_OBFUSCATE)
            .addMatchRules(
                DataTypeMatchRule.newBuilder()
                    .setAttributeFilter(
                        AttributeFilter.newBuilder()
                            .addAllPrefixes(List.of("http.request.header", "rpc.request.metadata")))
                    .setResult(Result.RESULT_MATCH)
                    .setPathValuePredicate(
                        PathValuePredicate.newBuilder()
                            .setPathPredicate(
                                PathPredicate.newBuilder()
                                    .setPathSegmentPredicate(
                                        StringPredicate.newBuilder()
                                            .setValue("key-5")
                                            .setOperator(
                                                ai.traceable.external.data.classification.config
                                                    .service.v1.Operator.OPERATOR_EQUALS)))
                            .setValuePredicate(
                                StringPredicate.newBuilder()
                                    .setValue("value-5")
                                    .setOperator(
                                        ai.traceable.external.data.classification.config.service.v1
                                            .Operator.OPERATOR_EQUALS))))
            .build();

    List<ai.traceable.external.data.classification.config.service.v1.DataType> translatedDataTypes =
        dataClassificationRulesTranslator.translateDataTypes(
            List.of(dataType1, dataType2, dataType3, dataType4, dataType5),
            Optional.of("env-1"),
            PREDICATE_SUPPORT_LEVEL_LEAF_PATH_SEGMENT);

    List<ai.traceable.external.data.classification.config.service.v1.DataType> expectedDataTypes =
        List.of(
            expectedDataType1,
            expectedDataType2,
            expectedDataType3,
            expectedDataType4,
            expectedDataType5);

    assertEquals(expectedDataTypes, translatedDataTypes);

    translatedDataTypes =
        dataClassificationRulesTranslator.translateDataTypes(
            List.of(dataType1, dataType2, dataType3, dataType4, dataType5),
            Optional.of("env-1"),
            PREDICATE_SUPPORT_LEVEL_UNSPECIFIED);

    expectedDataTypes =
        List.of(
            expectedDataType1,
            expectedDataType2,
            expectedDataType3,
            expectedDataType4,
            expectedDataType5_1);

    assertEquals(expectedDataTypes, translatedDataTypes);

    translatedDataTypes =
        dataClassificationRulesTranslator.translateDataTypes(
            List.of(dataType1, dataType2, dataType3, dataType4, dataType5),
            Optional.of("env-2"),
            PREDICATE_SUPPORT_LEVEL_UNSPECIFIED);

    expectedDataTypes = List.of(expectedDataType1, expectedDataType2, expectedDataType5_1);

    assertEquals(expectedDataTypes, translatedDataTypes);

    translatedDataTypes =
        dataClassificationRulesTranslator.translateDataTypes(
            List.of(dataType1, dataType2, dataType3, dataType4, dataType5),
            Optional.empty(),
            PREDICATE_SUPPORT_LEVEL_LEAF_PATH_SEGMENT);
    expectedDataTypes = List.of(expectedDataType2, expectedDataType5);
    assertEquals(expectedDataTypes, translatedDataTypes);

    translatedDataTypes =
        dataClassificationRulesTranslator.translateDataTypes(
            List.of(dataType1, dataType2, dataType3, dataType4, dataType5),
            Optional.of("env-random"),
            PREDICATE_SUPPORT_LEVEL_LEAF_PATH_SEGMENT);
    expectedDataTypes = List.of(expectedDataType2, expectedDataType5);
    assertEquals(expectedDataTypes, translatedDataTypes);
  }

  @Test
  void testTranslateUrlScope() {
    DataType inputType =
        DataType.newBuilder()
            .setId("id-1")
            .setRule(
                DataTypeRule.newBuilder()
                    .setName("datatype-1")
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_REDACT)
                    .addScopedPatterns(
                        ScopedPattern.newBuilder()
                            .setUrlMatchScope(
                                UrlMatchScope.newBuilder()
                                    .addUrlRegexMatches("/example-url/.*")
                                    .addUrlRegexMatches("/other"))
                            .addAllLocations(
                                List.of(Location.LOCATION_REQUEST_HEADER, Location.LOCATION_QUERY))
                            .setKeyPattern(
                                StringPattern.newBuilder()
                                    .setValue("value-1")
                                    .setOperator(Operator.OPERATOR_EQUALS))
                            .setAction(Action.ACTION_MATCH)))
            .build();

    ai.traceable.external.data.classification.config.service.v1.DataType expectedOutput =
        ai.traceable.external.data.classification.config.service.v1.DataType.newBuilder()
            .setDataTypeId("id-1")
            .setTransformation(DataTransformation.DATA_TRANSFORMATION_REDACT)
            .addMatchRules(
                DataTypeMatchRule.newBuilder()
                    .setAttributeFilter(
                        AttributeFilter.newBuilder()
                            .addAllPrefixes(
                                List.of(
                                    "http.request.header",
                                    "rpc.request.metadata",
                                    "http.url",
                                    "http.target",
                                    "http.path",
                                    "url.full",
                                    "url.query")))
                    .setSpanFilter(
                        SpanFilter.newBuilder()
                            .addRequiredMatchingAttributes(
                                AttributePredicate.newBuilder()
                                    .setNamePredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(
                                                ai.traceable.external.data.classification.config
                                                    .service.v1.Operator.OPERATOR_MATCHES_REGEX)
                                            .setValue(
                                                "http\\.url|http\\.target|http\\.path|url\\.full"))
                                    .setValuePredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(
                                                ai.traceable.external.data.classification.config
                                                    .service.v1.Operator.OPERATOR_MATCHES_REGEX)
                                            .setValue("/example-url/.*|/other"))))
                    .setResult(Result.RESULT_MATCH)
                    .setPathPredicate(
                        PathPredicate.newBuilder()
                            .setPathSegmentPredicate(
                                StringPredicate.newBuilder()
                                    .setValue("value-1")
                                    .setOperator(
                                        ai.traceable.external.data.classification.config.service.v1
                                            .Operator.OPERATOR_EQUALS))))
            .build();

    assertEquals(
        List.of(expectedOutput),
        dataClassificationRulesTranslator.translateDataTypes(
            List.of(inputType), Optional.empty(), PREDICATE_SUPPORT_LEVEL_UNSPECIFIED));
  }

  @Test
  void testTranslateValuePatternToFullValueRegexDataType() {
    // Create a DataType with VALUE_PATTERN
    DataType inputType =
        DataType.newBuilder()
            .setId("id-1")
            .setRule(
                DataTypeRule.newBuilder()
                    .setName("datatype-1")
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_REDACT)
                    .addScopedPatterns(
                        ScopedPattern.newBuilder()
                            .setGlobalScope(DataTypeRule.GlobalScope.getDefaultInstance())
                            .addLocations(Location.LOCATION_PATH)
                            .setValuePattern(
                                ValuePattern.newBuilder()
                                    .setValuePattern(
                                        StringPattern.newBuilder()
                                            .setValue("\\d{3}-\\d{2}-\\d{4}")
                                            .setOperator(Operator.OPERATOR_MATCHES_REGEX)))
                            .setAction(Action.ACTION_MATCH)))
            .build();

    FullValueRegexDataType expectedOutput =
        FullValueRegexDataType.newBuilder()
            .setDataTypeId("id-1")
            .setTransformation(FullValueRegexDataType.DataTransformation.DATA_TRANSFORMATION_REDACT)
            .setAttributeFilter(
                AttributeFilter.newBuilder()
                    .addAllPrefixes(
                        List.of("http.url", "http.target", "http.path", "url.full", "url.query")))
            .setSuppressionPattern(
                StringPredicate.newBuilder()
                    .setOperator(
                        ai.traceable.external.data.classification.config.service.v1.Operator
                            .OPERATOR_MATCHES_REGEX)
                    .setValue("\\d{3}-\\d{2}-\\d{4}"))
            .setResult(FullValueRegexDataType.Result.RESULT_MATCH)
            .build();

    List<FullValueRegexDataType> result =
        dataClassificationRulesTranslator.translateFullValueRegexDataTypes(
            List.of(inputType), Optional.empty());

    assertEquals(1, result.size());
    assertEquals(expectedOutput, result.get(0));

    // Verify that VALUE_PATTERN does not create DataType rules
    List<ai.traceable.external.data.classification.config.service.v1.DataType> dataTypes =
        dataClassificationRulesTranslator.translateDataTypes(
            List.of(inputType), Optional.empty(), PREDICATE_SUPPORT_LEVEL_UNSPECIFIED);
    assertEquals(0, dataTypes.size());
  }

  @Test
  void testTranslateMultipleValuePatternsToFullValueRegexDataType() {
    // Create a DataType with multiple VALUE_PATTERNs
    DataType inputType =
        DataType.newBuilder()
            .setId("id-1")
            .setRule(
                DataTypeRule.newBuilder()
                    .setName("datatype-1")
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_OBFUSCATE)
                    .addScopedPatterns(
                        ScopedPattern.newBuilder()
                            .setGlobalScope(DataTypeRule.GlobalScope.getDefaultInstance())
                            .addLocations(Location.LOCATION_PATH)
                            .setValuePattern(
                                ValuePattern.newBuilder()
                                    .setValuePattern(
                                        StringPattern.newBuilder()
                                            .setValue("pattern1")
                                            .setOperator(Operator.OPERATOR_EQUALS)))
                            .setAction(Action.ACTION_MATCH))
                    .addScopedPatterns(
                        ScopedPattern.newBuilder()
                            .setGlobalScope(DataTypeRule.GlobalScope.getDefaultInstance())
                            .addLocations(Location.LOCATION_PATH)
                            .setValuePattern(
                                ValuePattern.newBuilder()
                                    .setValuePattern(
                                        StringPattern.newBuilder()
                                            .setValue("pattern2")
                                            .setOperator(Operator.OPERATOR_MATCHES_REGEX)))
                            .setAction(Action.ACTION_IGNORE)))
            .build();

    FullValueRegexDataType expectedOutput1 =
        FullValueRegexDataType.newBuilder()
            .setDataTypeId("id-1")
            .setTransformation(
                FullValueRegexDataType.DataTransformation.DATA_TRANSFORMATION_OBFUSCATE)
            .setAttributeFilter(
                AttributeFilter.newBuilder()
                    .addAllPrefixes(
                        List.of("http.url", "http.target", "http.path", "url.full", "url.query")))
            .setSuppressionPattern(
                StringPredicate.newBuilder()
                    .setOperator(
                        ai.traceable.external.data.classification.config.service.v1.Operator
                            .OPERATOR_MATCHES_REGEX)
                    .setValue("pattern1"))
            .setResult(FullValueRegexDataType.Result.RESULT_MATCH)
            .build();

    FullValueRegexDataType expectedOutput2 =
        FullValueRegexDataType.newBuilder()
            .setDataTypeId("id-1")
            .setTransformation(
                FullValueRegexDataType.DataTransformation.DATA_TRANSFORMATION_OBFUSCATE)
            .setAttributeFilter(
                AttributeFilter.newBuilder()
                    .addAllPrefixes(
                        List.of("http.url", "http.target", "http.path", "url.full", "url.query")))
            .setSuppressionPattern(
                StringPredicate.newBuilder()
                    .setOperator(
                        ai.traceable.external.data.classification.config.service.v1.Operator
                            .OPERATOR_MATCHES_REGEX)
                    .setValue("pattern2"))
            .setResult(FullValueRegexDataType.Result.RESULT_IGNORE)
            .build();

    List<FullValueRegexDataType> result =
        dataClassificationRulesTranslator.translateFullValueRegexDataTypes(
            List.of(inputType), Optional.empty());

    assertEquals(2, result.size());
    assertEquals(expectedOutput1, result.get(0));
    assertEquals(expectedOutput2, result.get(1));
  }

  @Test
  void testTranslateValuePatternWithEnvironmentScopeToFullValueRegexDataType() {
    // Create a DataType with VALUE_PATTERN and environment scope
    DataType inputType =
        DataType.newBuilder()
            .setId("id-1")
            .setRule(
                DataTypeRule.newBuilder()
                    .setName("datatype-1")
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_REDACT)
                    .addScopedPatterns(
                        ScopedPattern.newBuilder()
                            .setEnvironmentScope(
                                EnvironmentScope.newBuilder().addEnvironmentIds("env-1"))
                            .addLocations(Location.LOCATION_PATH)
                            .setValuePattern(
                                ValuePattern.newBuilder()
                                    .setValuePattern(
                                        StringPattern.newBuilder()
                                            .setValue("pattern-env1")
                                            .setOperator(Operator.OPERATOR_EQUALS)))
                            .setAction(Action.ACTION_MATCH)))
            .build();

    // Test with matching environment
    List<FullValueRegexDataType> resultWithEnv =
        dataClassificationRulesTranslator.translateFullValueRegexDataTypes(
            List.of(inputType), Optional.of("env-1"));
    assertEquals(1, resultWithEnv.size());

    // Test with non-matching environment
    List<FullValueRegexDataType> resultWithoutEnv =
        dataClassificationRulesTranslator.translateFullValueRegexDataTypes(
            List.of(inputType), Optional.of("env-2"));
    assertEquals(0, resultWithoutEnv.size());

    // Test with empty environment (should not match environment-scoped patterns)
    List<FullValueRegexDataType> resultEmptyEnv =
        dataClassificationRulesTranslator.translateFullValueRegexDataTypes(
            List.of(inputType), Optional.empty());
    assertEquals(0, resultEmptyEnv.size());
  }
}
