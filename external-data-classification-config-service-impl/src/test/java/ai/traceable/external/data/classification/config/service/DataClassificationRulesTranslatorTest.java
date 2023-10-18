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
import ai.traceable.external.data.classification.config.service.v1.AttributeFilter;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTransformation;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTypeMatchRule;
import ai.traceable.external.data.classification.config.service.v1.DataType.Result;
import ai.traceable.external.data.classification.config.service.v1.PathPredicate;
import ai.traceable.external.data.classification.config.service.v1.PathValuePredicate;
import ai.traceable.external.data.classification.config.service.v1.StringPredicate;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class DataClassificationRulesTranslatorTest {
  private final DataClassificationRulesTranslator dataClassificationRulesTranslator =
      new DataClassificationRulesTranslator();

  @Test
  void translateDataTypesTest() {
    ResolvedPlatformDataType dataType1 =
        new ResolvedPlatformDataType(
            DataType.newBuilder()
                .setId("id-1")
                .setRule(
                    DataTypeRule.newBuilder()
                        .setName("datatype-1")
                        .addScopedPatterns(
                            ScopedPattern.newBuilder()
                                .setEnvironmentScope(
                                    EnvironmentScope.newBuilder()
                                        .addAllEnvironmentIds(List.of("env-1", "env-2")))
                                .addAllLocations(
                                    List.of(
                                        Location.LOCATION_REQUEST_HEADER, Location.LOCATION_QUERY))
                                .setKeyPattern(
                                    StringPattern.newBuilder()
                                        .setValue("value-1")
                                        .setOperator(Operator.OPERATOR_EQUALS))
                                .setAction(Action.ACTION_MATCH)))
                .build(),
            DataSuppression.DATA_SUPPRESSION_REDACT);

    ResolvedPlatformDataType dataType2 =
        new ResolvedPlatformDataType(
            DataType.newBuilder()
                .setId("id-2")
                .setRule(
                    DataTypeRule.newBuilder()
                        .setName("datatype-2")
                        .addScopedPatterns(
                            ScopedPattern.newBuilder()
                                .setGlobalScope(DataTypeRule.GlobalScope.getDefaultInstance())
                                .addLocations(Location.LOCATION_ANY)
                                .setKeyPattern(
                                    StringPattern.newBuilder()
                                        .setValue("value-2")
                                        .setOperator(Operator.OPERATOR_EQUALS))
                                .setAction(Action.ACTION_MATCH)))
                .build(),
            DataSuppression.DATA_SUPPRESSION_REDACT);

    ResolvedPlatformDataType dataType3 =
        new ResolvedPlatformDataType(
            DataType.newBuilder()
                .setId("id-3")
                .setRule(
                    DataTypeRule.newBuilder()
                        .setName("datatype-3")
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
                .build(),
            DataSuppression.DATA_SUPPRESSION_OBFUSCATE);

    ResolvedPlatformDataType dataType4 =
        new ResolvedPlatformDataType(
            DataType.newBuilder()
                .setId("id-4")
                .setRule(
                    DataTypeRule.newBuilder()
                        .setName("datatype-4")
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
                .build(),
            DataSuppression.DATA_SUPPRESSION_RAW);

    ResolvedPlatformDataType dataType5 =
        new ResolvedPlatformDataType(
            DataType.newBuilder()
                .setId("id-5")
                .setRule(
                    DataTypeRule.newBuilder()
                        .setName("datatype-5")
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
                .build(),
            DataSuppression.DATA_SUPPRESSION_OBFUSCATE);

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
}
