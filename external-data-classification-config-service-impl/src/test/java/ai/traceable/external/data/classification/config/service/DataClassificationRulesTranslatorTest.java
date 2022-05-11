package ai.traceable.external.data.classification.config.service;

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
import ai.traceable.external.data.classification.config.service.v1.AttributePredicate;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTransformation;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTypeMatchRule;
import ai.traceable.external.data.classification.config.service.v1.DataType.Result;
import ai.traceable.external.data.classification.config.service.v1.PathPredicate;
import ai.traceable.external.data.classification.config.service.v1.PathValuePredicate;
import ai.traceable.external.data.classification.config.service.v1.SpanFilter;
import ai.traceable.external.data.classification.config.service.v1.StringPredicate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class DataClassificationRulesTranslatorTest {
  private static final StringPredicate KEY_PREDICATE_FOR_ENVIRONMENT_SCOPE =
      StringPredicate.newBuilder()
          .setOperator(
              ai.traceable.external.data.classification.config.service.v1.Operator.OPERATOR_EQUALS)
          .setValue("deployment.environment")
          .build();
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
                    .addScopedPatterns(
                        ScopedPattern.newBuilder()
                            .setEnvironmentScope(
                                EnvironmentScope.newBuilder()
                                    .addAllEnvironmentIds(List.of("en|v-1", "env-2")))
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
                    .addScopedPatterns(
                        ScopedPattern.newBuilder()
                            .setEnvironmentScope(
                                EnvironmentScope.newBuilder().addEnvironmentIds("env-1"))
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

    Map<String, DataType> dataTypesToIdMap = new HashMap<>();
    dataTypesToIdMap.put("id-1", dataType1);
    dataTypesToIdMap.put("id-2", dataType2);
    dataTypesToIdMap.put("id-3", dataType3);
    dataTypesToIdMap.put("id-4", dataType4);

    Map<String, DataSuppression> dataTypesToDataSuppressionMap = new HashMap<>();
    dataTypesToDataSuppressionMap.put("id-1", DataSuppression.DATA_SUPPRESSION_REDACT);
    dataTypesToDataSuppressionMap.put("id-2", DataSuppression.DATA_SUPPRESSION_REDACT);
    dataTypesToDataSuppressionMap.put("id-3", DataSuppression.DATA_SUPPRESSION_OBFUSCATE);
    dataTypesToDataSuppressionMap.put("id-4", DataSuppression.DATA_SUPPRESSION_RAW);

    ai.traceable.external.data.classification.config.service.v1.DataType expectedDataType1 =
        ai.traceable.external.data.classification.config.service.v1.DataType.newBuilder()
            .setDataTypeId("id-1")
            .setTransformation(DataTransformation.DATA_TRANSFORMATION_REDACT)
            .addMatchRules(
                DataTypeMatchRule.newBuilder()
                    .setSpanFilter(
                        SpanFilter.newBuilder()
                            .addRequiredMatchingAttributes(
                                AttributePredicate.newBuilder()
                                    .setNamePredicate(KEY_PREDICATE_FOR_ENVIRONMENT_SCOPE)
                                    .setValuePredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(
                                                ai.traceable.external.data.classification.config
                                                    .service.v1.Operator.OPERATOR_MATCHES_REGEX)
                                            .setValue("(en\\|v\\-1|env\\-2)"))))
                    .setAttributeFilter(
                        AttributeFilter.newBuilder()
                            .addAllPrefixes(
                                List.of("http.request.header", "rpc.request.metadata", "http.url")))
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
                    .setSpanFilter(
                        SpanFilter.newBuilder()
                            .addRequiredMatchingAttributes(
                                AttributePredicate.newBuilder()
                                    .setNamePredicate(KEY_PREDICATE_FOR_ENVIRONMENT_SCOPE)
                                    .setValuePredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(
                                                ai.traceable.external.data.classification.config
                                                    .service.v1.Operator.OPERATOR_MATCHES_REGEX)
                                            .setValue("(env\\-1)"))))
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
                    .setSpanFilter(
                        SpanFilter.newBuilder()
                            .addRequiredMatchingAttributes(
                                AttributePredicate.newBuilder()
                                    .setNamePredicate(KEY_PREDICATE_FOR_ENVIRONMENT_SCOPE)
                                    .setValuePredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(
                                                ai.traceable.external.data.classification.config
                                                    .service.v1.Operator.OPERATOR_MATCHES_REGEX)
                                            .setValue("(env\\-1)"))))
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
                    .setSpanFilter(
                        SpanFilter.newBuilder()
                            .addRequiredMatchingAttributes(
                                AttributePredicate.newBuilder()
                                    .setNamePredicate(KEY_PREDICATE_FOR_ENVIRONMENT_SCOPE)
                                    .setValuePredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(
                                                ai.traceable.external.data.classification.config
                                                    .service.v1.Operator.OPERATOR_MATCHES_REGEX)
                                            .setValue("(env\\-1)"))))
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

    List<ai.traceable.external.data.classification.config.service.v1.DataType> translatedDataTypes =
        dataClassificationRulesTranslator.translateDataTypes(
            dataTypesToIdMap, dataTypesToDataSuppressionMap);

    assertEquals(
        Set.of(expectedDataType1, expectedDataType2, expectedDataType3, expectedDataType4),
        Set.copyOf(translatedDataTypes));
  }
}
