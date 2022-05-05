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
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTransformation;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class DataClassificationRulesTranslatorTest {
  private static final String SEPARATOR = ".";
  private static final String HTTP_REQUEST_HEADER = "http.request.header";
  private static final String RPC_REQUEST_METADATA = "rpc.request.metadata";
  private static final List<String> REQUEST_HEADERS_PREFIXES_LIST =
      List.of(HTTP_REQUEST_HEADER + SEPARATOR, RPC_REQUEST_METADATA + SEPARATOR);
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
                                List.of(Location.LOCATION_PATH, Location.LOCATION_QUERY))
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

    List<ai.traceable.external.data.classification.config.service.v1.DataType> translatedDataTypes =
        dataClassificationRulesTranslator.translateDataTypes(
            dataTypesToIdMap, dataTypesToDataSuppressionMap);
    assertEquals("id-3", translatedDataTypes.get(0).getDataTypeId());
    assertEquals(
        DataTransformation.DATA_TRANSFORMATION_OBFUSCATE,
        translatedDataTypes.get(0).getTransformation());
    assertEquals("id-2", translatedDataTypes.get(1).getDataTypeId());
    assertEquals(
        DataTransformation.DATA_TRANSFORMATION_REDACT,
        translatedDataTypes.get(1).getTransformation());
    assertEquals("id-4", translatedDataTypes.get(2).getDataTypeId());
    assertEquals(
        DataTransformation.DATA_TRANSFORMATION_UNSPECIFIED,
        translatedDataTypes.get(2).getTransformation());
    assertEquals("id-1", translatedDataTypes.get(3).getDataTypeId());
    assertEquals(
        DataTransformation.DATA_TRANSFORMATION_REDACT,
        translatedDataTypes.get(3).getTransformation());

    assertEquals(
        "(en\\|v\\-1|env\\-2)",
        translatedDataTypes
            .get(3)
            .getMatchRules(0)
            .getSpanFilter()
            .getRequiredMatchingAttributes(0)
            .getValuePredicate()
            .getValue());

    assertEquals(
        REQUEST_HEADERS_PREFIXES_LIST,
        translatedDataTypes.get(0).getMatchRules(0).getAttributeFilter().getPrefixesList());

    assertEquals(
        "value-2",
        translatedDataTypes
            .get(1)
            .getMatchRules(0)
            .getPathPredicate()
            .getPathSegmentPredicate()
            .getValue());
    assertEquals(
        "key-3",
        translatedDataTypes
            .get(0)
            .getMatchRules(0)
            .getPathValuePredicate()
            .getPathPredicate()
            .getPathSegmentPredicate()
            .getValue());
    assertEquals(
        "value-3",
        translatedDataTypes
            .get(0)
            .getMatchRules(0)
            .getPathValuePredicate()
            .getValuePredicate()
            .getValue());
  }
}
