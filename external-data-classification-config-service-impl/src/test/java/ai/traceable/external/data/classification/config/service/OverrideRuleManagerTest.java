package ai.traceable.external.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.data.classification.config.service.v1.DataClassificationOverride;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideRule;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideRule.DataSuppressionOverride;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Operator;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.StringPattern;
import ai.traceable.data.classification.config.service.v1.SpanFilter;
import ai.traceable.data.classification.config.service.v1.SpanKeyValueFilter;
import ai.traceable.external.data.classification.config.service.v1.AttributePredicate;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTransformation;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTypeMatchRule;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest;
import ai.traceable.external.data.classification.config.service.v1.PathPredicate;
import ai.traceable.external.data.classification.config.service.v1.StringPredicate;
import java.util.Collections;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OverrideRuleManagerTest {

  @Mock DataClassificationRulesDao dataClassificationRulesDao;
  @Mock DataClassificationRulesTranslator dataClassificationRulesTranslator;

  @InjectMocks OverrideRuleManager overrideRuleManager;

  @Test
  void testGetOverrides() {
    RequestContext testContext = RequestContext.forTenantId("testGetOverrides");
    List<DataClassificationOverride> scopedOverrides =
        List.of(DataClassificationOverride.newBuilder().setId("scoped").build());
    when(dataClassificationRulesDao.getDataSuppressionOverrideRulesForEnvironment(
            testContext, "env"))
        .thenReturn(scopedOverrides);
    assertEquals(
        scopedOverrides,
        this.overrideRuleManager.getOverrides(
            testContext,
            GetDataClassificationConfigRequest.EnvironmentFilter.newBuilder()
                .setEnvironmentName("env")
                .build()));

    List<DataClassificationOverride> unscopedOverrides =
        List.of(DataClassificationOverride.newBuilder().setId("unscoped").build());
    when(dataClassificationRulesDao.getUnscopedDataSuppressionOverrideRules(testContext))
        .thenReturn(unscopedOverrides);
    assertEquals(
        unscopedOverrides,
        this.overrideRuleManager.getOverrides(
            testContext,
            GetDataClassificationConfigRequest.EnvironmentFilter.getDefaultInstance()));
  }

  @Test
  void testApplyOverridesWithoutOverrides() {

    DataType original =
        DataType.newBuilder()
            .setDataTypeId("dt-id")
            .setTransformation(DataTransformation.DATA_TRANSFORMATION_OBFUSCATE)
            .build();

    assertEquals(
        List.of(original), this.overrideRuleManager.applyOverrides(List.of(original), List.of()));
  }

  @Test
  void testApplyRawOverride() {
    DataType original =
        DataType.newBuilder()
            .setDataTypeId("dt-id")
            .setTransformation(DataTransformation.DATA_TRANSFORMATION_OBFUSCATE)
            .build();
    DataType expectedRawTransform = original.toBuilder().clearTransformation().build();
    DataClassificationOverride overrideToRaw =
        DataClassificationOverride.newBuilder()
            .setDataClassificationOverrideRule(
                DataClassificationOverrideRule.newBuilder()
                    .setDataSuppressionOverride(
                        DataSuppressionOverride.newBuilder()
                            .setDataSuppression(DataSuppression.DATA_SUPPRESSION_RAW)))
            .build();

    when(this.dataClassificationRulesTranslator.translateDataSuppression(
            DataSuppression.DATA_SUPPRESSION_RAW))
        .thenReturn(DataTransformation.DATA_TRANSFORMATION_UNSPECIFIED);
    assertEquals(
        List.of(expectedRawTransform),
        this.overrideRuleManager.applyOverrides(List.of(original), List.of(overrideToRaw)));
    assertEquals(
        Collections.emptyList(),
        this.overrideRuleManager.applyOverridesAndFilterRawRules(
            List.of(original), List.of(overrideToRaw)));
  }

  @Test
  void testApplyOverrideToRedact() {
    DataType original =
        DataType.newBuilder()
            .setDataTypeId("dt-id")
            .setTransformation(DataTransformation.DATA_TRANSFORMATION_OBFUSCATE)
            .build();
    DataType expectedRedactOverride =
        original.toBuilder()
            .setTransformation(DataTransformation.DATA_TRANSFORMATION_REDACT)
            .build();
    DataClassificationOverride overrideToRedact =
        DataClassificationOverride.newBuilder()
            .setDataClassificationOverrideRule(
                DataClassificationOverrideRule.newBuilder()
                    .setDataSuppressionOverride(
                        DataSuppressionOverride.newBuilder()
                            .setDataSuppression(DataSuppression.DATA_SUPPRESSION_REDACT)))
            .build();
    when(this.dataClassificationRulesTranslator.translateDataSuppression(
            DataSuppression.DATA_SUPPRESSION_REDACT))
        .thenReturn(DataTransformation.DATA_TRANSFORMATION_REDACT);
    assertEquals(
        List.of(expectedRedactOverride),
        this.overrideRuleManager.applyOverrides(List.of(original), List.of(overrideToRedact)));
    assertEquals(
        List.of(expectedRedactOverride),
        this.overrideRuleManager.applyOverridesAndFilterRawRules(
            List.of(original), List.of(overrideToRedact)));
  }

  @Test
  void testConditionalOverride() {
    // conditional override should retain original data type
    DataTypeMatchRule matchRule =
        DataTypeMatchRule.newBuilder()
            .setPathPredicate(
                PathPredicate.newBuilder()
                    .setPathSegmentPredicate(
                        StringPredicate.newBuilder()
                            .setOperator(
                                ai.traceable.external.data.classification.config.service.v1.Operator
                                    .OPERATOR_EQUALS)
                            .setValue("path")))
            .build();
    DataType original =
        DataType.newBuilder()
            .setDataTypeId("dt-id")
            .setTransformation(DataTransformation.DATA_TRANSFORMATION_OBFUSCATE)
            .addMatchRules(matchRule)
            .build();
    DataType expectedOverride =
        original.toBuilder()
            .setTransformation(DataTransformation.DATA_TRANSFORMATION_REDACT)
            .clearMatchRules()
            .addMatchRules(
                matchRule.toBuilder()
                    .setSpanFilter(
                        ai.traceable.external.data.classification.config.service.v1.SpanFilter
                            .newBuilder()
                            .addRequiredMatchingAttributes(
                                AttributePredicate.newBuilder()
                                    .setNamePredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(
                                                ai.traceable.external.data.classification.config
                                                    .service.v1.Operator.OPERATOR_EQUALS)
                                            .setValue("ip"))
                                    .setValuePredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(
                                                ai.traceable.external.data.classification.config
                                                    .service.v1.Operator.OPERATOR_EQUALS)
                                            .setValue("ip-value")))))
            .build();
    DataClassificationOverride overrideToRedact =
        DataClassificationOverride.newBuilder()
            .setDataClassificationOverrideRule(
                DataClassificationOverrideRule.newBuilder()
                    .setSpanFilter(
                        SpanFilter.newBuilder()
                            .setKeyValueFilter(
                                SpanKeyValueFilter.newBuilder()
                                    .setKeyPattern(
                                        StringPattern.newBuilder()
                                            .setOperator(Operator.OPERATOR_EQUALS)
                                            .setValue("ip"))
                                    .setValuePattern(
                                        StringPattern.newBuilder()
                                            .setOperator(Operator.OPERATOR_EQUALS)
                                            .setValue("ip-value"))
                                    .build()))
                    .setDataSuppressionOverride(
                        DataSuppressionOverride.newBuilder()
                            .setDataSuppression(DataSuppression.DATA_SUPPRESSION_REDACT)))
            .build();
    when(this.dataClassificationRulesTranslator.translateDataSuppression(
            DataSuppression.DATA_SUPPRESSION_REDACT))
        .thenReturn(DataTransformation.DATA_TRANSFORMATION_REDACT);
    assertEquals(
        List.of(expectedOverride, original),
        this.overrideRuleManager.applyOverrides(List.of(original), List.of(overrideToRedact)));
  }

  @Test
  void testMixedOverrides() {
    DataTypeMatchRule matchRule =
        DataTypeMatchRule.newBuilder()
            .setPathPredicate(
                PathPredicate.newBuilder()
                    .setPathSegmentPredicate(
                        StringPredicate.newBuilder()
                            .setOperator(
                                ai.traceable.external.data.classification.config.service.v1.Operator
                                    .OPERATOR_EQUALS)
                            .setValue("path")))
            .build();
    DataType original =
        DataType.newBuilder()
            .setDataTypeId("dt-id")
            .setTransformation(DataTransformation.DATA_TRANSFORMATION_OBFUSCATE)
            .addMatchRules(matchRule)
            .build();
    DataType expectedFirstOverride =
        original.toBuilder()
            .setTransformation(DataTransformation.DATA_TRANSFORMATION_REDACT)
            .clearMatchRules()
            .addMatchRules(
                matchRule.toBuilder()
                    .setSpanFilter(
                        ai.traceable.external.data.classification.config.service.v1.SpanFilter
                            .newBuilder()
                            .addRequiredMatchingAttributes(
                                AttributePredicate.newBuilder()
                                    .setNamePredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(
                                                ai.traceable.external.data.classification.config
                                                    .service.v1.Operator.OPERATOR_EQUALS)
                                            .setValue("ip"))
                                    .setValuePredicate(
                                        StringPredicate.newBuilder()
                                            .setOperator(
                                                ai.traceable.external.data.classification.config
                                                    .service.v1.Operator.OPERATOR_NOT_EQUALS)
                                            .setValue("ip-value")))))
            .build();
    DataType expectedSecondOverride = original.toBuilder().clearTransformation().build();
    DataClassificationOverride firstRedactingOverride =
        DataClassificationOverride.newBuilder()
            .setDataClassificationOverrideRule(
                DataClassificationOverrideRule.newBuilder()
                    .setSpanFilter(
                        SpanFilter.newBuilder()
                            .setNegationFilter(
                                SpanFilter.newBuilder()
                                    .setKeyValueFilter(
                                        SpanKeyValueFilter.newBuilder()
                                            .setKeyPattern(
                                                StringPattern.newBuilder()
                                                    .setOperator(Operator.OPERATOR_EQUALS)
                                                    .setValue("ip"))
                                            .setValuePattern(
                                                StringPattern.newBuilder()
                                                    .setOperator(Operator.OPERATOR_EQUALS)
                                                    .setValue("ip-value")))
                                    .build()))
                    .setDataSuppressionOverride(
                        DataSuppressionOverride.newBuilder()
                            .setDataSuppression(DataSuppression.DATA_SUPPRESSION_REDACT)))
            .build();

    DataClassificationOverride unconditionalRawOverride =
        DataClassificationOverride.newBuilder()
            .setDataClassificationOverrideRule(
                DataClassificationOverrideRule.newBuilder()
                    .setDataSuppressionOverride(
                        DataSuppressionOverride.newBuilder()
                            .setDataSuppression(DataSuppression.DATA_SUPPRESSION_RAW)))
            .build();

    when(this.dataClassificationRulesTranslator.translateDataSuppression(
            DataSuppression.DATA_SUPPRESSION_REDACT))
        .thenReturn(DataTransformation.DATA_TRANSFORMATION_REDACT);
    when(this.dataClassificationRulesTranslator.translateDataSuppression(
            DataSuppression.DATA_SUPPRESSION_RAW))
        .thenReturn(DataTransformation.DATA_TRANSFORMATION_UNSPECIFIED);
    assertEquals(
        List.of(expectedFirstOverride, expectedSecondOverride),
        this.overrideRuleManager.applyOverrides(
            List.of(original),
            List.of(
                firstRedactingOverride,
                unconditionalRawOverride,
                firstRedactingOverride,
                unconditionalRawOverride)));
  }

  @Test
  void testSkipOverrideOfSameKind() {
    DataType original =
        DataType.newBuilder()
            .setDataTypeId("dt-id")
            .setTransformation(DataTransformation.DATA_TRANSFORMATION_OBFUSCATE)
            .build();
    DataClassificationOverride overrideToRedact =
        DataClassificationOverride.newBuilder()
            .setDataClassificationOverrideRule(
                DataClassificationOverrideRule.newBuilder()
                    .setSpanFilter(
                        SpanFilter.newBuilder()
                            .setKeyValueFilter(
                                SpanKeyValueFilter.newBuilder()
                                    .setKeyPattern(
                                        StringPattern.newBuilder()
                                            .setOperator(Operator.OPERATOR_EQUALS)
                                            .setValue("ip"))
                                    .setValuePattern(
                                        StringPattern.newBuilder()
                                            .setOperator(Operator.OPERATOR_EQUALS)
                                            .setValue("ip-value"))
                                    .build()))
                    .setDataSuppressionOverride(
                        DataSuppressionOverride.newBuilder()
                            .setDataSuppression(DataSuppression.DATA_SUPPRESSION_OBFUSCATE)))
            .build();
    when(this.dataClassificationRulesTranslator.translateDataSuppression(
            DataSuppression.DATA_SUPPRESSION_OBFUSCATE))
        .thenReturn(DataTransformation.DATA_TRANSFORMATION_OBFUSCATE);
    assertEquals(
        List.of(original),
        this.overrideRuleManager.applyOverrides(List.of(original), List.of(overrideToRedact)));
  }
}
