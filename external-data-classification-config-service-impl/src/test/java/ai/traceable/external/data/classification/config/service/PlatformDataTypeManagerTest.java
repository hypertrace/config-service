package ai.traceable.external.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.PredicateSupportLevel;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PlatformDataTypeManagerTest {
  @Mock DataClassificationRulesDao dataClassificationRulesDao;
  @Mock DataClassificationRulesTranslator rulesTranslator;
  @InjectMocks PlatformDataTypeManager platformDataTypeManager;

  @Test
  void testGetDataTypes() {
    RequestContext testContext = RequestContext.forTenantId("testGetDataTypes");
    DataType dataType1 =
        DataType.newBuilder()
            .setId("datatype-1")
            .setRule(
                DataTypeRule.newBuilder()
                    .setName("datatypename-1")
                    .addScopedPatterns(
                        DataTypeRule.ScopedPattern.newBuilder()
                            .setGlobalScope(DataTypeRule.GlobalScope.newBuilder())))
            .build();

    DataType dataType2 =
        DataType.newBuilder()
            .setId("datatype-2")
            .setRule(
                DataTypeRule.newBuilder()
                    .setName("datatypename-2")
                    .addScopedPatterns(
                        DataTypeRule.ScopedPattern.newBuilder()
                            .setEnvironmentScope(
                                DataTypeRule.EnvironmentScope.newBuilder()
                                    .addEnvironmentIds("env-1"))))
            .build();

    when(this.dataClassificationRulesDao.getResolvedDataTypesInEvaluationOrder(testContext))
        .thenReturn(List.of(dataType2, dataType1));

    List<ai.traceable.external.data.classification.config.service.v1.DataType> expectedResult =
        List.of(
            ai.traceable.external.data.classification.config.service.v1.DataType.newBuilder()
                .setDataTypeId(dataType2.getId())
                .build(),
            ai.traceable.external.data.classification.config.service.v1.DataType.newBuilder()
                .setDataTypeId(dataType1.getId())
                .build());

    when(this.rulesTranslator.translateDataTypes(
            List.of(dataType2, dataType1),
            Optional.of("env-1"),
            PredicateSupportLevel.PREDICATE_SUPPORT_LEVEL_LEAF_PATH_SEGMENT))
        .thenReturn(expectedResult);
    assertEquals(
        expectedResult,
        this.platformDataTypeManager.getDataTypes(
            testContext,
            Optional.of("env-1"),
            PredicateSupportLevel.PREDICATE_SUPPORT_LEVEL_LEAF_PATH_SEGMENT));
  }
}
