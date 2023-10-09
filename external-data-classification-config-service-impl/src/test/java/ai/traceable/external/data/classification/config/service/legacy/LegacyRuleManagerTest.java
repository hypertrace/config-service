package ai.traceable.external.data.classification.config.service.legacy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.external.data.classification.config.service.legacy.RedactionRulesDao.RedactionRuleFilter;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.sensitivedata.config.service.v1.Parameter;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LegacyRuleManagerTest {
  @Mock RedactionRulesDao redactionRulesDao;
  @Mock RedactionRulesTranslator redactionRulesTranslator;
  @Mock InsightsServiceCoordinator insightsServiceCoordinator;
  @InjectMocks LegacyRuleManager legacyRuleManager;

  @Test
  void testIsLegacy() {
    assertFalse(
        this.legacyRuleManager.isLegacyDataSet(
            DataSet.newBuilder().setId(UUID.randomUUID().toString()).build()));

    assertTrue(
        this.legacyRuleManager.isLegacyDataSet(
            DataSet.newBuilder().setId("legacy-dataset-redacted-id").build()));
  }

  @Test
  void testGetLegacyRedactionRules() {
    RequestContext testContext = RequestContext.forTenantId("testGetLegacyRedactionRules");
    List<RedactionRule> testRules = List.of(RedactionRule.newBuilder().setId("test-rule").build());
    List<DataType> expectedResults =
        List.of(DataType.newBuilder().setDataTypeId("test-rule").build());
    List<DataSet> dataSets =
        List.of(DataSet.newBuilder().setId("legacy-dataset-obfuscated-id").build());
    RedactionRuleFilter expectedRuleFilter =
        new RedactionRuleFilter(Set.of(RedactionStrategy.REDACTION_STRATEGY_HASH));
    when(this.redactionRulesDao.getEnabledRedactionRules(testContext, expectedRuleFilter))
        .thenReturn(testRules);
    when(this.redactionRulesTranslator.translateRedactionRules(testRules))
        .thenReturn(expectedResults);

    assertEquals(
        expectedResults,
        this.legacyRuleManager.getDataTypesFromLegacyRedactionRules(testContext, dataSets));
  }

  @Test
  void testGetLegacySensitiveHeaders() {
    RequestContext testContext = RequestContext.forTenantId("testGetLegacySensitiveHeaders");

    List<DataSet> unrelatedDataSet =
        List.of(DataSet.newBuilder().setId("random-new-data-set").build());
    assertEquals(
        Collections.emptyList(),
        this.legacyRuleManager.getDataTypesFromLegacySensitiveHeaders(
            testContext, unrelatedDataSet));

    List<DataSet> dataSetIncludingSensitiveHeader =
        List.of(
            DataSet.newBuilder().setId("random-new-data-set").build(),
            DataSet.newBuilder().setId("legacy-dataset-sensitive-headers-id").build());

    DataType expectedDataType = DataType.newBuilder().setDataTypeId("test-rule").build();
    List<Parameter> expectedParams = List.of(Parameter.newBuilder().setName("test").build());
    RedactionStrategy expectedStrategy = RedactionStrategy.REDACTION_STRATEGY_REDACT;
    when(this.insightsServiceCoordinator.getSensitiveHeaderParameters(testContext))
        .thenReturn(expectedParams);
    when(this.redactionRulesDao.getParamTypeHeaderRedactionStrategy(testContext))
        .thenReturn(expectedStrategy);
    when(this.redactionRulesTranslator.translateDataTypeForSensitiveHeaders(
            expectedParams, expectedStrategy))
        .thenReturn(Optional.of(expectedDataType));

    assertEquals(
        List.of(expectedDataType),
        this.legacyRuleManager.getDataTypesFromLegacySensitiveHeaders(
            testContext, dataSetIncludingSensitiveHeader));
  }
}
