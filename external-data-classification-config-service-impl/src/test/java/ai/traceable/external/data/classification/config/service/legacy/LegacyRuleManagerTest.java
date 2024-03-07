package ai.traceable.external.data.classification.config.service.legacy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.external.data.classification.config.service.legacy.RedactionRulesDao.RedactionRuleFilter;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.sensitivedata.config.service.v1.Parameter;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
  void testGetLegacyRedactionRules() {
    RequestContext testContext = RequestContext.forTenantId("testGetLegacyRedactionRules");
    List<RedactionRule> testRules = List.of(RedactionRule.newBuilder().setId("test-rule").build());
    List<DataType> expectedResults =
        List.of(DataType.newBuilder().setDataTypeId("test-rule").build());
    Set<String> enabledDataTypeIds = Set.of("test-rule");
    RedactionRuleFilter expectedRuleFilter = new RedactionRuleFilter(enabledDataTypeIds);
    when(this.redactionRulesDao.getRulesMatchFilter(testContext, expectedRuleFilter))
        .thenReturn(testRules);
    when(this.redactionRulesTranslator.translateRedactionRules(testRules))
        .thenReturn(expectedResults);

    assertEquals(
        expectedResults,
        this.legacyRuleManager.getDataTypesFromLegacyRedactionRules(
            testContext, enabledDataTypeIds));
  }

  @Test
  void testGetLegacySensitiveHeaders() {
    RequestContext testContext = RequestContext.forTenantId("testGetLegacySensitiveHeaders");

    assertEquals(
        Collections.emptyList(),
        this.legacyRuleManager.getDataTypesFromLegacySensitiveHeaders(
            testContext, Set.of("random-id")));

    Set<String> enabledDataTypeIds = Set.of("random-id", "legacy-datatype-sensitive-headers-id");

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
            testContext, enabledDataTypeIds));
  }
}
