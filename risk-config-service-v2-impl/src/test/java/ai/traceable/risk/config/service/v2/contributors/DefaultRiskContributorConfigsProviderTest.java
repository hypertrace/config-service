package ai.traceable.risk.config.service.v2.contributors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import ai.traceable.risk.config.service.v2.EntityType;
import ai.traceable.risk.config.service.v2.RiskConfigServiceConfig;
import ai.traceable.risk.config.service.v2.RiskConfigServiceRequestValidator;
import ai.traceable.risk.config.service.v2.RiskContributorCategory;
import ai.traceable.risk.config.service.v2.RiskContributorConfigs;
import ai.traceable.risk.config.service.v2.RiskFactor;
import ai.traceable.risk.config.service.v2.RiskFactorCategory;
import ai.traceable.risk.config.service.v2.contributors.builder.RiskContributorConfigBuilder;
import ai.traceable.risk.config.service.v2.contributors.validator.RiskContributorConfigsValidator;
import ai.traceable.risk.config.service.v2.contributors.validator.RiskContributorConfigsValidatorImpl;
import ai.traceable.risk.config.service.v2.elements.builder.LabelElementConfigsBuilder;
import ai.traceable.risk.config.service.v2.elements.builder.LabelPredicateBuilder;
import ai.traceable.risk.config.service.v2.elements.builder.LabelsConfigProvider;
import ai.traceable.risk.config.service.v2.elements.builder.RiskElementConfigBuilder;
import ai.traceable.risk.config.service.v2.elements.normalizer.LabelIdNormalizer;
import ai.traceable.risk.config.service.v2.elements.validator.RiskElementConfigValidatorImpl;
import ai.traceable.risk.config.service.v2.factors.builder.RiskFactorConfigBuilder;
import ai.traceable.risk.config.service.v2.factors.builder.RiskFactorListBuilder;
import ai.traceable.risk.config.service.v2.factors.comparator.RiskFactorConfigsComparatorImpl;
import ai.traceable.risk.config.service.v2.factors.validator.RiskFactorConfigsValidatorImpl;
import com.typesafe.config.ConfigFactory;
import io.grpc.StatusRuntimeException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class DefaultRiskContributorConfigsProviderTest {

  private final LabelsConfigProvider labelsConfigProvider = mock(LabelsConfigProvider.class);

  private final RiskContributorConfigBuilder configBuilder =
      new RiskContributorConfigBuilder(
          new RiskFactorListBuilder(
              new RiskFactorConfigBuilder(
                  new RiskElementConfigBuilder(),
                  new LabelElementConfigsBuilder(
                      new LabelPredicateBuilder(labelsConfigProvider), new LabelIdNormalizer())),
              new RiskFactorConfigsComparatorImpl()));
  private final RiskContributorConfigsValidator contributorConfigsValidator =
      new RiskContributorConfigsValidatorImpl(
          new RiskFactorConfigsValidatorImpl(
              new RiskElementConfigValidatorImpl(new RiskConfigServiceRequestValidator()),
              new RiskConfigServiceRequestValidator()),
          new RiskConfigServiceRequestValidator());

  @Test
  void testGetNoServiceConfigApiLikelihoodFactors() {
    // Given
    var factorsMap = getFactorsMap(buildProvider(null), EntityType.ENTITY_TYPE_API);
    // Then
    {
      RiskFactor factor = factorsMap.get(RiskFactorCategory.RISK_FACTOR_CATEGORY_EASE_OF_ACCESS);
      assertTrue(factor.getRiskFactorInfo().getIsDefault());
      assertEquals(
          RiskContributorCategory.RISK_CONTRIBUTOR_CATEGORY_LIKELIHOOD,
          factor.getRiskFactorInfo().getRiskContributorCategory());
      assertFalse(factor.getRiskFactorConfig().getDisabled());
      assertEquals(12, factor.getRiskFactorConfig().getRiskElementConfigsCount());
      factor
          .getRiskFactorConfig()
          .getRiskElementConfigsList()
          .forEach(riskElementConfig -> assertFalse(riskElementConfig.getId().isEmpty()));
    }
    {
      RiskFactor factor = factorsMap.get(RiskFactorCategory.RISK_FACTOR_CATEGORY_VULNERABILITY);
      assertTrue(factor.getRiskFactorInfo().getIsDefault());
      assertEquals(
          RiskContributorCategory.RISK_CONTRIBUTOR_CATEGORY_LIKELIHOOD,
          factor.getRiskFactorInfo().getRiskContributorCategory());
      assertFalse(factor.getRiskFactorConfig().getDisabled());
      assertEquals(4, factor.getRiskFactorConfig().getRiskElementConfigsCount());
      factor
          .getRiskFactorConfig()
          .getRiskElementConfigsList()
          .forEach(riskElementConfig -> assertFalse(riskElementConfig.getId().isEmpty()));
    }
    {
      RiskFactor factor =
          factorsMap.get(RiskFactorCategory.RISK_FACTOR_CATEGORY_EASE_OF_RESOURCE_DISCOVERY);
      assertTrue(factor.getRiskFactorInfo().getIsDefault());
      assertEquals(
          RiskContributorCategory.RISK_CONTRIBUTOR_CATEGORY_LIKELIHOOD,
          factor.getRiskFactorInfo().getRiskContributorCategory());
      assertFalse(factor.getRiskFactorConfig().getDisabled());
      assertEquals(3, factor.getRiskFactorConfig().getRiskElementConfigsCount());
      factor
          .getRiskFactorConfig()
          .getRiskElementConfigsList()
          .forEach(riskElementConfig -> assertFalse(riskElementConfig.getId().isEmpty()));
    }
  }

  @Test
  void testGetNoServiceConfigApiImpactFactors() {
    // Given
    var factorsMap = getFactorsMap(buildProvider(null), EntityType.ENTITY_TYPE_API);
    // Then
    assertEquals(6, factorsMap.size());
    {
      RiskFactor factor =
          factorsMap.get(RiskFactorCategory.RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE);
      assertTrue(factor.getRiskFactorInfo().getIsDefault());
      assertEquals(
          RiskContributorCategory.RISK_CONTRIBUTOR_CATEGORY_IMPACT,
          factor.getRiskFactorInfo().getRiskContributorCategory());
      assertFalse(factor.getRiskFactorConfig().getDisabled());
      assertEquals(4, factor.getRiskFactorConfig().getRiskElementConfigsCount());
      factor
          .getRiskFactorConfig()
          .getRiskElementConfigsList()
          .forEach(riskElementConfig -> assertFalse(riskElementConfig.getId().isEmpty()));
    }
    {
      RiskFactor factor = factorsMap.get(RiskFactorCategory.RISK_FACTOR_CATEGORY_BLAST_RADIUS);
      assertTrue(factor.getRiskFactorInfo().getIsDefault());
      assertEquals(
          RiskContributorCategory.RISK_CONTRIBUTOR_CATEGORY_IMPACT,
          factor.getRiskFactorInfo().getRiskContributorCategory());
      assertFalse(factor.getRiskFactorConfig().getDisabled());
      assertEquals(2, factor.getRiskFactorConfig().getRiskElementConfigsCount());
      factor
          .getRiskFactorConfig()
          .getRiskElementConfigsList()
          .forEach(riskElementConfig -> assertFalse(riskElementConfig.getId().isEmpty()));
    }
    {
      RiskFactor factor = factorsMap.get(RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS);
      assertTrue(factor.getRiskFactorInfo().getIsDefault());
      assertEquals(
          RiskContributorCategory.RISK_CONTRIBUTOR_CATEGORY_IMPACT,
          factor.getRiskFactorInfo().getRiskContributorCategory());
      assertTrue(factor.getRiskFactorConfig().getDisabled());
      assertEquals(3, factor.getRiskFactorConfig().getRiskElementConfigsCount());
      factor
          .getRiskFactorConfig()
          .getRiskElementConfigsList()
          .forEach(riskElementConfig -> assertFalse(riskElementConfig.getId().isEmpty()));
    }
  }

  @Test
  void testGetNoServiceConfigMcpTool() {
    // Given
    var factorsMap = getFactorsMap(buildProvider(null), EntityType.ENTITY_TYPE_MCP_TOOL);
    // Then
    assertEquals(4, factorsMap.size());
    assertTrue(factorsMap.containsKey(RiskFactorCategory.RISK_FACTOR_CATEGORY_EASE_OF_ACCESS));
    assertTrue(factorsMap.containsKey(RiskFactorCategory.RISK_FACTOR_CATEGORY_VULNERABILITY));
    assertTrue(
        factorsMap.containsKey(RiskFactorCategory.RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE));
    assertTrue(factorsMap.containsKey(RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS));
    assertFalse(
        factorsMap.containsKey(RiskFactorCategory.RISK_FACTOR_CATEGORY_EASE_OF_RESOURCE_DISCOVERY));
    assertFalse(factorsMap.containsKey(RiskFactorCategory.RISK_FACTOR_CATEGORY_BLAST_RADIUS));
  }

  @Test
  void testGetWithServiceConfigApiLikelihoodFactors() {
    // Given
    var factorsMap =
        getFactorsMap(
            buildProvider("valid-risk-contributor-config.conf"), EntityType.ENTITY_TYPE_API);
    // Then
    {
      RiskFactor factor = factorsMap.get(RiskFactorCategory.RISK_FACTOR_CATEGORY_EASE_OF_ACCESS);
      assertTrue(factor.getRiskFactorInfo().getIsDefault());
      assertEquals(
          RiskContributorCategory.RISK_CONTRIBUTOR_CATEGORY_LIKELIHOOD,
          factor.getRiskFactorInfo().getRiskContributorCategory());
      assertFalse(factor.getRiskFactorConfig().getDisabled());
      assertEquals(12, factor.getRiskFactorConfig().getRiskElementConfigsCount());
      factor
          .getRiskFactorConfig()
          .getRiskElementConfigsList()
          .forEach(riskElementConfig -> assertFalse(riskElementConfig.getId().isEmpty()));
    }
    {
      RiskFactor factor = factorsMap.get(RiskFactorCategory.RISK_FACTOR_CATEGORY_VULNERABILITY);
      assertTrue(factor.getRiskFactorInfo().getIsDefault());
      assertEquals(
          RiskContributorCategory.RISK_CONTRIBUTOR_CATEGORY_LIKELIHOOD,
          factor.getRiskFactorInfo().getRiskContributorCategory());
      assertFalse(factor.getRiskFactorConfig().getDisabled());
      assertEquals(4, factor.getRiskFactorConfig().getRiskElementConfigsCount());
      factor
          .getRiskFactorConfig()
          .getRiskElementConfigsList()
          .forEach(riskElementConfig -> assertFalse(riskElementConfig.getId().isEmpty()));
    }
    {
      RiskFactor factor =
          factorsMap.get(RiskFactorCategory.RISK_FACTOR_CATEGORY_EASE_OF_RESOURCE_DISCOVERY);
      assertTrue(factor.getRiskFactorInfo().getIsDefault());
      assertEquals(
          RiskContributorCategory.RISK_CONTRIBUTOR_CATEGORY_LIKELIHOOD,
          factor.getRiskFactorInfo().getRiskContributorCategory());
      assertFalse(factor.getRiskFactorConfig().getDisabled());
      assertEquals(3, factor.getRiskFactorConfig().getRiskElementConfigsCount());
      factor
          .getRiskFactorConfig()
          .getRiskElementConfigsList()
          .forEach(riskElementConfig -> assertFalse(riskElementConfig.getId().isEmpty()));
    }
  }

  @Test
  void testGetWithServiceConfigApiImpactFactors() {
    // Given
    var factorsMap =
        getFactorsMap(
            buildProvider("valid-risk-contributor-config.conf"), EntityType.ENTITY_TYPE_API);
    // Then
    assertEquals(6, factorsMap.size());
    {
      RiskFactor factor = factorsMap.get(RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS);
      assertTrue(factor.getRiskFactorInfo().getIsDefault());
      assertEquals(
          RiskContributorCategory.RISK_CONTRIBUTOR_CATEGORY_IMPACT,
          factor.getRiskFactorInfo().getRiskContributorCategory());
      assertFalse(factor.getRiskFactorConfig().getDisabled());
      assertEquals(3, factor.getRiskFactorConfig().getRiskElementConfigsCount());
      factor
          .getRiskFactorConfig()
          .getRiskElementConfigsList()
          .forEach(
              riskElementConfig -> {
                assertFalse(riskElementConfig.getId().isEmpty());
                if (riskElementConfig.getId().equals("sensitive")) {
                  assertEquals(8, riskElementConfig.getRiskElementScoring().getScore());
                }
              });
    }
    {
      RiskFactor factor = factorsMap.get(RiskFactorCategory.RISK_FACTOR_CATEGORY_BLAST_RADIUS);
      assertTrue(factor.getRiskFactorInfo().getIsDefault());
      assertEquals(
          RiskContributorCategory.RISK_CONTRIBUTOR_CATEGORY_IMPACT,
          factor.getRiskFactorInfo().getRiskContributorCategory());
      assertFalse(factor.getRiskFactorConfig().getDisabled());
      assertEquals(2, factor.getRiskFactorConfig().getRiskElementConfigsCount());
      factor
          .getRiskFactorConfig()
          .getRiskElementConfigsList()
          .forEach(
              riskElementConfig -> {
                assertFalse(riskElementConfig.getId().isEmpty());
                if (riskElementConfig.getRiskElementPredicate().hasDependentApis()) {
                  assertEquals(
                      2, riskElementConfig.getRiskElementPredicate().getDependentApis().getValue());
                }
              });
    }
    {
      RiskFactor factor =
          factorsMap.get(RiskFactorCategory.RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE);
      assertTrue(factor.getRiskFactorInfo().getIsDefault());
      assertEquals(
          RiskContributorCategory.RISK_CONTRIBUTOR_CATEGORY_IMPACT,
          factor.getRiskFactorInfo().getRiskContributorCategory());
      assertFalse(factor.getRiskFactorConfig().getDisabled());
      assertEquals(4, factor.getRiskFactorConfig().getRiskElementConfigsCount());
      factor
          .getRiskFactorConfig()
          .getRiskElementConfigsList()
          .forEach(riskElementConfig -> assertFalse(riskElementConfig.getId().isEmpty()));
    }
  }

  @Test
  void testGetWithServiceConfigMcpToolIsUnaffected() {
    // Given
    DefaultRiskContributorConfigsProvider provider =
        buildProvider("valid-risk-contributor-config.conf");
    // When
    RiskContributorConfigs mcpToolConfigs = provider.get().get(EntityType.ENTITY_TYPE_MCP_TOOL);
    // Then
    assertEquals(4, mcpToolConfigs.getRiskFactorsCount());
  }

  @Test
  void testGetWithInvalidScore() {
    // Given
    DefaultRiskContributorConfigsProvider provider =
        buildProvider("risk-contributor-config-with-invalid-score.conf");
    // Then
    assertThrows(StatusRuntimeException.class, provider::get);
  }

  @Test
  void testGetWithIncompleteFactor() {
    // Given
    DefaultRiskContributorConfigsProvider provider =
        buildProvider("risk-contributor-config-with-incomplete-factor.conf");
    // Then
    assertThrows(StatusRuntimeException.class, provider::get);
  }

  private DefaultRiskContributorConfigsProvider buildProvider(String configResource) {
    return new DefaultRiskContributorConfigsProvider(
        configBuilder,
        contributorConfigsValidator,
        new RiskConfigServiceConfig(
            configResource == null
                ? ConfigFactory.empty()
                : ConfigFactory.parseResources(configResource)));
  }

  private Map<RiskFactorCategory, RiskFactor> getFactorsMap(
      DefaultRiskContributorConfigsProvider provider, EntityType entityType) {
    RiskContributorConfigs configs = provider.get().get(entityType);
    return configs.getRiskFactorsList().stream()
        .collect(
            Collectors.toMap(
                riskFactor -> riskFactor.getRiskFactorConfig().getRiskFactorCategory(),
                Function.identity()));
  }
}
