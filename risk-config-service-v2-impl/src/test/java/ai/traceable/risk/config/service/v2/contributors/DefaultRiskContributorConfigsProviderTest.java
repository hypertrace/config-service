package ai.traceable.risk.config.service.v2.contributors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.risk.config.service.v2.RiskConfigServiceConfig;
import ai.traceable.risk.config.service.v2.RiskConfigServiceRequestValidator;
import ai.traceable.risk.config.service.v2.RiskContributorCategory;
import ai.traceable.risk.config.service.v2.RiskContributorConfigs;
import ai.traceable.risk.config.service.v2.RiskFactor;
import ai.traceable.risk.config.service.v2.RiskFactorCategory;
import ai.traceable.risk.config.service.v2.contributors.builder.RiskContributorConfigBuilder;
import ai.traceable.risk.config.service.v2.contributors.validator.RiskContributorConfigsValidator;
import ai.traceable.risk.config.service.v2.contributors.validator.RiskContributorConfigsValidatorImpl;
import ai.traceable.risk.config.service.v2.elements.builder.RiskElementConfigBuilder;
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

public class DefaultRiskContributorConfigsProviderTest {

  private final RiskContributorConfigBuilder configBuilder =
      new RiskContributorConfigBuilder(
          new RiskFactorListBuilder(
              new RiskFactorConfigBuilder(new RiskElementConfigBuilder()),
              new RiskFactorConfigsComparatorImpl()));
  private final RiskContributorConfigsValidator contributorConfigsValidator =
      new RiskContributorConfigsValidatorImpl(
          new RiskFactorConfigsValidatorImpl(
              new RiskElementConfigValidatorImpl(new RiskConfigServiceRequestValidator()),
              new RiskConfigServiceRequestValidator()),
          new RiskConfigServiceRequestValidator());

  @Test
  public void testGetNoServiceConfig() {
    DefaultRiskContributorConfigsProvider provider =
        new DefaultRiskContributorConfigsProvider(
            configBuilder,
            contributorConfigsValidator,
            new RiskConfigServiceConfig(ConfigFactory.empty()));
    RiskContributorConfigs configs = provider.get();
    assertEquals(6, configs.getRiskFactorsCount());
    Map<RiskFactorCategory, RiskFactor> factorsMap =
        configs.getRiskFactorsList().stream()
            .collect(
                Collectors.toMap(
                    riskFactor -> riskFactor.getRiskFactorConfig().getRiskFactorCategory(),
                    Function.identity()));
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
          .forEach(riskElementConfig -> assertTrue(riskElementConfig.getId().length() > 0));
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
          .forEach(riskElementConfig -> assertTrue(riskElementConfig.getId().length() > 0));
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
          .forEach(riskElementConfig -> assertTrue(riskElementConfig.getId().length() > 0));
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
          .forEach(riskElementConfig -> assertTrue(riskElementConfig.getId().length() > 0));
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
          .forEach(riskElementConfig -> assertTrue(riskElementConfig.getId().length() > 0));
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
          .forEach(riskElementConfig -> assertTrue(riskElementConfig.getId().length() > 0));
    }
  }

  @Test
  public void testGetWithServiceConfig() {
    DefaultRiskContributorConfigsProvider provider =
        new DefaultRiskContributorConfigsProvider(
            configBuilder,
            contributorConfigsValidator,
            new RiskConfigServiceConfig(
                ConfigFactory.parseResources("valid-risk-contributor-config.conf")));
    RiskContributorConfigs configs = provider.get();
    assertEquals(6, configs.getRiskFactorsCount());
    Map<RiskFactorCategory, RiskFactor> factorsMap =
        configs.getRiskFactorsList().stream()
            .collect(
                Collectors.toMap(
                    riskFactor -> riskFactor.getRiskFactorConfig().getRiskFactorCategory(),
                    Function.identity()));
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
                assertTrue(riskElementConfig.getId().length() > 0);
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
                assertTrue(riskElementConfig.getId().length() > 0);
                if (riskElementConfig.getRiskElementPredicate().hasDependentApis()) {
                  assertEquals(
                      2, riskElementConfig.getRiskElementPredicate().getDependentApis().getValue());
                }
              });
    }
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
          .forEach(riskElementConfig -> assertTrue(riskElementConfig.getId().length() > 0));
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
          .forEach(riskElementConfig -> assertTrue(riskElementConfig.getId().length() > 0));
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
          .forEach(riskElementConfig -> assertTrue(riskElementConfig.getId().length() > 0));
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
          .forEach(riskElementConfig -> assertTrue(riskElementConfig.getId().length() > 0));
    }
  }

  @Test
  public void testGetWithInvalidServiceConfig() {
    DefaultRiskContributorConfigsProvider provider =
        new DefaultRiskContributorConfigsProvider(
            configBuilder,
            contributorConfigsValidator,
            new RiskConfigServiceConfig(
                ConfigFactory.parseResources("risk-contributor-config-with-invalid-score.conf")));
    assertThrows(StatusRuntimeException.class, provider::get);

    provider =
        new DefaultRiskContributorConfigsProvider(
            configBuilder,
            contributorConfigsValidator,
            new RiskConfigServiceConfig(
                ConfigFactory.parseResources(
                    "risk-contributor-config-with-incomplete-factor.conf")));
    assertThrows(StatusRuntimeException.class, provider::get);
  }
}
