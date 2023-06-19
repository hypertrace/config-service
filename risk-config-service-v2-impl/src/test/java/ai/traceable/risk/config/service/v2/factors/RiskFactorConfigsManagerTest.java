package ai.traceable.risk.config.service.v2.factors;

import static ai.traceable.risk.config.service.v2.factors.MockFactorConfigsData.buildRiskFactorWithoutScope;
import static ai.traceable.risk.config.service.v2.factors.MockFactorConfigsData.getDefaultRiskContributorConfigs;
import static ai.traceable.risk.config.service.v2.scope.MockScopeData.getEnvironmentBasedScope;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.risk.config.service.v2.RiskConfigIdGenerator;
import ai.traceable.risk.config.service.v2.RiskConfigScope;
import ai.traceable.risk.config.service.v2.RiskContributorConfigs;
import ai.traceable.risk.config.service.v2.RiskFactorCategory;
import ai.traceable.risk.config.service.v2.RiskFactorConfig;
import ai.traceable.risk.config.service.v2.RiskFactorConfigUpdateDetails;
import ai.traceable.risk.config.service.v2.elements.builder.RiskElementConfigBuilder;
import ai.traceable.risk.config.service.v2.factors.builder.RiskFactorConfigBuilder;
import ai.traceable.risk.config.service.v2.factors.builder.RiskFactorListBuilder;
import ai.traceable.risk.config.service.v2.factors.comparator.RiskFactorConfigsComparator;
import ai.traceable.risk.config.service.v2.factors.comparator.RiskFactorConfigsComparatorImpl;
import ai.traceable.risk.config.service.v2.factors.manager.RiskFactorConfigsManager;
import ai.traceable.risk.config.service.v2.factors.manager.RiskFactorConfigsManagerImpl;
import java.util.List;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RiskFactorConfigsManagerTest {
  private RiskContributorConfigs defaultRiskContributorConfigs;
  private RiskFactorConfigsManager configsManager;
  private final RiskFactorConfigBuilder riskFactorConfigBuilder =
      new RiskFactorConfigBuilder(new RiskElementConfigBuilder());
  private final RiskFactorConfigsComparator factorConfigsComparator =
      new RiskFactorConfigsComparatorImpl();
  private RiskConfigScope environmentRiskConfigScope;

  @BeforeEach
  public void setup() {
    defaultRiskContributorConfigs = getDefaultRiskContributorConfigs();
    RiskConfigIdGenerator configIdGenerator = new RiskConfigIdGenerator(new UuidGenerator());
    IdentifiedObjectStore<RiskFactorConfig> factorConfigStore =
        new MockFactorConfigsData.MockRiskFactorConfigStore(
            null, null, null, mock(ConfigChangeEventGenerator.class), configIdGenerator);
    environmentRiskConfigScope = getEnvironmentBasedScope();
    configsManager =
        new RiskFactorConfigsManagerImpl(
            factorConfigStore,
            new RiskFactorListBuilder(riskFactorConfigBuilder, factorConfigsComparator),
            riskFactorConfigBuilder,
            factorConfigsComparator,
            defaultRiskContributorConfigs,
            configIdGenerator);
  }

  @Test
  public void testGetUpdateDeleteRiskContributorConfigs() {
    RequestContext requestContext = RequestContext.forTenantId("tenant");

    RiskContributorConfigs fetchedRiskContributorConfigs =
        configsManager.getRiskContributorConfigs(requestContext, environmentRiskConfigScope);
    assertEquals(
        defaultRiskContributorConfigs.getRiskFactorsCount(),
        fetchedRiskContributorConfigs.getRiskFactorsCount());
    fetchedRiskContributorConfigs
        .getRiskFactorsList()
        .forEach(
            riskFactor ->
                assertTrue(
                    defaultRiskContributorConfigs
                        .getRiskFactorsList()
                        .contains(buildRiskFactorWithoutScope(riskFactor))));

    RiskFactorConfigUpdateDetails updateDetailsWithDisabledUpdateOnly =
        RiskFactorConfigUpdateDetails.newBuilder()
            .setRiskFactorCategory(
                RiskFactorCategory.RISK_FACTOR_CATEGORY_EASE_OF_RESOURCE_DISCOVERY)
            .setDisabled(true)
            .build();
    fetchedRiskContributorConfigs =
        configsManager.updateRiskContributorConfigs(
            requestContext,
            List.of(updateDetailsWithDisabledUpdateOnly),
            environmentRiskConfigScope);
    fetchedRiskContributorConfigs
        .getRiskFactorsList()
        .forEach(
            riskFactor -> {
              if (riskFactor
                  .getRiskFactorConfig()
                  .getRiskFactorCategory()
                  .equals(RiskFactorCategory.RISK_FACTOR_CATEGORY_EASE_OF_RESOURCE_DISCOVERY)) {
                assertTrue(riskFactor.getRiskFactorConfig().getDisabled());
              }
            });

    updateDetailsWithDisabledUpdateOnly =
        RiskFactorConfigUpdateDetails.newBuilder()
            .setRiskFactorCategory(
                RiskFactorCategory.RISK_FACTOR_CATEGORY_EASE_OF_RESOURCE_DISCOVERY)
            .setDisabled(false)
            .build();
    fetchedRiskContributorConfigs =
        configsManager.updateRiskContributorConfigs(
            requestContext,
            List.of(updateDetailsWithDisabledUpdateOnly),
            environmentRiskConfigScope);
    fetchedRiskContributorConfigs
        .getRiskFactorsList()
        .forEach(
            riskFactor -> {
              if (riskFactor
                  .getRiskFactorConfig()
                  .getRiskFactorCategory()
                  .equals(RiskFactorCategory.RISK_FACTOR_CATEGORY_EASE_OF_RESOURCE_DISCOVERY)) {
                assertFalse(riskFactor.getRiskFactorConfig().getDisabled());
              }
            });
  }
}
