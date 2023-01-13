package ai.traceable.risk.config.service.v2.contributors.builder;

import static ai.traceable.risk.config.service.v2.factors.MockFactorConfigsData.getDefaultSensitiveDataExposureFactor;
import static ai.traceable.risk.config.service.v2.factors.MockFactorConfigsData.getDefaultSensitiveDataExposureFactorWithChangedResponseSensitivityCriticalScore;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.risk.config.service.v2.RiskContributorConfigs;
import ai.traceable.risk.config.service.v2.factors.builder.RiskFactorConfigBuilder;
import ai.traceable.risk.config.service.v2.factors.builder.RiskFactorListBuilder;
import ai.traceable.risk.config.service.v2.factors.comparator.RiskFactorConfigsComparator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RiskContributorConfigBuilderTest {

  @Mock private RiskFactorConfigBuilder factorConfigBuilder;
  @Mock private RiskFactorConfigsComparator factorConfigsComparator;

  private RiskContributorConfigBuilder contributorConfigBuilder;

  @BeforeEach
  void setUp() {
    RiskFactorListBuilder riskFactorListBuilder =
        new RiskFactorListBuilder(factorConfigBuilder, factorConfigsComparator);
    contributorConfigBuilder = new RiskContributorConfigBuilder(riskFactorListBuilder);
  }

  @Test
  void testGetNewBuilder() {
    assertEquals(
        contributorConfigBuilder.getNewBuilder().build(),
        RiskContributorConfigs.getDefaultInstance());
  }

  @Test
  void testMergeConfigs() {
    final RiskContributorConfigs highPriorityConfig =
        RiskContributorConfigs.newBuilder()
            .addRiskFactors(
                getDefaultSensitiveDataExposureFactorWithChangedResponseSensitivityCriticalScore())
            .build();

    final RiskContributorConfigs lowPriorityConfig =
        RiskContributorConfigs.newBuilder()
            .addRiskFactors(getDefaultSensitiveDataExposureFactor())
            .build();

    final RiskContributorConfigs result =
        contributorConfigBuilder.mergeConfigs(highPriorityConfig, lowPriorityConfig);

    result
        .getRiskFactorsList()
        .forEach(
            riskFactor ->
                riskFactor
                    .getRiskFactorConfig()
                    .getRiskElementConfigsList()
                    .forEach(
                        riskElementConfig -> {
                          if (riskElementConfig.getId().equals("responseSensitivityCritical")) {
                            assertEquals(riskElementConfig.getRiskElementScoring().getScore(), 9);
                          }
                        }));
  }
}
