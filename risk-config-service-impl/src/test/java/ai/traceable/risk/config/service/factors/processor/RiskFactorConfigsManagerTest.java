package ai.traceable.risk.config.service.factors.processor;

import static ai.traceable.risk.config.service.factors.processor.MockFactorConfigsData.getDefaultMotiveFactor;
import static ai.traceable.risk.config.service.factors.processor.MockFactorConfigsData.getDefaultSensitiveDataExposureFactor;
import static ai.traceable.risk.config.service.factors.processor.MockFactorConfigsData.getSensitiveDataExposureFactorConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.risk.config.service.factors.RiskFactorConfigsManager;
import ai.traceable.risk.config.service.factors.processor.utils.RiskElementConfigUtils;
import ai.traceable.risk.config.service.factors.processor.utils.RiskFactorConfigUtils;
import ai.traceable.risk.config.service.factors.processor.utils.RiskFactorListUtils;
import ai.traceable.risk.config.service.v1.IntOperator;
import ai.traceable.risk.config.service.v1.IntPredicate;
import ai.traceable.risk.config.service.v1.RiskContributorConfigs;
import ai.traceable.risk.config.service.v1.RiskContributorConfigsResetFilter;
import ai.traceable.risk.config.service.v1.RiskElementConfig;
import ai.traceable.risk.config.service.v1.RiskElementInfo;
import ai.traceable.risk.config.service.v1.RiskElementScoring;
import ai.traceable.risk.config.service.v1.RiskFactorConfig;
import ai.traceable.risk.config.service.v1.RiskFactorScoreContribution;
import ai.traceable.risk.config.service.v1.RiskFactorScoring;
import ai.traceable.risk.config.service.v1.StringOperator;
import ai.traceable.risk.config.service.v1.StringPredicate;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RiskFactorConfigsManagerTest {
  private IdentifiedObjectStore<RiskFactorConfig> factorConfigStore;
  private IdentifiedObjectStore<RiskElementConfig> elementConfigStore;
  private RiskContributorConfigs defaultRiskContributorConfigs;
  private RiskContributorConfigs defaultRiskImpactConfigs;
  private RiskFactorConfigsManager configsManager;

  @BeforeEach
  public void setup() {
    defaultRiskContributorConfigs = MockFactorConfigsData.mockDefaultRiskContributorConfigs();
    defaultRiskImpactConfigs = MockFactorConfigsData.mockDefaultRiskImpactConfigs();
    factorConfigStore = new MockFactorConfigsData.MockRiskFactorConfigStore(null, null, null);
    elementConfigStore = new MockFactorConfigsData.MockRiskElementConfigStore(null, null, null);
    configsManager =
        new RiskFactorConfigsManagerImpl(
            factorConfigStore,
            elementConfigStore,
            new RiskFactorListUtils(new RiskFactorConfigUtils(new RiskElementConfigUtils())),
            defaultRiskContributorConfigs,
            defaultRiskImpactConfigs);
  }

  @Test
  public void testGetUpdateDeleteLikelihoodConfigs() {
    RequestContext requestContext = RequestContext.forTenantId("likelihood-tenant");

    // check for get default configs
    RiskContributorConfigs riskLikelihoodConfigs =
        configsManager.getRiskLikelihoodConfigs(requestContext);
    assertEquals(
        defaultRiskContributorConfigs.getRiskFactorsCount(),
        riskLikelihoodConfigs.getRiskFactorsCount());
    riskLikelihoodConfigs
        .getRiskFactorsList()
        .forEach(
            factor ->
                assertTrue(defaultRiskContributorConfigs.getRiskFactorsList().contains(factor)));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            configsManager.updateRiskLikelihoodConfigs(
                requestContext, List.of(RiskFactorConfig.getDefaultInstance())));
    assertThrows(
        StatusRuntimeException.class,
        () ->
            configsManager.updateRiskLikelihoodConfigs(
                requestContext, List.of(RiskFactorConfig.newBuilder().setId("random").build())));
    assertThrows(
        StatusRuntimeException.class,
        () ->
            configsManager.updateRiskLikelihoodConfigs(
                requestContext,
                List.of(
                    RiskFactorConfig.newBuilder()
                        .setId("motive")
                        .addRiskElementConfigs(
                            RiskElementConfig.newBuilder().setId("random").build())
                        .build())));
    assertThrows(
        StatusRuntimeException.class,
        () ->
            configsManager.updateRiskLikelihoodConfigs(
                requestContext, List.of(getCustomTagUpdatedConfig(false))));

    // update modified configs
    RiskFactorConfig riskFactorConfig1 =
        RiskFactorConfig.newBuilder()
            .setId("motive")
            .addRiskElementConfigs(
                RiskElementConfig.newBuilder()
                    .setId("response-has-pii")
                    .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(9)))
            .setRiskFactorScoring(
                RiskFactorScoring.newBuilder()
                    .setDisabled(true)
                    .setScoreContribution(
                        RiskFactorScoreContribution.RISK_FACTOR_SCORE_CONTRIBUTION_ABSOLUTE))
            .build();
    RiskFactorConfig riskFactorConfig2 = getCustomTagUpdatedConfig(true);
    RiskFactorConfig updatedRiskFactorConfig1 =
        RiskFactorConfig.newBuilder()
            .setId("motive")
            .addRiskElementConfigs(
                RiskElementConfig.newBuilder()
                    .setId("response-has-pii")
                    .setRiskElementInfo(
                        RiskElementInfo.newBuilder()
                            .setResponsePiiCount(
                                IntPredicate.newBuilder()
                                    .setOperator(IntOperator.INT_OPERATOR_GREATER_THAN)
                                    .setValue(0)))
                    .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(9)))
            .setRiskFactorScoring(RiskFactorScoring.newBuilder().setDisabled(true))
            .build();

    configsManager.updateRiskImpactConfigs(
        requestContext,
        List.of(
            getDefaultSensitiveDataExposureFactor().getRiskFactorConfig().toBuilder()
                .setRiskFactorScoring(RiskFactorScoring.newBuilder().setDisabled(true))
                .build()));
    configsManager.updateRiskLikelihoodConfigs(
        requestContext, List.of(riskFactorConfig1, riskFactorConfig2));
    riskLikelihoodConfigs = configsManager.getRiskLikelihoodConfigs(requestContext);
    assertEquals(
        defaultRiskContributorConfigs.getRiskFactorsCount(),
        riskLikelihoodConfigs.getRiskFactorsCount());
    assertTrue(
        riskLikelihoodConfigs
            .getRiskFactorsList()
            .contains(
                MockFactorConfigsData.getDefaultMotiveFactor().toBuilder()
                    .setRiskFactorConfig(updatedRiskFactorConfig1)
                    .setIsDefault(false)
                    .build()));
    assertTrue(
        riskLikelihoodConfigs
            .getRiskFactorsList()
            .contains(
                MockFactorConfigsData.getDefaultCustomTagRiskFactor().toBuilder()
                    .setRiskFactorConfig(riskFactorConfig2)
                    .setIsDefault(false)
                    .build()));

    // update config back to default config
    configsManager.updateRiskLikelihoodConfigs(
        requestContext,
        List.of(MockFactorConfigsData.getDefaultMotiveFactor().getRiskFactorConfig()));
    riskLikelihoodConfigs = configsManager.getRiskLikelihoodConfigs(requestContext);
    assertTrue(
        riskLikelihoodConfigs
            .getRiskFactorsList()
            .contains(MockFactorConfigsData.getDefaultMotiveFactor()));

    // reset config
    configsManager.updateRiskLikelihoodConfigs(
        requestContext, List.of(riskFactorConfig1, riskFactorConfig2));
    configsManager.resetRiskLikelihoodConfigs(
        requestContext,
        RiskContributorConfigsResetFilter.newBuilder().addRiskFactorIds("custom-tag").build());
    riskLikelihoodConfigs = configsManager.getRiskLikelihoodConfigs(requestContext);
    assertEquals(
        defaultRiskContributorConfigs.getRiskFactorsCount(),
        riskLikelihoodConfigs.getRiskFactorsCount());
    assertTrue(
        riskLikelihoodConfigs
            .getRiskFactorsList()
            .contains(
                MockFactorConfigsData.getDefaultMotiveFactor().toBuilder()
                    .setRiskFactorConfig(updatedRiskFactorConfig1)
                    .setIsDefault(false)
                    .build()));
    assertTrue(
        riskLikelihoodConfigs
            .getRiskFactorsList()
            .contains(MockFactorConfigsData.getDefaultCustomTagRiskFactor()));

    configsManager.resetRiskLikelihoodConfigs(
        requestContext, RiskContributorConfigsResetFilter.getDefaultInstance());
    riskLikelihoodConfigs = configsManager.getRiskLikelihoodConfigs(requestContext);
    assertEquals(
        defaultRiskContributorConfigs.getRiskFactorsCount(),
        riskLikelihoodConfigs.getRiskFactorsCount());
    riskLikelihoodConfigs
        .getRiskFactorsList()
        .forEach(
            factor ->
                assertTrue(defaultRiskContributorConfigs.getRiskFactorsList().contains(factor)));
  }

  @Test
  public void testGetUpdateDeleteImpactConfigs() {
    RequestContext requestContext = RequestContext.forTenantId("impact-tenant");

    // check for get default configs
    RiskContributorConfigs riskImpactConfigs = configsManager.getRiskImpactConfigs(requestContext);
    assertEquals(
        defaultRiskImpactConfigs.getRiskFactorsCount(), riskImpactConfigs.getRiskFactorsCount());
    riskImpactConfigs
        .getRiskFactorsList()
        .forEach(
            factor -> assertTrue(defaultRiskImpactConfigs.getRiskFactorsList().contains(factor)));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            configsManager.updateRiskImpactConfigs(
                requestContext, List.of(RiskFactorConfig.getDefaultInstance())));
    assertThrows(
        StatusRuntimeException.class,
        () ->
            configsManager.updateRiskImpactConfigs(
                requestContext, List.of(RiskFactorConfig.newBuilder().setId("random").build())));
    assertThrows(
        StatusRuntimeException.class,
        () ->
            configsManager.updateRiskImpactConfigs(
                requestContext,
                List.of(
                    RiskFactorConfig.newBuilder()
                        .setId("sensitive-data-exposure")
                        .addRiskElementConfigs(
                            RiskElementConfig.newBuilder().setId("random").build())
                        .build())));
    assertThrows(
        StatusRuntimeException.class,
        () ->
            configsManager.updateRiskImpactConfigs(
                requestContext, List.of(getCustomTagUpdatedConfig(false))));

    // update modified configs
    RiskFactorConfig riskFactorConfig1 =
        RiskFactorConfig.newBuilder()
            .setId("sensitive-data-exposure")
            .addRiskElementConfigs(
                RiskElementConfig.newBuilder()
                    .setId("request-has-5-or-more-params")
                    .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(9)))
            .setRiskFactorScoring(
                RiskFactorScoring.newBuilder()
                    .setDisabled(true)
                    .setScoreContribution(
                        RiskFactorScoreContribution.RISK_FACTOR_SCORE_CONTRIBUTION_ABSOLUTE))
            .build();
    RiskFactorConfig riskFactorConfig2 = getCustomTagUpdatedConfig(true);
    RiskFactorConfig updatedRiskFactorConfig1 = getSensitiveDataExposureFactorConfig(true, 9);

    configsManager.updateRiskLikelihoodConfigs(
        requestContext,
        List.of(
            getDefaultMotiveFactor().getRiskFactorConfig().toBuilder()
                .setRiskFactorScoring(RiskFactorScoring.newBuilder().setDisabled(true))
                .build()));
    configsManager.updateRiskImpactConfigs(
        requestContext, List.of(riskFactorConfig1, riskFactorConfig2));
    riskImpactConfigs = configsManager.getRiskImpactConfigs(requestContext);
    assertEquals(
        defaultRiskImpactConfigs.getRiskFactorsCount(), riskImpactConfigs.getRiskFactorsCount());
    assertTrue(
        riskImpactConfigs
            .getRiskFactorsList()
            .contains(
                getDefaultSensitiveDataExposureFactor().toBuilder()
                    .setRiskFactorConfig(updatedRiskFactorConfig1)
                    .setIsDefault(false)
                    .build()));
    assertTrue(
        riskImpactConfigs
            .getRiskFactorsList()
            .contains(
                MockFactorConfigsData.getDefaultCustomTagRiskFactor().toBuilder()
                    .setRiskFactorConfig(riskFactorConfig2)
                    .setIsDefault(false)
                    .build()));

    // update config back to default config
    configsManager.updateRiskImpactConfigs(
        requestContext,
        List.of(
            MockFactorConfigsData.getDefaultSensitiveDataExposureFactor().getRiskFactorConfig()));
    riskImpactConfigs = configsManager.getRiskImpactConfigs(requestContext);
    assertTrue(
        riskImpactConfigs
            .getRiskFactorsList()
            .contains(MockFactorConfigsData.getDefaultSensitiveDataExposureFactor()));

    // reset config
    configsManager.updateRiskImpactConfigs(
        requestContext, List.of(riskFactorConfig1, riskFactorConfig2));
    configsManager.resetRiskImpactConfigs(
        requestContext,
        RiskContributorConfigsResetFilter.newBuilder().addRiskFactorIds("custom-tag").build());
    riskImpactConfigs = configsManager.getRiskImpactConfigs(requestContext);
    assertEquals(
        defaultRiskImpactConfigs.getRiskFactorsCount(), riskImpactConfigs.getRiskFactorsCount());
    assertTrue(
        riskImpactConfigs
            .getRiskFactorsList()
            .contains(
                getDefaultSensitiveDataExposureFactor().toBuilder()
                    .setRiskFactorConfig(updatedRiskFactorConfig1)
                    .setIsDefault(false)
                    .build()));
    assertTrue(
        riskImpactConfigs
            .getRiskFactorsList()
            .contains(MockFactorConfigsData.getDefaultCustomTagRiskFactor()));

    configsManager.resetRiskImpactConfigs(
        requestContext, RiskContributorConfigsResetFilter.getDefaultInstance());
    riskImpactConfigs = configsManager.getRiskImpactConfigs(requestContext);
    assertEquals(
        defaultRiskImpactConfigs.getRiskFactorsCount(), riskImpactConfigs.getRiskFactorsCount());
    riskImpactConfigs
        .getRiskFactorsList()
        .forEach(
            factor -> assertTrue(defaultRiskImpactConfigs.getRiskFactorsList().contains(factor)));
  }

  private RiskFactorConfig getCustomTagUpdatedConfig(boolean withPredicate) {
    return RiskFactorConfig.newBuilder()
        .setId("custom-tag")
        .addRiskElementConfigs(randomElement(withPredicate))
        .setRiskFactorScoring(
            RiskFactorScoring.newBuilder()
                .setDisabled(true)
                .setScoreContribution(
                    RiskFactorScoreContribution.RISK_FACTOR_SCORE_CONTRIBUTION_ABSOLUTE))
        .build();
  }

  private RiskElementConfig randomElement(boolean withPredicate) {
    RiskElementConfig.Builder builder =
        RiskElementConfig.newBuilder()
            .setId("random")
            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(9));
    if (withPredicate) {
      builder.setRiskElementInfo(
          RiskElementInfo.newBuilder()
              .setLabelId(
                  StringPredicate.newBuilder()
                      .setOperator(StringOperator.STRING_OPERATOR_EQUALS)
                      .setValue("random")));
    }
    return builder.build();
  }
}
