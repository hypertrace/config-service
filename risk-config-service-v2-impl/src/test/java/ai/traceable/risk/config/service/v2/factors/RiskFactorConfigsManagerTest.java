package ai.traceable.risk.config.service.v2.factors;

import static ai.traceable.risk.config.service.v2.StringOperator.STRING_OPERATOR_EQUALS;
import static ai.traceable.risk.config.service.v2.factors.MockFactorConfigsData.buildRiskFactorWithoutScope;
import static ai.traceable.risk.config.service.v2.factors.MockFactorConfigsData.getDefaultRiskContributorConfigs;
import static ai.traceable.risk.config.service.v2.scope.MockScopeData.getEnvironmentBasedScope;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.risk.config.service.v2.RiskConfigIdGenerator;
import ai.traceable.risk.config.service.v2.RiskConfigScope;
import ai.traceable.risk.config.service.v2.RiskContributorConfigs;
import ai.traceable.risk.config.service.v2.RiskElementConfigUpdateDetails;
import ai.traceable.risk.config.service.v2.RiskElementConfigUpdates;
import ai.traceable.risk.config.service.v2.RiskElementPredicate;
import ai.traceable.risk.config.service.v2.RiskElementScoring;
import ai.traceable.risk.config.service.v2.RiskFactorCategory;
import ai.traceable.risk.config.service.v2.RiskFactorConfig;
import ai.traceable.risk.config.service.v2.RiskFactorConfigUpdateDetails;
import ai.traceable.risk.config.service.v2.StringPredicate;
import ai.traceable.risk.config.service.v2.elements.builder.LabelElementConfigsBuilder;
import ai.traceable.risk.config.service.v2.elements.builder.LabelPredicateBuilder;
import ai.traceable.risk.config.service.v2.elements.builder.LabelsConfigProvider;
import ai.traceable.risk.config.service.v2.elements.builder.RiskElementConfigBuilder;
import ai.traceable.risk.config.service.v2.elements.normalizer.LabelIdNormalizer;
import ai.traceable.risk.config.service.v2.factors.builder.RiskFactorConfigBuilder;
import ai.traceable.risk.config.service.v2.factors.builder.RiskFactorListBuilder;
import ai.traceable.risk.config.service.v2.factors.comparator.RiskFactorConfigsComparator;
import ai.traceable.risk.config.service.v2.factors.comparator.RiskFactorConfigsComparatorImpl;
import ai.traceable.risk.config.service.v2.factors.manager.RiskFactorConfigsManager;
import ai.traceable.risk.config.service.v2.factors.manager.RiskFactorConfigsManagerImpl;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.label.config.service.v1.Label;
import org.hypertrace.label.config.service.v1.LabelData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RiskFactorConfigsManagerTest {
  private RiskContributorConfigs defaultRiskContributorConfigs;
  private RiskFactorConfigsManager configsManager;

  private final RiskFactorConfigsComparator factorConfigsComparator =
      new RiskFactorConfigsComparatorImpl();
  private final RiskConfigIdGenerator configIdGenerator =
      new RiskConfigIdGenerator(new UuidGenerator());
  private RiskConfigScope environmentRiskConfigScope;
  private IdentifiedObjectStore<RiskFactorConfig> factorConfigStore;

  @BeforeEach
  public void setup() {
    defaultRiskContributorConfigs = getDefaultRiskContributorConfigs();
    RiskConfigIdGenerator configIdGenerator = new RiskConfigIdGenerator(new UuidGenerator());
    LabelsConfigProvider labelsConfigProvider = mock(LabelsConfigProvider.class);
    when(labelsConfigProvider.getLabels(any())).thenReturn(mockLabels());
    RiskFactorConfigBuilder riskFactorConfigBuilder =
        new RiskFactorConfigBuilder(
            new RiskElementConfigBuilder(),
            new LabelElementConfigsBuilder(
                new LabelPredicateBuilder(labelsConfigProvider), new LabelIdNormalizer()));
    factorConfigStore =
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

  private List<Label> mockLabels() {
    return List.of(
        Label.newBuilder()
            .setId("newLabel")
            .setData(LabelData.newBuilder().setKey("newLabel"))
            .build(),
        Label.newBuilder().setId("Sentry").setData(LabelData.newBuilder().setKey("Sentry")).build(),
        Label.newBuilder()
            .setId("Critical")
            .setData(LabelData.newBuilder().setKey("Critical"))
            .build(),
        Label.newBuilder()
            .setId("Sensitive")
            .setData(LabelData.newBuilder().setKey("Sensitive"))
            .build());
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
              riskFactor
                  .getRiskFactorConfig()
                  .getRiskElementConfigsList()
                  .forEach(riskElementConfig -> assertFalse(riskElementConfig.getDisabled()));
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
              riskFactor
                  .getRiskFactorConfig()
                  .getRiskElementConfigsList()
                  .forEach(riskElementConfig -> assertFalse(riskElementConfig.getDisabled()));
            });

    RiskFactorConfigUpdateDetails updateDetails =
        RiskFactorConfigUpdateDetails.newBuilder()
            .setRiskFactorCategory(RiskFactorCategory.RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE)
            .setRiskElementConfigUpdates(
                RiskElementConfigUpdates.newBuilder()
                    .addRiskElementConfigUpdateDetails(
                        RiskElementConfigUpdateDetails.newBuilder()
                            .setId("responseSensitivityCritical")
                            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(1))))
            .build();
    fetchedRiskContributorConfigs =
        configsManager.updateRiskContributorConfigs(
            requestContext, List.of(updateDetails), environmentRiskConfigScope);
    fetchedRiskContributorConfigs
        .getRiskFactorsList()
        .forEach(
            riskFactor -> {
              if (riskFactor
                  .getRiskFactorConfig()
                  .getRiskFactorCategory()
                  .equals(RiskFactorCategory.RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE)) {
                assertFalse(riskFactor.getRiskFactorConfig().getDisabled());
                riskFactor
                    .getRiskFactorConfig()
                    .getRiskElementConfigsList()
                    .forEach(
                        riskElementConfig -> {
                          if (riskElementConfig.getId().equals("responseSensitivityCritical")) {
                            assertFalse(riskElementConfig.getDisabled());
                            assertTrue(riskElementConfig.hasRiskElementPredicate());
                            assertEquals(1, riskElementConfig.getRiskElementScoring().getScore());
                          }
                        });
              }
            });
    String id =
        configIdGenerator.generateId(
            RiskFactorCategory.RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE.name(),
            environmentRiskConfigScope);
    Optional<RiskFactorConfig> factorConfigFromStore =
        factorConfigStore.getData(requestContext, id);
    assertTrue(factorConfigFromStore.isPresent());
    factorConfigFromStore
        .get()
        .getRiskElementConfigsList()
        .forEach(
            riskElementConfig -> {
              if (riskElementConfig.getId().equals("responseSensitivityCritical")) {
                assertFalse(riskElementConfig.getDisabled());
                assertFalse(riskElementConfig.hasRiskElementPredicate());
                assertEquals(1, riskElementConfig.getRiskElementScoring().getScore());
              }
            });

    RiskFactorConfigUpdateDetails updateDetailsForLabelWithDisabledElement =
        RiskFactorConfigUpdateDetails.newBuilder()
            .setRiskFactorCategory(RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS)
            .setRiskElementConfigUpdates(
                RiskElementConfigUpdates.newBuilder()
                    .addRiskElementConfigUpdateDetails(
                        RiskElementConfigUpdateDetails.newBuilder()
                            .setId("critical")
                            .setDisabled(true)
                            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(1))))
            .build();
    fetchedRiskContributorConfigs =
        configsManager.updateRiskContributorConfigs(
            requestContext,
            List.of(updateDetailsForLabelWithDisabledElement),
            environmentRiskConfigScope);
    fetchedRiskContributorConfigs
        .getRiskFactorsList()
        .forEach(
            riskFactor -> {
              if (riskFactor
                  .getRiskFactorConfig()
                  .getRiskFactorCategory()
                  .equals(RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS)) {
                assertFalse(riskFactor.getRiskFactorConfig().getDisabled());
                riskFactor
                    .getRiskFactorConfig()
                    .getRiskElementConfigsList()
                    .forEach(
                        riskElementConfig -> {
                          if (riskElementConfig.getId().equals("critical")) {
                            assertTrue(riskElementConfig.getDisabled());
                            assertTrue(riskElementConfig.hasRiskElementPredicate());
                            assertEquals(1, riskElementConfig.getRiskElementScoring().getScore());
                          }
                        });
              }
            });
    id =
        configIdGenerator.generateId(
            RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS.name(), environmentRiskConfigScope);
    factorConfigFromStore = factorConfigStore.getData(requestContext, id);
    assertTrue(factorConfigFromStore.isPresent());
    factorConfigFromStore
        .get()
        .getRiskElementConfigsList()
        .forEach(
            riskElementConfig -> {
              if (riskElementConfig.getId().equals("critical")) {
                assertTrue(riskElementConfig.getDisabled());
                assertFalse(riskElementConfig.hasRiskElementPredicate());
                assertEquals(1, riskElementConfig.getRiskElementScoring().getScore());
              }
            });

    RiskFactorConfigUpdateDetails updateDetailsForLabelWithEnabledElement =
        RiskFactorConfigUpdateDetails.newBuilder()
            .setRiskFactorCategory(RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS)
            .setRiskElementConfigUpdates(
                RiskElementConfigUpdates.newBuilder()
                    .addRiskElementConfigUpdateDetails(
                        RiskElementConfigUpdateDetails.newBuilder()
                            .setId("critical")
                            .setDisabled(false)
                            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(1))))
            .build();
    fetchedRiskContributorConfigs =
        configsManager.updateRiskContributorConfigs(
            requestContext,
            List.of(updateDetailsForLabelWithEnabledElement),
            environmentRiskConfigScope);
    fetchedRiskContributorConfigs
        .getRiskFactorsList()
        .forEach(
            riskFactor -> {
              if (riskFactor
                  .getRiskFactorConfig()
                  .getRiskFactorCategory()
                  .equals(RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS)) {
                assertFalse(riskFactor.getRiskFactorConfig().getDisabled());
                riskFactor
                    .getRiskFactorConfig()
                    .getRiskElementConfigsList()
                    .forEach(
                        riskElementConfig -> {
                          if (riskElementConfig.getId().equals("critical")) {
                            assertFalse(riskElementConfig.getDisabled());
                            assertTrue(riskElementConfig.hasRiskElementPredicate());
                            assertEquals(1, riskElementConfig.getRiskElementScoring().getScore());
                          }
                        });
              }
            });
    id =
        configIdGenerator.generateId(
            RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS.name(), environmentRiskConfigScope);
    factorConfigFromStore = factorConfigStore.getData(requestContext, id);
    assertTrue(factorConfigFromStore.isPresent());
    factorConfigFromStore
        .get()
        .getRiskElementConfigsList()
        .forEach(
            riskElementConfig -> {
              if (riskElementConfig.getId().equals("critical")) {
                assertFalse(riskElementConfig.getDisabled());
                assertFalse(riskElementConfig.hasRiskElementPredicate());
                assertEquals(1, riskElementConfig.getRiskElementScoring().getScore());
              }
            });

    RiskFactorConfigUpdateDetails updateDetailsForLabelWithNewElementEnabled =
        RiskFactorConfigUpdateDetails.newBuilder()
            .setRiskFactorCategory(RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS)
            .setRiskElementConfigUpdates(
                RiskElementConfigUpdates.newBuilder()
                    .addRiskElementConfigUpdateDetails(
                        RiskElementConfigUpdateDetails.newBuilder()
                            .setId("newLabel")
                            .setDisabled(false)
                            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(1))))
            .build();
    fetchedRiskContributorConfigs =
        configsManager.updateRiskContributorConfigs(
            requestContext,
            List.of(updateDetailsForLabelWithNewElementEnabled),
            environmentRiskConfigScope);
    fetchedRiskContributorConfigs
        .getRiskFactorsList()
        .forEach(
            riskFactor -> {
              if (riskFactor
                  .getRiskFactorConfig()
                  .getRiskFactorCategory()
                  .equals(RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS)) {
                assertFalse(riskFactor.getRiskFactorConfig().getDisabled());
                riskFactor
                    .getRiskFactorConfig()
                    .getRiskElementConfigsList()
                    .forEach(
                        riskElementConfig -> {
                          if (riskElementConfig.getId().equals("newLabel")) {
                            assertFalse(riskElementConfig.getDisabled());
                            assertEquals(
                                RiskElementPredicate.newBuilder()
                                    .setLabelId(
                                        StringPredicate.newBuilder()
                                            .setOperator(STRING_OPERATOR_EQUALS)
                                            .setValue("newLabel"))
                                    .build(),
                                riskElementConfig.getRiskElementPredicate());
                            assertEquals(1, riskElementConfig.getRiskElementScoring().getScore());
                          }
                        });
              }
            });
    id =
        configIdGenerator.generateId(
            RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS.name(), environmentRiskConfigScope);
    factorConfigFromStore = factorConfigStore.getData(requestContext, id);
    assertTrue(factorConfigFromStore.isPresent());
    factorConfigFromStore
        .get()
        .getRiskElementConfigsList()
        .forEach(
            riskElementConfig -> {
              if (riskElementConfig.getId().equals("newLabel")) {
                assertFalse(riskElementConfig.getDisabled());
                assertTrue(riskElementConfig.hasRiskElementPredicate());
                assertEquals(1, riskElementConfig.getRiskElementScoring().getScore());
              }
            });

    RiskFactorConfigUpdateDetails updateDetailsForLabelWithNewElementDisabled =
        RiskFactorConfigUpdateDetails.newBuilder()
            .setRiskFactorCategory(RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS)
            .setRiskElementConfigUpdates(
                RiskElementConfigUpdates.newBuilder()
                    .addRiskElementConfigUpdateDetails(
                        RiskElementConfigUpdateDetails.newBuilder()
                            .setId("newLabel")
                            .setDisabled(true)
                            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(1))))
            .build();
    fetchedRiskContributorConfigs =
        configsManager.updateRiskContributorConfigs(
            requestContext,
            List.of(updateDetailsForLabelWithNewElementDisabled),
            environmentRiskConfigScope);
    fetchedRiskContributorConfigs
        .getRiskFactorsList()
        .forEach(
            riskFactor -> {
              if (riskFactor
                  .getRiskFactorConfig()
                  .getRiskFactorCategory()
                  .equals(RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS)) {
                assertFalse(riskFactor.getRiskFactorConfig().getDisabled());
                riskFactor
                    .getRiskFactorConfig()
                    .getRiskElementConfigsList()
                    .forEach(
                        riskElementConfig -> {
                          if (riskElementConfig.getId().equals("newLabel")) {
                            assertTrue(riskElementConfig.getDisabled());
                            assertEquals(
                                RiskElementPredicate.newBuilder()
                                    .setLabelId(
                                        StringPredicate.newBuilder()
                                            .setOperator(STRING_OPERATOR_EQUALS)
                                            .setValue("newLabel"))
                                    .build(),
                                riskElementConfig.getRiskElementPredicate());
                            assertEquals(1, riskElementConfig.getRiskElementScoring().getScore());
                          }
                        });
              }
            });
    id =
        configIdGenerator.generateId(
            RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS.name(), environmentRiskConfigScope);
    factorConfigFromStore = factorConfigStore.getData(requestContext, id);
    assertTrue(factorConfigFromStore.isPresent());
    factorConfigFromStore
        .get()
        .getRiskElementConfigsList()
        .forEach(
            riskElementConfig -> {
              if (riskElementConfig.getId().equals("newLabel")) {
                assertTrue(riskElementConfig.getDisabled());
                assertTrue(riskElementConfig.hasRiskElementPredicate());
                assertEquals(1, riskElementConfig.getRiskElementScoring().getScore());
              }
            });

    RiskFactorConfigUpdateDetails updateDetailsForLabelWithNewElementNotInLabelStore =
        RiskFactorConfigUpdateDetails.newBuilder()
            .setRiskFactorCategory(RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS)
            .setRiskElementConfigUpdates(
                RiskElementConfigUpdates.newBuilder()
                    .addRiskElementConfigUpdateDetails(
                        RiskElementConfigUpdateDetails.newBuilder()
                            .setId("newLabel1")
                            .setDisabled(false)
                            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(1))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            configsManager.updateRiskContributorConfigs(
                requestContext,
                List.of(updateDetailsForLabelWithNewElementNotInLabelStore),
                environmentRiskConfigScope));
  }
}
