package ai.traceable.risk.config.service.factorgrid.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.risk.config.service.v1.RiskFactorGridCell;
import ai.traceable.risk.config.service.v1.RiskFactorGridConfigValues;
import ai.traceable.risk.config.service.v1.RiskScoreCategory;
import io.grpc.Status;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class RiskFactorGridConfigUtilsTest {

  private final RiskFactorGridConfigUtils configUtils = new RiskFactorGridConfigUtils();

  @Test
  public void testMergeConfigs() {
    RiskFactorGridConfigValues lowPriorityConfig = getDefaultConfig();
    Set<RiskFactorGridCell> defaultCellsSet =
        Set.copyOf(lowPriorityConfig.getRiskFactorGridCellsList());
    {
      RiskFactorGridConfigValues highPriorityConfig =
          RiskFactorGridConfigValues.getDefaultInstance();
      RiskFactorGridConfigValues mergedConfig =
          configUtils.mergeConfigs(highPriorityConfig, lowPriorityConfig);
      assertEquals(3, mergedConfig.getRiskFactorGridCellsCount());
      assertEquals(defaultCellsSet, Set.copyOf(mergedConfig.getRiskFactorGridCellsList()));
    }
    {
      RiskFactorGridConfigValues highPriorityConfig =
          RiskFactorGridConfigValues.newBuilder()
              .addRiskFactorGridCells(
                  RiskFactorGridCell.newBuilder()
                      .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                      .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                      .setScore(9)
                      .build())
              .build();
      RiskFactorGridConfigValues mergedConfig =
          configUtils.mergeConfigs(highPriorityConfig, lowPriorityConfig);
      assertEquals(3, mergedConfig.getRiskFactorGridCellsCount());
      assertEquals(defaultCellsSet, Set.copyOf(mergedConfig.getRiskFactorGridCellsList()));
    }
    {
      RiskFactorGridConfigValues highPriorityConfig =
          RiskFactorGridConfigValues.newBuilder()
              .addRiskFactorGridCells(
                  RiskFactorGridCell.newBuilder()
                      .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                      .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                      .setScore(8)
                      .build())
              .build();
      RiskFactorGridConfigValues mergedConfig =
          configUtils.mergeConfigs(highPriorityConfig, lowPriorityConfig);
      assertEquals(3, mergedConfig.getRiskFactorGridCellsCount());
      highPriorityConfig
          .getRiskFactorGridCellsList()
          .forEach(
              cell -> {
                if (cell.getImpactScoreCategory() == RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL
                    && cell.getLikelihoodScoreCategory()
                        == RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL) {
                  assertEquals(8, cell.getScore());
                } else {
                  assertTrue(defaultCellsSet.contains(cell));
                }
              });
    }
    {
      RiskFactorGridConfigValues highPriorityConfig =
          RiskFactorGridConfigValues.newBuilder()
              .addRiskFactorGridCells(
                  RiskFactorGridCell.newBuilder()
                      .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_HIGH)
                      .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                      .setScore(9)
                      .build())
              .build();
      RiskFactorGridConfigValues mergedConfig =
          configUtils.mergeConfigs(highPriorityConfig, lowPriorityConfig);
      assertEquals(4, mergedConfig.getRiskFactorGridCellsCount());
      highPriorityConfig
          .getRiskFactorGridCellsList()
          .forEach(
              cell -> {
                if (cell.getImpactScoreCategory() == RiskScoreCategory.RISK_SCORE_CATEGORY_HIGH
                    && cell.getLikelihoodScoreCategory()
                        == RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL) {
                  assertEquals(9, cell.getScore());
                } else {
                  assertTrue(defaultCellsSet.contains(cell));
                }
              });
    }
  }

  @Test
  public void testIsConfigDefault() {
    RiskFactorGridConfigValues defaultConfig = getDefaultConfig();
    {
      RiskFactorGridConfigValues specificConfig = RiskFactorGridConfigValues.getDefaultInstance();
      assertTrue(configUtils.isConfigDefault(specificConfig, defaultConfig));
    }
    {
      RiskFactorGridConfigValues specificConfig =
          RiskFactorGridConfigValues.newBuilder()
              .addRiskFactorGridCells(
                  RiskFactorGridCell.newBuilder()
                      .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                      .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                      .setScore(9)
                      .build())
              .build();
      assertTrue(configUtils.isConfigDefault(specificConfig, defaultConfig));
    }
    {
      RiskFactorGridConfigValues specificConfig =
          RiskFactorGridConfigValues.newBuilder()
              .addRiskFactorGridCells(
                  RiskFactorGridCell.newBuilder()
                      .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                      .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                      .setScore(8)
                      .build())
              .build();
      assertFalse(configUtils.isConfigDefault(specificConfig, defaultConfig));
    }
    {
      RiskFactorGridConfigValues specificConfig =
          RiskFactorGridConfigValues.newBuilder()
              .addRiskFactorGridCells(
                  RiskFactorGridCell.newBuilder()
                      .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_HIGH)
                      .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                      .setScore(9)
                      .build())
              .build();
      assertFalse(configUtils.isConfigDefault(specificConfig, defaultConfig));
    }
  }

  @Test
  public void testValidateConfig() {
    assertEquals(
        Status.OUT_OF_RANGE.getCode(),
        configUtils
            .validateConfig(
                RiskFactorGridConfigValues.newBuilder()
                    .addRiskFactorGridCells(
                        RiskFactorGridCell.newBuilder()
                            .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                            .setLikelihoodScoreCategory(
                                RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                            .setScore(-1)
                            .build())
                    .build())
            .getCode());
    assertEquals(
        Status.OUT_OF_RANGE.getCode(),
        configUtils
            .validateConfig(
                RiskFactorGridConfigValues.newBuilder()
                    .addRiskFactorGridCells(
                        RiskFactorGridCell.newBuilder()
                            .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                            .setLikelihoodScoreCategory(
                                RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                            .setScore(11)
                            .build())
                    .build())
            .getCode());
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        configUtils
            .validateConfig(
                RiskFactorGridConfigValues.newBuilder()
                    .addRiskFactorGridCells(
                        RiskFactorGridCell.newBuilder()
                            .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                            .setScore(4)
                            .build())
                    .build())
            .getCode());
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        configUtils
            .validateConfig(
                RiskFactorGridConfigValues.newBuilder()
                    .addRiskFactorGridCells(
                        RiskFactorGridCell.newBuilder()
                            .setLikelihoodScoreCategory(
                                RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                            .setScore(4)
                            .build())
                    .build())
            .getCode());
  }

  private RiskFactorGridConfigValues getDefaultConfig() {
    return RiskFactorGridConfigValues.newBuilder()
        .addRiskFactorGridCells(
            RiskFactorGridCell.newBuilder()
                .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                .setScore(9)
                .build())
        .addRiskFactorGridCells(
            RiskFactorGridCell.newBuilder()
                .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_HIGH)
                .setScore(4)
                .build())
        .addRiskFactorGridCells(
            RiskFactorGridCell.newBuilder()
                .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_HIGH)
                .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                .setScore(5)
                .build())
        .build();
  }
}
