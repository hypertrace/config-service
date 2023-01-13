package ai.traceable.risk.config.service.v2.grid;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.risk.config.service.v2.RiskConfigServiceRequestValidator;
import ai.traceable.risk.config.service.v2.RiskScoreCategory;
import ai.traceable.risk.config.service.v2.RiskScoringGridCell;
import ai.traceable.risk.config.service.v2.RiskScoringGridConfigValues;
import ai.traceable.risk.config.service.v2.grid.builder.RiskScoringGridConfigBuilder;
import ai.traceable.risk.config.service.v2.grid.comparator.RiskScoringGridConfigComparator;
import ai.traceable.risk.config.service.v2.grid.comparator.RiskScoringGridConfigComparatorImpl;
import ai.traceable.risk.config.service.v2.grid.validator.RiskScoringGridConfigValidator;
import ai.traceable.risk.config.service.v2.grid.validator.RiskScoringGridConfigValidatorImpl;
import io.grpc.Status;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class RiskScoringGridConfigBuilderTest {

  private final RiskScoringGridConfigBuilder configBuilder = new RiskScoringGridConfigBuilder();
  private final RiskScoringGridConfigValidator validator =
      new RiskScoringGridConfigValidatorImpl(new RiskConfigServiceRequestValidator());
  private final RiskScoringGridConfigComparator comparator =
      new RiskScoringGridConfigComparatorImpl();

  @Test
  public void testMergeConfigs() {
    RiskScoringGridConfigValues lowPriorityConfig = getDefaultConfig();
    Set<RiskScoringGridCell> defaultCellsSet =
        Set.copyOf(lowPriorityConfig.getRiskScoringGridCellsList());
    {
      RiskScoringGridConfigValues highPriorityConfig =
          RiskScoringGridConfigValues.getDefaultInstance();
      RiskScoringGridConfigValues mergedConfig =
          configBuilder.mergeConfigs(highPriorityConfig, lowPriorityConfig);
      assertEquals(4, mergedConfig.getRiskScoringGridCellsCount());
      assertEquals(defaultCellsSet, Set.copyOf(mergedConfig.getRiskScoringGridCellsList()));
    }
    {
      RiskScoringGridConfigValues highPriorityConfig =
          RiskScoringGridConfigValues.newBuilder()
              .addRiskScoringGridCells(
                  RiskScoringGridCell.newBuilder()
                      .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                      .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                      .setScore(2))
              .build();

      RiskScoringGridConfigValues mergedConfig =
          configBuilder.mergeConfigs(highPriorityConfig, lowPriorityConfig);
      assertEquals(4, mergedConfig.getRiskScoringGridCellsCount());
      assertEquals(defaultCellsSet, Set.copyOf(mergedConfig.getRiskScoringGridCellsList()));
    }
    {
      RiskScoringGridConfigValues highPriorityConfig =
          RiskScoringGridConfigValues.newBuilder()
              .addRiskScoringGridCells(
                  RiskScoringGridCell.newBuilder()
                      .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                      .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                      .setScore(1))
              .build();

      RiskScoringGridConfigValues mergedConfig =
          configBuilder.mergeConfigs(highPriorityConfig, lowPriorityConfig);
      assertEquals(4, mergedConfig.getRiskScoringGridCellsCount());
      mergedConfig
          .getRiskScoringGridCellsList()
          .forEach(
              cell -> {
                if (cell.getLikelihoodScoreCategory()
                        .equals(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                    && cell.getImpactScoreCategory()
                        .equals(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)) {
                  assertEquals(1, cell.getScore());
                } else {
                  assertTrue(defaultCellsSet.contains(cell));
                }
              });
    }
    {
      RiskScoringGridConfigValues highPriorityConfig =
          RiskScoringGridConfigValues.newBuilder()
              .addRiskScoringGridCells(
                  RiskScoringGridCell.newBuilder()
                      .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                      .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_MEDIUM)
                      .setScore(3))
              .build();
      RiskScoringGridConfigValues mergedConfig =
          configBuilder.mergeConfigs(highPriorityConfig, lowPriorityConfig);
      assertEquals(5, mergedConfig.getRiskScoringGridCellsCount());
      mergedConfig
          .getRiskScoringGridCellsList()
          .forEach(
              cell -> {
                if (cell.getLikelihoodScoreCategory()
                        .equals(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                    && cell.getImpactScoreCategory()
                        .equals(RiskScoreCategory.RISK_SCORE_CATEGORY_MEDIUM)) {
                  assertEquals(3, cell.getScore());
                } else {
                  assertTrue(defaultCellsSet.contains(cell));
                }
              });
    }
  }

  @Test
  public void testIsConfigDefault() {
    RiskScoringGridConfigValues defaultConfig = getDefaultConfig();
    {
      RiskScoringGridConfigValues specificConfig = RiskScoringGridConfigValues.getDefaultInstance();
      assertTrue(comparator.isGridValuesEqual(specificConfig, defaultConfig));
    }
    {
      RiskScoringGridConfigValues specificConfig =
          RiskScoringGridConfigValues.newBuilder()
              .addRiskScoringGridCells(
                  RiskScoringGridCell.newBuilder()
                      .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                      .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                      .setScore(2))
              .build();
      assertTrue(comparator.isGridValuesEqual(specificConfig, defaultConfig));
    }
    {
      RiskScoringGridConfigValues specificConfig =
          RiskScoringGridConfigValues.newBuilder()
              .addRiskScoringGridCells(
                  RiskScoringGridCell.newBuilder()
                      .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                      .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                      .setScore(3))
              .build();
      assertFalse(comparator.isGridValuesEqual(specificConfig, defaultConfig));
    }
    {
      RiskScoringGridConfigValues specificConfig =
          RiskScoringGridConfigValues.newBuilder()
              .addRiskScoringGridCells(
                  RiskScoringGridCell.newBuilder()
                      .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                      .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_MEDIUM)
                      .setScore(3))
              .build();
      assertFalse(comparator.isGridValuesEqual(specificConfig, defaultConfig));
    }
  }

  @Test
  public void testValidateConfig() {
    assertEquals(
        Status.OUT_OF_RANGE.getCode(),
        validator
            .validateRiskScoringGridValues(
                RiskScoringGridConfigValues.newBuilder()
                    .addRiskScoringGridCells(
                        RiskScoringGridCell.newBuilder()
                            .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                            .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                            .setScore(-1))
                    .build())
            .getCode());
    assertEquals(
        Status.OUT_OF_RANGE.getCode(),
        validator
            .validateRiskScoringGridValues(
                RiskScoringGridConfigValues.newBuilder()
                    .addRiskScoringGridCells(
                        RiskScoringGridCell.newBuilder()
                            .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                            .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                            .setScore(11))
                    .build())
            .getCode());
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        validator
            .validateRiskScoringGridValues(
                RiskScoringGridConfigValues.newBuilder()
                    .addRiskScoringGridCells(
                        RiskScoringGridCell.newBuilder()
                            .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                            .setScore(2))
                    .build())
            .getCode());
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        validator
            .validateRiskScoringGridValues(
                RiskScoringGridConfigValues.newBuilder()
                    .addRiskScoringGridCells(
                        RiskScoringGridCell.newBuilder()
                            .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                            .setScore(2))
                    .build())
            .getCode());
  }

  private RiskScoringGridConfigValues getDefaultConfig() {
    return RiskScoringGridConfigValues.newBuilder()
        .addRiskScoringGridCells(
            RiskScoringGridCell.newBuilder()
                .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                .setScore(2))
        .addRiskScoringGridCells(
            RiskScoringGridCell.newBuilder()
                .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_MEDIUM)
                .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_MEDIUM)
                .setScore(4))
        .addRiskScoringGridCells(
            RiskScoringGridCell.newBuilder()
                .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_HIGH)
                .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_HIGH)
                .setScore(6))
        .addRiskScoringGridCells(
            RiskScoringGridCell.newBuilder()
                .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                .setScore(8))
        .build();
  }
}
