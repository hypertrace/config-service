package ai.traceable.risk.config.service.factorgrid.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.risk.config.service.factorgrid.RiskFactorGridConfigManager;
import ai.traceable.risk.config.service.processor.RiskConfigConverter;
import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskFactorGridCell;
import ai.traceable.risk.config.service.v1.RiskFactorGridConfig;
import ai.traceable.risk.config.service.v1.RiskFactorGridConfigValues;
import ai.traceable.risk.config.service.v1.RiskScoreCategory;
import io.grpc.StatusRuntimeException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.config.objectstore.DefaultObjectStore;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RiskFactorGridConfigManagerTest {
  private RiskFactorGridConfigValues defaultRiskFactorGridConfigValues;
  private DefaultObjectStore<RiskFactorGridConfigValues> configStore;
  private RiskFactorGridConfigManager riskFactorGridConfigManager;

  @BeforeEach
  public void setup() {
    defaultRiskFactorGridConfigValues = getDefaultConfig();
    configStore = new MockRiskFactorGridConfigStore(null, null, null);
    riskFactorGridConfigManager =
        new RiskFactorGridConfigManagerImpl(
            configStore, new RiskFactorGridConfigUtils(), defaultRiskFactorGridConfigValues);
  }

  @Test
  public void testGetUpdateDeleteRiskFactorGridConfig() {
    RequestContext requestContext = RequestContext.forTenantId("tenant");

    RiskFactorGridConfig defaultRiskFactorGridConfig =
        riskFactorGridConfigManager.getRiskFactorGridConfig(requestContext);
    assertTrue(defaultRiskFactorGridConfig.getIsDefault());
    assertEquals(
        defaultRiskFactorGridConfigValues,
        defaultRiskFactorGridConfig.getRiskFactorGridConfigValues());
    assertEquals(
        defaultRiskFactorGridConfig,
        riskFactorGridConfigManager.resetRiskFactorGridConfig(requestContext));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            riskFactorGridConfigManager.updateRiskFactorGridConfig(
                requestContext,
                RiskFactorGridConfigValues.newBuilder()
                    .addRiskFactorGridCells(
                        RiskFactorGridCell.newBuilder()
                            .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                            .setLikelihoodScoreCategory(
                                RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                            .setScore(11)
                            .build())
                    .build()));

    assertEquals(
        defaultRiskFactorGridConfig,
        riskFactorGridConfigManager.updateRiskFactorGridConfig(
            requestContext, defaultRiskFactorGridConfigValues));
    RiskFactorGridConfigValues values =
        RiskFactorGridConfigValues.newBuilder()
            .addRiskFactorGridCells(
                RiskFactorGridCell.newBuilder()
                    .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_HIGH)
                    .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                    .setScore(8)
                    .build())
            .build();
    assertEquals(
        4,
        riskFactorGridConfigManager
            .updateRiskFactorGridConfig(requestContext, values)
            .getRiskFactorGridConfigValues()
            .getRiskFactorGridCellsCount());
    RiskFactorGridConfig riskFactorGridConfig =
        riskFactorGridConfigManager.getRiskFactorGridConfig(requestContext);
    assertFalse(riskFactorGridConfig.getIsDefault());
    assertEquals(
        4, riskFactorGridConfig.getRiskFactorGridConfigValues().getRiskFactorGridCellsCount());

    assertEquals(
        defaultRiskFactorGridConfig,
        riskFactorGridConfigManager.resetRiskFactorGridConfig(requestContext));
    assertEquals(
        defaultRiskFactorGridConfig,
        riskFactorGridConfigManager.getRiskFactorGridConfig(requestContext));
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

  static class MockRiskFactorGridConfigStore extends RiskFactorGridConfigStore {

    private Map<String, RiskFactorGridConfigValues> values = new HashMap<>();

    protected MockRiskFactorGridConfigStore(
        ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
        RiskConfigConverter<RiskFactorGridConfigValues> configConverter,
        RiskConfigUtils<RiskFactorGridConfigValues> configUtils) {
      super(configServiceBlockingStub, configConverter, configUtils);
    }

    @Override
    public Optional<RiskFactorGridConfigValues> getObject(RequestContext requestContext) {
      return Optional.ofNullable(values.get(requestContext.getTenantId().get()));
    }

    @Override
    public RiskFactorGridConfigValues upsertObject(
        RequestContext requestContext, RiskFactorGridConfigValues config) {
      values.put(requestContext.getTenantId().get(), config);
      return config;
    }

    @Override
    public Optional<RiskFactorGridConfigValues> deleteObject(RequestContext requestContext) {
      return Optional.ofNullable(values.remove(requestContext.getTenantId().get()));
    }
  }
}
