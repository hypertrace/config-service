package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.risk.config.service.v1.GetRiskScoringConfigsRequest;
import ai.traceable.risk.config.service.v1.GetRiskScoringConfigsResponse;
import ai.traceable.risk.config.service.v1.ResetRiskFactorGridConfigRequest;
import ai.traceable.risk.config.service.v1.ResetRiskLevelConfigRequest;
import ai.traceable.risk.config.service.v1.RiskConfigServiceGrpc;
import ai.traceable.risk.config.service.v1.RiskFactorGridCell;
import ai.traceable.risk.config.service.v1.RiskFactorGridConfig;
import ai.traceable.risk.config.service.v1.RiskFactorGridConfigValues;
import ai.traceable.risk.config.service.v1.RiskLevelConfig;
import ai.traceable.risk.config.service.v1.RiskLevelConfigValues;
import ai.traceable.risk.config.service.v1.RiskScoreCategory;
import ai.traceable.risk.config.service.v1.UpdateRiskFactorGridConfigRequest;
import ai.traceable.risk.config.service.v1.UpdateRiskLevelConfigRequest;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class RiskConfigServiceIntegrationTest extends TraceableConfigServiceIntegrationTestBase {
  private static RiskConfigServiceGrpc.RiskConfigServiceBlockingStub riskConfigServiceStub;

  @BeforeAll
  static void init() {
    riskConfigServiceStub =
        RiskConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  public void testGetRiskScoringConfigs() {
    GetRiskScoringConfigsResponse riskScoringConfigsResponse =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                riskConfigServiceStub.getRiskScoringConfigs(
                    GetRiskScoringConfigsRequest.getDefaultInstance()));

    RiskLevelConfig riskLevelConfig = riskScoringConfigsResponse.getRiskLevelConfig();
    assertTrue(riskLevelConfig.getIsDefault());
    assertEquals(2, riskLevelConfig.getRiskLevelConfigValues().getMediumLevelMinScore());
    assertEquals(5, riskLevelConfig.getRiskLevelConfigValues().getHighLevelMinScore());
    assertEquals(9, riskLevelConfig.getRiskLevelConfigValues().getCriticalLevelMinScore());
    RiskFactorGridConfig riskFactorGridConfig =
        riskScoringConfigsResponse.getRiskFactorGridConfig();
    assertTrue(riskFactorGridConfig.getIsDefault());
    assertEquals(
        16, riskFactorGridConfig.getRiskFactorGridConfigValues().getRiskFactorGridCellsCount());
    assertRiskFactorGrid(riskFactorGridConfig.getRiskFactorGridConfigValues(), 3);
  }

  @Test
  public void testUpdateResetRiskLevelConfig() {
    RiskLevelConfigValues invalidRiskLevelConfigValues =
        RiskLevelConfigValues.newBuilder()
            .setMediumLevelMinScore(6)
            .setHighLevelMinScore(3)
            .setCriticalLevelMinScore(8)
            .build();
    assertThrows(
        Exception.class,
        () ->
            GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.updateRiskLevelConfig(
                        UpdateRiskLevelConfigRequest.newBuilder()
                            .setRiskLevelConfigValues(invalidRiskLevelConfigValues)
                            .build())));

    RiskLevelConfigValues riskLevelConfigValues =
        RiskLevelConfigValues.newBuilder()
            .setMediumLevelMinScore(3)
            .setHighLevelMinScore(6)
            .setCriticalLevelMinScore(8)
            .build();
    assertEquals(
        riskLevelConfigValues,
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.updateRiskLevelConfig(
                        UpdateRiskLevelConfigRequest.newBuilder()
                            .setRiskLevelConfigValues(riskLevelConfigValues)
                            .build()))
            .getRiskLevelConfig()
            .getRiskLevelConfigValues());
    GetRiskScoringConfigsResponse riskScoringConfigsResponse =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                riskConfigServiceStub.getRiskScoringConfigs(
                    GetRiskScoringConfigsRequest.getDefaultInstance()));

    RiskLevelConfig riskLevelConfig = riskScoringConfigsResponse.getRiskLevelConfig();
    assertFalse(riskLevelConfig.getIsDefault());
    assertEquals(3, riskLevelConfig.getRiskLevelConfigValues().getMediumLevelMinScore());
    assertEquals(6, riskLevelConfig.getRiskLevelConfigValues().getHighLevelMinScore());
    assertEquals(8, riskLevelConfig.getRiskLevelConfigValues().getCriticalLevelMinScore());

    assertTrue(
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.resetRiskLevelConfig(
                        ResetRiskLevelConfigRequest.getDefaultInstance()))
            .getRiskLevelConfig()
            .getIsDefault());

    riskScoringConfigsResponse =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                riskConfigServiceStub.getRiskScoringConfigs(
                    GetRiskScoringConfigsRequest.getDefaultInstance()));
    riskLevelConfig = riskScoringConfigsResponse.getRiskLevelConfig();
    assertTrue(riskLevelConfig.getIsDefault());
    assertEquals(2, riskLevelConfig.getRiskLevelConfigValues().getMediumLevelMinScore());
    assertEquals(5, riskLevelConfig.getRiskLevelConfigValues().getHighLevelMinScore());
    assertEquals(9, riskLevelConfig.getRiskLevelConfigValues().getCriticalLevelMinScore());
  }

  @Test
  public void testUpdateResetRiskFactorGridConfig() {
    RiskFactorGridConfigValues invalidRiskFactorGridConfigValues =
        RiskFactorGridConfigValues.newBuilder()
            .addRiskFactorGridCells(
                RiskFactorGridCell.newBuilder()
                    .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                    .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_CRITICAL)
                    .setScore(11)
                    .build())
            .build();
    assertThrows(
        Exception.class,
        () ->
            GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.updateRiskFactorGridConfig(
                        UpdateRiskFactorGridConfigRequest.newBuilder()
                            .setRiskFactorGridConfigValues(invalidRiskFactorGridConfigValues)
                            .build())));

    RiskFactorGridConfigValues riskFactorGridConfigValues =
        RiskFactorGridConfigValues.newBuilder()
            .addRiskFactorGridCells(
                RiskFactorGridCell.newBuilder()
                    .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                    .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_MEDIUM)
                    .setScore(4)
                    .build())
            .build();
    RiskFactorGridConfigValues updatedRiskFactorGridConfigValues =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.updateRiskFactorGridConfig(
                        UpdateRiskFactorGridConfigRequest.newBuilder()
                            .setRiskFactorGridConfigValues(riskFactorGridConfigValues)
                            .build()))
            .getRiskFactorGridConfig()
            .getRiskFactorGridConfigValues();
    assertEquals(16, updatedRiskFactorGridConfigValues.getRiskFactorGridCellsCount());
    assertRiskFactorGrid(updatedRiskFactorGridConfigValues, 4);

    GetRiskScoringConfigsResponse riskScoringConfigsResponse =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                riskConfigServiceStub.getRiskScoringConfigs(
                    GetRiskScoringConfigsRequest.getDefaultInstance()));

    RiskFactorGridConfig riskFactorGridConfig =
        riskScoringConfigsResponse.getRiskFactorGridConfig();
    assertFalse(riskFactorGridConfig.getIsDefault());
    assertRiskFactorGrid(riskFactorGridConfig.getRiskFactorGridConfigValues(), 4);

    assertTrue(
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.resetRiskFactorGridConfig(
                        ResetRiskFactorGridConfigRequest.getDefaultInstance()))
            .getRiskFactorGridConfig()
            .getIsDefault());

    riskScoringConfigsResponse =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                riskConfigServiceStub.getRiskScoringConfigs(
                    GetRiskScoringConfigsRequest.getDefaultInstance()));
    riskFactorGridConfig = riskScoringConfigsResponse.getRiskFactorGridConfig();
    assertTrue(riskFactorGridConfig.getIsDefault());
    assertRiskFactorGrid(riskFactorGridConfig.getRiskFactorGridConfigValues(), 3);
  }

  private void assertRiskFactorGrid(
      RiskFactorGridConfigValues riskFactorGridConfigValues, int score) {
    riskFactorGridConfigValues
        .getRiskFactorGridCellsList()
        .forEach(
            cell -> {
              if (cell.getLikelihoodScoreCategory() == RiskScoreCategory.RISK_SCORE_CATEGORY_LOW
                  && cell.getImpactScoreCategory()
                      == RiskScoreCategory.RISK_SCORE_CATEGORY_MEDIUM) {
                assertEquals(score, cell.getScore());
              }
            });
  }
}
