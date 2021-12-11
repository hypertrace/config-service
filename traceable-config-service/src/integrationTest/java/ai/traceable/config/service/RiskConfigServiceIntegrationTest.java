package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.risk.config.service.v1.CustomizationOptions;
import ai.traceable.risk.config.service.v1.GetRiskImpactConfigsRequest;
import ai.traceable.risk.config.service.v1.GetRiskLikelihoodConfigsRequest;
import ai.traceable.risk.config.service.v1.GetRiskScoringConfigsRequest;
import ai.traceable.risk.config.service.v1.GetRiskScoringConfigsResponse;
import ai.traceable.risk.config.service.v1.IntOperator;
import ai.traceable.risk.config.service.v1.IntPredicate;
import ai.traceable.risk.config.service.v1.ResetRiskFactorGridConfigRequest;
import ai.traceable.risk.config.service.v1.ResetRiskImpactConfigsRequest;
import ai.traceable.risk.config.service.v1.ResetRiskLevelConfigRequest;
import ai.traceable.risk.config.service.v1.ResetRiskLikelihoodConfigsRequest;
import ai.traceable.risk.config.service.v1.RiskConfigServiceGrpc;
import ai.traceable.risk.config.service.v1.RiskContributorConfigs;
import ai.traceable.risk.config.service.v1.RiskContributorConfigsResetFilter;
import ai.traceable.risk.config.service.v1.RiskElementConfig;
import ai.traceable.risk.config.service.v1.RiskElementInfo;
import ai.traceable.risk.config.service.v1.RiskElementScoring;
import ai.traceable.risk.config.service.v1.RiskFactor;
import ai.traceable.risk.config.service.v1.RiskFactorConfig;
import ai.traceable.risk.config.service.v1.RiskFactorGridCell;
import ai.traceable.risk.config.service.v1.RiskFactorGridConfig;
import ai.traceable.risk.config.service.v1.RiskFactorGridConfigValues;
import ai.traceable.risk.config.service.v1.RiskFactorInfo;
import ai.traceable.risk.config.service.v1.RiskFactorScoreContribution;
import ai.traceable.risk.config.service.v1.RiskFactorScoring;
import ai.traceable.risk.config.service.v1.RiskFactorType;
import ai.traceable.risk.config.service.v1.RiskLevelConfig;
import ai.traceable.risk.config.service.v1.RiskLevelConfigValues;
import ai.traceable.risk.config.service.v1.RiskScoreCategory;
import ai.traceable.risk.config.service.v1.StringOperator;
import ai.traceable.risk.config.service.v1.StringPredicate;
import ai.traceable.risk.config.service.v1.UpdateRiskFactorGridConfigRequest;
import ai.traceable.risk.config.service.v1.UpdateRiskImpactConfigsRequest;
import ai.traceable.risk.config.service.v1.UpdateRiskLevelConfigRequest;
import ai.traceable.risk.config.service.v1.UpdateRiskLikelihoodConfigsRequest;
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

  @Test
  public void testGetUpsertResetRiskLikelihoodConfigs() {
    RiskContributorConfigs defaultRiskLikelihoodConfigs =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.getRiskLikelihoodConfigs(
                        GetRiskLikelihoodConfigsRequest.getDefaultInstance()))
            .getRiskLikelihoodConfigs();
    assertTrue(
        defaultRiskLikelihoodConfigs
            .getRiskFactorsList()
            .contains(getDefaultCustomTagRiskFactor()));
    assertTrue(
        defaultRiskLikelihoodConfigs.getRiskFactorsList().contains(getDefaultMotiveFactor()));
    RiskContributorConfigs riskLikelihoodConfigs;

    assertThrows(
        Exception.class,
        () ->
            GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.updateRiskLikelihoodConfigs(
                        UpdateRiskLikelihoodConfigsRequest.newBuilder()
                            .addRiskFactorConfigs(RiskFactorConfig.getDefaultInstance())
                            .build())));
    assertThrows(
        Exception.class,
        () ->
            GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.updateRiskLikelihoodConfigs(
                        UpdateRiskLikelihoodConfigsRequest.newBuilder()
                            .addRiskFactorConfigs(
                                RiskFactorConfig.newBuilder().setId("random").build())
                            .build())));
    assertThrows(
        Exception.class,
        () ->
            GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.updateRiskLikelihoodConfigs(
                        UpdateRiskLikelihoodConfigsRequest.newBuilder()
                            .addRiskFactorConfigs(
                                RiskFactorConfig.newBuilder()
                                    .setId("motive")
                                    .addRiskElementConfigs(
                                        RiskElementConfig.newBuilder().setId("random").build())
                                    .build())
                            .build())));
    assertThrows(
        Exception.class,
        () ->
            GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.updateRiskLikelihoodConfigs(
                        UpdateRiskLikelihoodConfigsRequest.newBuilder()
                            .addRiskFactorConfigs(getCustomTagUpdatedConfig(false))
                            .build())));

    RiskFactorConfig riskFactorConfig1 =
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
            .setRiskFactorScoring(
                RiskFactorScoring.newBuilder()
                    .setDisabled(true)
                    .setScoreContribution(
                        RiskFactorScoreContribution.RISK_FACTOR_SCORE_CONTRIBUTION_ABSOLUTE))
            .build();
    RiskFactorConfig riskFactorConfig2 = getCustomTagUpdatedConfig(true);
    RiskFactorConfig updatedRiskFactorConfig1 =
        riskFactorConfig1.toBuilder()
            .setRiskFactorScoring(RiskFactorScoring.newBuilder().setDisabled(true))
            .build();

    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            riskConfigServiceStub.updateRiskLikelihoodConfigs(
                UpdateRiskLikelihoodConfigsRequest.newBuilder()
                    .addRiskFactorConfigs(riskFactorConfig1)
                    .addRiskFactorConfigs(riskFactorConfig2)
                    .build()));
    riskLikelihoodConfigs =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.getRiskLikelihoodConfigs(
                        GetRiskLikelihoodConfigsRequest.getDefaultInstance()))
            .getRiskLikelihoodConfigs();
    assertEquals(
        defaultRiskLikelihoodConfigs.getRiskFactorsCount(),
        riskLikelihoodConfigs.getRiskFactorsCount());
    assertTrue(
        riskLikelihoodConfigs
            .getRiskFactorsList()
            .contains(
                getDefaultMotiveFactor().toBuilder()
                    .setRiskFactorConfig(updatedRiskFactorConfig1)
                    .setIsDefault(false)
                    .build()));
    assertTrue(
        riskLikelihoodConfigs
            .getRiskFactorsList()
            .contains(
                getDefaultCustomTagRiskFactor().toBuilder()
                    .setRiskFactorConfig(riskFactorConfig2)
                    .setIsDefault(false)
                    .build()));

    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            riskConfigServiceStub.resetRiskLikelihoodConfigs(
                ResetRiskLikelihoodConfigsRequest.newBuilder()
                    .setFilter(
                        RiskContributorConfigsResetFilter.newBuilder()
                            .addRiskFactorIds("custom-tag"))
                    .build()));
    riskLikelihoodConfigs =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.getRiskLikelihoodConfigs(
                        GetRiskLikelihoodConfigsRequest.getDefaultInstance()))
            .getRiskLikelihoodConfigs();
    assertEquals(
        defaultRiskLikelihoodConfigs.getRiskFactorsCount(),
        riskLikelihoodConfigs.getRiskFactorsCount());
    assertTrue(
        riskLikelihoodConfigs
            .getRiskFactorsList()
            .contains(
                getDefaultMotiveFactor().toBuilder()
                    .setRiskFactorConfig(updatedRiskFactorConfig1)
                    .setIsDefault(false)
                    .build()));
    assertTrue(
        riskLikelihoodConfigs.getRiskFactorsList().contains(getDefaultCustomTagRiskFactor()));

    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            riskConfigServiceStub.resetRiskLikelihoodConfigs(
                ResetRiskLikelihoodConfigsRequest.getDefaultInstance()));
    riskLikelihoodConfigs =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.getRiskLikelihoodConfigs(
                        GetRiskLikelihoodConfigsRequest.getDefaultInstance()))
            .getRiskLikelihoodConfigs();
    assertEquals(
        defaultRiskLikelihoodConfigs.getRiskFactorsCount(),
        riskLikelihoodConfigs.getRiskFactorsCount());
    riskLikelihoodConfigs
        .getRiskFactorsList()
        .forEach(
            factor ->
                assertTrue(defaultRiskLikelihoodConfigs.getRiskFactorsList().contains(factor)));
  }

  @Test
  public void testGetUpsertResetRiskImpactConfigs() {
    RiskContributorConfigs defaultRiskImpactConfigs =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.getRiskImpactConfigs(
                        GetRiskImpactConfigsRequest.getDefaultInstance()))
            .getRiskImpactConfigs();
    assertTrue(
        defaultRiskImpactConfigs.getRiskFactorsList().contains(getDefaultCustomTagRiskFactor()));
    assertTrue(
        defaultRiskImpactConfigs
            .getRiskFactorsList()
            .contains(getDefaultSensitiveDataExposureFactor()));
    RiskContributorConfigs riskImpactConfigs;

    assertThrows(
        Exception.class,
        () ->
            GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.updateRiskImpactConfigs(
                        UpdateRiskImpactConfigsRequest.newBuilder()
                            .addRiskFactorConfigs(RiskFactorConfig.getDefaultInstance())
                            .build())));
    assertThrows(
        Exception.class,
        () ->
            GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.updateRiskImpactConfigs(
                        UpdateRiskImpactConfigsRequest.newBuilder()
                            .addRiskFactorConfigs(
                                RiskFactorConfig.newBuilder().setId("random").build())
                            .build())));
    assertThrows(
        Exception.class,
        () ->
            GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.updateRiskImpactConfigs(
                        UpdateRiskImpactConfigsRequest.newBuilder()
                            .addRiskFactorConfigs(
                                RiskFactorConfig.newBuilder()
                                    .setId("sensitiveDataExposure")
                                    .addRiskElementConfigs(
                                        RiskElementConfig.newBuilder().setId("random").build())
                                    .build())
                            .build())));
    assertThrows(
        Exception.class,
        () ->
            GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.updateRiskImpactConfigs(
                        UpdateRiskImpactConfigsRequest.newBuilder()
                            .addRiskFactorConfigs(getCustomTagUpdatedConfig(false))
                            .build())));

    RiskFactorConfig riskFactorConfig1 =
        RiskFactorConfig.newBuilder()
            .setId("sensitive-data-exposure")
            .addRiskElementConfigs(
                RiskElementConfig.newBuilder()
                    .setId("request-has-5-or-more-params")
                    .setRiskElementInfo(
                        RiskElementInfo.newBuilder()
                            .setRequestParamsCount(
                                IntPredicate.newBuilder()
                                    .setOperator(IntOperator.INT_OPERATOR_GREATER_THAN)
                                    .setValue(5)))
                    .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(9)))
            .setRiskFactorScoring(
                RiskFactorScoring.newBuilder()
                    .setDisabled(true)
                    .setScoreContribution(
                        RiskFactorScoreContribution.RISK_FACTOR_SCORE_CONTRIBUTION_ABSOLUTE))
            .build();
    RiskFactorConfig riskFactorConfig2 = getCustomTagUpdatedConfig(true);
    RiskFactorConfig updatedRiskFactorConfig1 = getSensitiveDataExposureFactorConfig(true, 9);

    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            riskConfigServiceStub.updateRiskImpactConfigs(
                UpdateRiskImpactConfigsRequest.newBuilder()
                    .addRiskFactorConfigs(riskFactorConfig1)
                    .addRiskFactorConfigs(riskFactorConfig2)
                    .build()));
    riskImpactConfigs =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.getRiskImpactConfigs(
                        GetRiskImpactConfigsRequest.getDefaultInstance()))
            .getRiskImpactConfigs();
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
                getDefaultCustomTagRiskFactor().toBuilder()
                    .setRiskFactorConfig(riskFactorConfig2)
                    .setIsDefault(false)
                    .build()));

    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            riskConfigServiceStub.resetRiskImpactConfigs(
                ResetRiskImpactConfigsRequest.newBuilder()
                    .setFilter(
                        RiskContributorConfigsResetFilter.newBuilder()
                            .addRiskFactorIds("custom-tag"))
                    .build()));
    riskImpactConfigs =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.getRiskImpactConfigs(
                        GetRiskImpactConfigsRequest.getDefaultInstance()))
            .getRiskImpactConfigs();
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
    assertTrue(riskImpactConfigs.getRiskFactorsList().contains(getDefaultCustomTagRiskFactor()));

    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            riskConfigServiceStub.resetRiskImpactConfigs(
                ResetRiskImpactConfigsRequest.getDefaultInstance()));
    riskImpactConfigs =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    riskConfigServiceStub.getRiskImpactConfigs(
                        GetRiskImpactConfigsRequest.getDefaultInstance()))
            .getRiskImpactConfigs();
    assertEquals(
        defaultRiskImpactConfigs.getRiskFactorsCount(), riskImpactConfigs.getRiskFactorsCount());
    riskImpactConfigs
        .getRiskFactorsList()
        .forEach(
            factor -> assertTrue(defaultRiskImpactConfigs.getRiskFactorsList().contains(factor)));
  }

  private RiskFactor getDefaultCustomTagRiskFactor() {
    return RiskFactor.newBuilder()
        .setIsDefault(true)
        .addCustomizationOptions(CustomizationOptions.CUSTOMIZATION_OPTIONS_ELEMENT_ADD_DELETE)
        .addCustomizationOptions(
            CustomizationOptions.CUSTOMIZATION_OPTIONS_FACTOR_SCORE_CONTRIBUTION)
        .setRiskFactorInfo(
            RiskFactorInfo.newBuilder()
                .setName("custom-tag")
                .setRiskFactorType(RiskFactorType.RISK_FACTOR_TYPE_CUSTOM_TAGS))
        .setRiskFactorConfig(
            RiskFactorConfig.newBuilder()
                .setId("custom-tag")
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("tag-1")
                        .setRiskElementInfo(
                            RiskElementInfo.newBuilder()
                                .setLabelId(
                                    StringPredicate.newBuilder()
                                        .setOperator(StringOperator.STRING_OPERATOR_EQUALS)
                                        .setValue("tag1")))
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(3)))
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("tag-2")
                        .setRiskElementInfo(
                            RiskElementInfo.newBuilder()
                                .setLabelId(
                                    StringPredicate.newBuilder()
                                        .setOperator(StringOperator.STRING_OPERATOR_EQUALS)
                                        .setValue("tag2")))
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(4))))
        .build();
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

  private RiskFactor getDefaultSensitiveDataExposureFactor() {
    return RiskFactor.newBuilder()
        .setIsDefault(true)
        .setRiskFactorInfo(
            RiskFactorInfo.newBuilder()
                .setName("sensitive-data-exposure")
                .setRiskFactorType(RiskFactorType.RISK_FACTOR_TYPE_SENSITIVE_DATA_EXPOSURE))
        .setRiskFactorConfig(
            RiskFactorConfig.newBuilder()
                .setId("sensitive-data-exposure")
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("request-has-1-or-more-params")
                        .setRiskElementInfo(
                            RiskElementInfo.newBuilder()
                                .setRequestParamsCount(
                                    IntPredicate.newBuilder()
                                        .setOperator(IntOperator.INT_OPERATOR_GREATER_THAN)
                                        .setValue(1)))
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(1)))
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("request-has-3-or-more-params")
                        .setRiskElementInfo(
                            RiskElementInfo.newBuilder()
                                .setRequestParamsCount(
                                    IntPredicate.newBuilder()
                                        .setOperator(IntOperator.INT_OPERATOR_GREATER_THAN)
                                        .setValue(3)))
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(3)))
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("request-has-5-or-more-params")
                        .setRiskElementInfo(
                            RiskElementInfo.newBuilder()
                                .setRequestParamsCount(
                                    IntPredicate.newBuilder()
                                        .setOperator(IntOperator.INT_OPERATOR_GREATER_THAN)
                                        .setValue(5)))
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(7)))
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("request-has-10-or-more-params")
                        .setRiskElementInfo(
                            RiskElementInfo.newBuilder()
                                .setRequestParamsCount(
                                    IntPredicate.newBuilder()
                                        .setOperator(IntOperator.INT_OPERATOR_GREATER_THAN)
                                        .setValue(10)))
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(10))))
        .build();
  }

  private RiskFactorConfig getSensitiveDataExposureFactorConfig(
      boolean disabled, int request5orMoreParamsScore) {
    return RiskFactorConfig.newBuilder()
        .setId("sensitive-data-exposure")
        .setRiskFactorScoring(RiskFactorScoring.newBuilder().setDisabled(disabled))
        .addRiskElementConfigs(
            RiskElementConfig.newBuilder()
                .setId("request-has-1-or-more-params")
                .setRiskElementInfo(
                    RiskElementInfo.newBuilder()
                        .setRequestParamsCount(
                            IntPredicate.newBuilder()
                                .setOperator(IntOperator.INT_OPERATOR_GREATER_THAN)
                                .setValue(1)))
                .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(1)))
        .addRiskElementConfigs(
            RiskElementConfig.newBuilder()
                .setId("request-has-3-or-more-params")
                .setRiskElementInfo(
                    RiskElementInfo.newBuilder()
                        .setRequestParamsCount(
                            IntPredicate.newBuilder()
                                .setOperator(IntOperator.INT_OPERATOR_GREATER_THAN)
                                .setValue(3)))
                .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(3)))
        .addRiskElementConfigs(
            RiskElementConfig.newBuilder()
                .setId("request-has-5-or-more-params")
                .setRiskElementInfo(
                    RiskElementInfo.newBuilder()
                        .setRequestParamsCount(
                            IntPredicate.newBuilder()
                                .setOperator(IntOperator.INT_OPERATOR_GREATER_THAN)
                                .setValue(5)))
                .setRiskElementScoring(
                    RiskElementScoring.newBuilder().setScore(request5orMoreParamsScore)))
        .addRiskElementConfigs(
            RiskElementConfig.newBuilder()
                .setId("request-has-10-or-more-params")
                .setRiskElementInfo(
                    RiskElementInfo.newBuilder()
                        .setRequestParamsCount(
                            IntPredicate.newBuilder()
                                .setOperator(IntOperator.INT_OPERATOR_GREATER_THAN)
                                .setValue(10)))
                .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(10)))
        .build();
  }

  private RiskFactor getDefaultMotiveFactor() {
    return RiskFactor.newBuilder()
        .setIsDefault(true)
        .setRiskFactorInfo(
            RiskFactorInfo.newBuilder()
                .setName("motive")
                .setRiskFactorType(RiskFactorType.RISK_FACTOR_TYPE_MOTIVE))
        .setRiskFactorConfig(
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
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(6))))
        .build();
  }
}
