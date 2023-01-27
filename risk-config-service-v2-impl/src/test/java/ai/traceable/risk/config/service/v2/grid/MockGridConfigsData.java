package ai.traceable.risk.config.service.v2.grid;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.risk.config.service.v2.RiskConfigBuilder;
import ai.traceable.risk.config.service.v2.RiskConfigConverter;
import ai.traceable.risk.config.service.v2.RiskConfigIdGenerator;
import ai.traceable.risk.config.service.v2.RiskScoreCategory;
import ai.traceable.risk.config.service.v2.RiskScoringGridCell;
import ai.traceable.risk.config.service.v2.RiskScoringGridConfigValues;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.DeletedContextualConfigObject;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class MockGridConfigsData {

  private static final String ID_NAME = "RiskScoringGrid";

  public static RiskScoringGridConfigValues getDefaultGridConfig() {
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

  public static class MockRiskScoringGridConfigStore extends RiskScoringGridConfigStore {

    private Map<String, Map<String, ContextualConfigObject<RiskScoringGridConfigValues>>>
        tenantValuesMap = new HashMap<>();
    private final RiskConfigIdGenerator configIdGenerator;

    public MockRiskScoringGridConfigStore(
        ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
        RiskConfigConverter<RiskScoringGridConfigValues> configConverter,
        RiskConfigBuilder<RiskScoringGridConfigValues> configBuilder,
        ConfigChangeEventGenerator configChangeEventGenerator,
        RiskConfigIdGenerator configIdGenerator) {
      super(
          configServiceBlockingStub,
          configConverter,
          configBuilder,
          configChangeEventGenerator,
          configIdGenerator);
      this.configIdGenerator = configIdGenerator;
    }

    @Override
    public List<ContextualConfigObject<RiskScoringGridConfigValues>> getAllObjects(
        RequestContext requestContext) {
      String tenantId = requestContext.getTenantId().get();
      return tenantValuesMap.containsKey(tenantId)
          ? new ArrayList<>(tenantValuesMap.get(tenantId).values())
          : Collections.emptyList();
    }

    @Override
    public Optional<RiskScoringGridConfigValues> getData(RequestContext requestContext, String id) {
      return Optional.ofNullable(tenantValuesMap.get(requestContext.getTenantId().get()))
          .map(valuesMap -> valuesMap.get(id))
          .map(ConfigObject::getData);
    }

    @Override
    public ContextualConfigObject<RiskScoringGridConfigValues> upsertObject(
        RequestContext requestContext, RiskScoringGridConfigValues riskScoringGridConfigValues) {
      String tenantId = requestContext.getTenantId().get();
      if (!tenantValuesMap.containsKey(tenantId)) {
        tenantValuesMap.put(tenantId, new HashMap<>());
      }

      ContextualConfigObject<RiskScoringGridConfigValues> configObject =
          mock(ContextualConfigObject.class);
      when(configObject.getData()).thenReturn(riskScoringGridConfigValues);

      tenantValuesMap
          .get(tenantId)
          .put(
              configIdGenerator.generateId(
                  ID_NAME, riskScoringGridConfigValues.getRiskConfigScope()),
              configObject);
      return configObject;
    }

    @Override
    public Optional<DeletedContextualConfigObject<RiskScoringGridConfigValues>> deleteObject(
        RequestContext requestContext, String id) {
      requestContext.getTenantId().map(tenantValuesMap::get).ifPresent(map -> map.remove(id));
      return Optional.of(mock(DeletedContextualConfigObject.class));
    }
  }
}
