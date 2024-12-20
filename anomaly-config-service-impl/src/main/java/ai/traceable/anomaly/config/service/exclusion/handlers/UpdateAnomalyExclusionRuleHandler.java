package ai.traceable.anomaly.config.service.exclusion.handlers;

import static ai.traceable.anomaly.config.service.exclusion.utils.ParamScopeUtils.populateParamScope;

import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleConfig;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleData;
import ai.traceable.anomaly.config.service.v1.exclusion.UpdateAnomalyExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.exclusion.UpdateAnomalyExclusionRuleResponse;
import jakarta.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class UpdateAnomalyExclusionRuleHandler {
  private final ConfigServiceHandler configServiceHandler;

  @Inject
  UpdateAnomalyExclusionRuleHandler(ConfigServiceHandler configServiceHandler) {
    this.configServiceHandler = configServiceHandler;
  }

  public UpdateAnomalyExclusionRuleResponse updateRule(
      UpdateAnomalyExclusionRuleRequest request, RequestContext requestContext) {

    AnomalyExclusionRuleConfig anomalyExclusionRuleConfig =
        populateParamScope(
            configServiceHandler.getExclusionConfigByRuleId(request.getRuleId(), requestContext));

    AnomalyExclusionRuleData updatedRuleData =
        anomalyExclusionRuleConfig.getRuleData().toBuilder()
            .setName(request.getName())
            .setDescription(request.getDescription())
            .build();

    AnomalyExclusionRuleConfig updatedExclusionRuleConfig =
        anomalyExclusionRuleConfig.toBuilder()
            .setRuleData(updatedRuleData)
            .setConfigStatus(request.getConfigStatus())
            .build();

    configServiceHandler.upsertExclusionConfigByRuleId(updatedExclusionRuleConfig, requestContext);

    return UpdateAnomalyExclusionRuleResponse.newBuilder()
        .setConfig(updatedExclusionRuleConfig)
        .build();
  }
}
