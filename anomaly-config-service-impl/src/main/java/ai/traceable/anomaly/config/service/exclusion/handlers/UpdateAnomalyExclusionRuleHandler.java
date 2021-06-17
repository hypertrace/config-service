package ai.traceable.anomaly.config.service.exclusion.handlers;

import ai.traceable.anomaly.config.service.exclusion.converters.AnomalyExclusionRuleConfigConverter;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleConfig;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleData;
import ai.traceable.anomaly.config.service.v1.exclusion.UpdateAnomalyExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.exclusion.UpdateAnomalyExclusionRuleResponse;
import javax.inject.Inject;
import org.hypertrace.config.service.v1.GetConfigResponse;

public class UpdateAnomalyExclusionRuleHandler {
  private final ConfigServiceHandler configServiceHandler;
  private final AnomalyExclusionRuleConfigConverter ruleConfigConverter;

  @Inject
  UpdateAnomalyExclusionRuleHandler(
      ConfigServiceHandler configServiceHandler,
      AnomalyExclusionRuleConfigConverter ruleConfigConverter) {
    this.configServiceHandler = configServiceHandler;
    this.ruleConfigConverter = ruleConfigConverter;
  }

  public UpdateAnomalyExclusionRuleResponse updateRule(UpdateAnomalyExclusionRuleRequest request) {

    GetConfigResponse getConfigResponse =
        configServiceHandler.getExclusionConfigByRuleId(request.getRuleId());
    AnomalyExclusionRuleConfig anomalyExclusionRuleConfig =
        ruleConfigConverter.convert(getConfigResponse.getConfig());

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

    configServiceHandler.upsertExclusionConfigByRuleId(
        request.getRuleId(), ruleConfigConverter.convert(updatedExclusionRuleConfig));

    return UpdateAnomalyExclusionRuleResponse.newBuilder()
        .setConfig(updatedExclusionRuleConfig)
        .build();
  }
}
