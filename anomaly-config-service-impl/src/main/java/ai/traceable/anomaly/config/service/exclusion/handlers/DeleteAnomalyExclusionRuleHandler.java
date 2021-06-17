package ai.traceable.anomaly.config.service.exclusion.handlers;

import ai.traceable.anomaly.config.service.v1.exclusion.DeleteAnomalyExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.exclusion.DeleteAnomalyExclusionRuleResponse;
import javax.inject.Inject;

public class DeleteAnomalyExclusionRuleHandler {
  private final ConfigServiceHandler configServiceHandler;

  @Inject
  DeleteAnomalyExclusionRuleHandler(ConfigServiceHandler configServiceHandler) {
    this.configServiceHandler = configServiceHandler;
  }

  public DeleteAnomalyExclusionRuleResponse deleteRule(DeleteAnomalyExclusionRuleRequest request) {
    configServiceHandler.deleteExclusionConfigByRuleId(request.getRuleId());
    return DeleteAnomalyExclusionRuleResponse.newBuilder().build();
  }
}
