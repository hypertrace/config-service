package ai.traceable.anomaly.config.service.exclusion.utils;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamInfo;
import ai.traceable.anomaly.config.service.v1.AnomalyParamInfoScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamScope;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleConfig;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleData;

public class ParamScopeUtils {

  public static AnomalyExclusionRuleConfig populateParamScope(
      AnomalyExclusionRuleConfig anomalyExclusionRuleConfig) {

    AnomalyExclusionRuleData ruleData = anomalyExclusionRuleConfig.getRuleData();
    return anomalyExclusionRuleConfig.toBuilder().setRuleData(populateParamScope(ruleData)).build();
  }

  public static AnomalyExclusionRuleData populateParamScope(
      AnomalyExclusionRuleData anomalyExclusionRuleData) {

    AnomalyConfigScope configScope = anomalyExclusionRuleData.getAnomalyConfigScope();
    if (configScope.hasParamScope()) {
      AnomalyParamScope paramScope = configScope.getParamScope();
      AnomalyParamScope.Builder paramScopeBuilder = paramScope.toBuilder();

      if (!paramScope.hasParamInfo()) {
        paramScopeBuilder.setParamInfo(
            AnomalyParamInfo.newBuilder().setParamName(paramScope.getParamName()).build());
      }
      if (!paramScope.hasScope()) {
        paramScopeBuilder.setScope(
            AnomalyParamInfoScope.newBuilder().setApiScope(paramScope.getApiScope()).build());
      }

      return anomalyExclusionRuleData.toBuilder()
          .setAnomalyConfigScope(
              AnomalyConfigScope.newBuilder().setParamScope(paramScopeBuilder).build())
          .build();
    }
    return anomalyExclusionRuleData;
  }
}
