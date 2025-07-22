package ai.traceable.anomaly.config.service.global.status;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.RuleType;
import ai.traceable.anomaly.config.service.v1.RuleVersionConfigType;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatusChange;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface GlobalAnomalyConfigStatusManager {

  List<ScopedAnomalyConfigStatus> getAllScopedAnomalyConfigStatusConfigs(
      RequestContext requestContext, List<AnomalyConfigScope> applicableScopesList);

  ScopedAnomalyConfigStatus getScopedAnomalyConfigStatus(
      RequestContext requestContext, AnomalyConfigScope configScope);

  List<ScopedAnomalyConfigStatusChange> getAllUnresolvedScopedAnomalyConfigStatusConfigs(
      RequestContext requestContext, List<AnomalyConfigScope> applicableScopesList);

  ScopedAnomalyConfigStatusChange getUnresolvedScopedAnomalyConfigStatus(
      RequestContext requestContext, AnomalyConfigScope configScope);

  ScopedAnomalyConfigStatusChange updateScopedAnomalyConfigStatus(
      RequestContext requestContext, ScopedAnomalyConfigStatusChange scopedConfigStatusChange);

  void deleteScopedAnomalyGlobalConfigStatus(
      RequestContext requestContext, AnomalyConfigScope scope);

  ScopedAnomalyConfigStatusChange deleteRuleVersionConfigType(
      RequestContext requestContext,
      AnomalyConfigScope scope,
      List<RuleVersionConfigType> ruleVersionConfigTypes,
      RuleType ruleType);
}
