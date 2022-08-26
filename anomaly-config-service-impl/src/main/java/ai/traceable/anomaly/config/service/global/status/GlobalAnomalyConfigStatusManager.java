package ai.traceable.anomaly.config.service.global.status;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatusChange;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface GlobalAnomalyConfigStatusManager {

  List<ScopedAnomalyConfigStatus> getAllScopedAnomalyConfigStatusConfigs(
      RequestContext requestContext);

  ScopedAnomalyConfigStatus getScopedAnomalyConfigStatus(
      RequestContext requestContext, AnomalyConfigScope configScope);

  List<ScopedAnomalyConfigStatus> getAllUnresolvedScopedAnomalyConfigStatusConfigs(
      RequestContext requestContext);

  ScopedAnomalyConfigStatus getUnresolvedScopedAnomalyConfigStatus(
      RequestContext requestContext, AnomalyConfigScope configScope);

  ScopedAnomalyConfigStatusChange updateScopedAnomalyConfigStatus(
      RequestContext requestContext, ScopedAnomalyConfigStatusChange scopedConfigStatusChange);

  void deleteScopedAnomalyGlobalConfigStatus(
      RequestContext requestContext, AnomalyConfigScope scope);
}
