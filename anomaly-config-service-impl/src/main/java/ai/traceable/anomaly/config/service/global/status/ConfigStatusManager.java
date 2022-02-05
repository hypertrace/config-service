package ai.traceable.anomaly.config.service.global.status;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Deprecated
public interface ConfigStatusManager {

  AnomalyConfigStatus getAnomalyConfigStatus(
      RequestContext requestContext, AnomalyConfigScope configScope);

  AnomalyConfigStatusChange updateAnomalyConfigStatus(
      RequestContext requestContext,
      AnomalyConfigScope configScope,
      AnomalyConfigStatusChange configStatusChange);
}
