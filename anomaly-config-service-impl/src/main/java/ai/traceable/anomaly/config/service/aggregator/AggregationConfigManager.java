package ai.traceable.anomaly.config.service.aggregator;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.aggregator.ScopedAnomalyEventAggregationConfig;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface AggregationConfigManager {
  ScopedAnomalyEventAggregationConfig getScopedAnomalyAggregationConfig(
      RequestContext requestContext, AnomalyConfigScope configScope);

  ScopedAnomalyEventAggregationConfig updateScopedAnomalyEventAggregationConfig(
      RequestContext requestContext,
      ScopedAnomalyEventAggregationConfig scopedAnomalyEventAggregationConfig);

  List<ScopedAnomalyEventAggregationConfig> getAllScopedAnomalyEventAggregationConfigs(
      RequestContext requestContext);

  ScopedAnomalyEventAggregationConfig getUnresolvedScopedAnomalyEventAggregationConfig(
      RequestContext requestContext, AnomalyConfigScope configScope);

  List<ScopedAnomalyEventAggregationConfig> getAllUnresolvedScopedAnomalyEventAggregationConfigs(
      RequestContext requestContext);

  void deleteScopedAnomalyEventAggregationConfig(
      RequestContext requestContext, AnomalyConfigScope anomalyConfigScope);
}
