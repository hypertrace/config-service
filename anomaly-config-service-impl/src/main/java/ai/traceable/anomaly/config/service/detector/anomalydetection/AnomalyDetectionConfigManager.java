package ai.traceable.anomaly.config.service.detector.anomalydetection;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.detector.DeleteAnomalyConfigOption;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface AnomalyDetectionConfigManager {
  ScopedAnomalyDetectionConfig getScopedAnomalyDetectionConfig(
      RequestContext requestContext,
      AnomalyConfigScope configScope,
      GetAnomalyDetectionConfigsFilter filter);

  ScopedAnomalyDetectionConfig updateScopedAnomalyDetectionConfig(
      RequestContext requestContext, ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig);

  List<ScopedAnomalyDetectionConfig> getAllScopedAnomalyDetectionConfig(
      RequestContext requestContext, GetAnomalyDetectionConfigsFilter filter);

  ScopedAnomalyDetectionConfig getUnresolvedScopedAnomalyDetectionConfig(
      RequestContext requestContext,
      AnomalyConfigScope configScope,
      GetAnomalyDetectionConfigsFilter filter);

  List<ScopedAnomalyDetectionConfig> getAllUnresolvedScopedAnomalyDetectionConfigs(
      RequestContext requestContext, GetAnomalyDetectionConfigsFilter filter);

  ScopedAnomalyDetectionConfig deleteScopedAnomalyDetectionConfig(
      RequestContext requestContext,
      ScopedAnomalyDetectionConfig deleteScopedAnomalyDetectionConfig,
      DeleteAnomalyConfigOption deleteAnomalyConfigOption);
}
