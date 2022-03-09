package ai.traceable.anomaly.config.service.trainer.trainingconfig;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.trainer.DeleteAnomalyConfigOption;
import ai.traceable.anomaly.config.service.v1.trainer.GetTrainingConfigsFilter;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface TrainingConfigManager {

  ScopedTrainingConfig getScopedTrainingConfig(
      RequestContext requestContext,
      AnomalyConfigScope configScope,
      GetTrainingConfigsFilter filter);

  ScopedTrainingConfig updateScopedTrainingConfig(
      RequestContext requestContext, ScopedTrainingConfig scopedTrainingConfig);

  List<ScopedTrainingConfig> getAllScopedTrainingConfig(
      RequestContext requestContext, GetTrainingConfigsFilter filter);

  ScopedTrainingConfig getUnresolvedTrainingConfig(
      RequestContext context, AnomalyConfigScope configScope, GetTrainingConfigsFilter filter);

  List<ScopedTrainingConfig> getAllUnresolvedTrainingConfig(
      RequestContext context, GetTrainingConfigsFilter filter);

  ScopedTrainingConfig deleteTrainingConfig(
      RequestContext context,
      ScopedTrainingConfig filter,
      DeleteAnomalyConfigOption deleteAnomalyConfigOption);
}
