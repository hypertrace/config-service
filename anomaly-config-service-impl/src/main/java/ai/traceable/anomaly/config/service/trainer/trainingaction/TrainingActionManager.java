package ai.traceable.anomaly.config.service.trainer.trainingaction;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingActionConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingAction;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface TrainingActionManager {

  ScopedTrainingActionConfig upsertTrainingAction(
      RequestContext requestContext, AnomalyConfigScope configScope, TrainingAction trainingAction);

  List<ScopedTrainingActionConfig> getAllTrainingActions(RequestContext requestContext);

  ScopedTrainingActionConfig getTrainingAction(
      RequestContext requestContext, AnomalyConfigScope configScope);

  void deleteTrainingAction(RequestContext requestContext, AnomalyConfigScope configScope);
}
