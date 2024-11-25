package ai.traceable.anomaly.config.service.trainer.trainingconfig.filter;

import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfigTypeSpecificFilter;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfigTypeSpecificFilter.TypeCase;

public interface TrainingConfigTypeFilterMatcher {

  boolean match(
      TrainingConfig trainingConfig,
      TrainingConfigTypeSpecificFilter trainingConfigTypeSpecificFilter);

  TypeCase type();
}
