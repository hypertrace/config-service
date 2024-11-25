package ai.traceable.anomaly.config.service.trainer.trainingconfig.filter;

import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig.TrainingConfigCase;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfigTypeSpecificFilter;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfigTypeSpecificFilter.TypeCase;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DefaultTrainingConfigTypeFilterMatcher implements TrainingConfigTypeFilterMatcher {

  private final TypeCase trainingConfigSpecificFilterType;
  private final TrainingConfigCase trainingConfigType;

  @Override
  public boolean match(
      TrainingConfig trainingConfig,
      TrainingConfigTypeSpecificFilter trainingConfigTypeSpecificFilter) {
    return trainingConfig.getTrainingConfigCase().equals(trainingConfigType);
  }

  @Override
  public TypeCase type() {
    return trainingConfigSpecificFilterType;
  }
}
