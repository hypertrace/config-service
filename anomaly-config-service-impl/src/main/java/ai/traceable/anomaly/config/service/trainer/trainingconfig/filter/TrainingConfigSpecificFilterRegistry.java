package ai.traceable.anomaly.config.service.trainer.trainingconfig.filter;

import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfigTypeSpecificFilter.TypeCase;
import com.google.inject.ImplementedBy;

@ImplementedBy(TrainingConfigSpecificFilterRegistryImpl.class)
public interface TrainingConfigSpecificFilterRegistry {

  TrainingConfigTypeFilterMatcher type(TypeCase typeCase);
}
