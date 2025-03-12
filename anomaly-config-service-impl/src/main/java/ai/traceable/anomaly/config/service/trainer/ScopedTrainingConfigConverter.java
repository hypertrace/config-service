package ai.traceable.anomaly.config.service.trainer;

import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedVulnerabilityTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.UpdateScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.UpdateVulnerabilityScopedTrainingConfigRequest;
import com.google.inject.ImplementedBy;
import java.util.List;

@ImplementedBy(DefaultScopedTrainingConfigConverter.class)
public interface ScopedTrainingConfigConverter {
  List<ScopedVulnerabilityTrainingConfig> convert(
      List<ScopedTrainingConfig> vulnerabilityUnresolvedTrainingConfig);

  UpdateScopedTrainingConfigRequest convert(UpdateVulnerabilityScopedTrainingConfigRequest request);

  ScopedVulnerabilityTrainingConfig convert(ScopedTrainingConfig updatedScopedTrainingConfig);
}
