package ai.traceable.anomaly.config.service.trainer;

import static ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig.TrainingConfigCase.VULNERABILITY_TRAINING_CONFIG;
import static java.util.stream.Collectors.toUnmodifiableList;

import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedVulnerabilityTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.UpdateScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.UpdateVulnerabilityScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityConfig;
import java.util.List;
import java.util.Optional;

public class DefaultScopedTrainingConfigConverter implements ScopedTrainingConfigConverter {
  @Override
  public List<ScopedVulnerabilityTrainingConfig> convert(
      List<ScopedTrainingConfig> vulnerabilityUnresolvedTrainingConfig) {
    return vulnerabilityUnresolvedTrainingConfig.stream()
        .map(this::convert)
        .collect(toUnmodifiableList());
  }

  @Override
  public UpdateScopedTrainingConfigRequest convert(
      UpdateVulnerabilityScopedTrainingConfigRequest request) {

    List<TrainingConfig> trainingConfigs =
        request.getScopedVulnerabilityConfig().getVulnerabilityConfigsList().stream()
            .map(this::convert)
            .collect(toUnmodifiableList());
    return UpdateScopedTrainingConfigRequest.newBuilder()
        .setScopedTrainingConfig(
            ScopedTrainingConfig.newBuilder()
                .setConfigScope(request.getScopedVulnerabilityConfig().getConfigScope())
                .addAllTrainingConfigs(trainingConfigs))
        .build();
  }

  @Override
  public ScopedVulnerabilityTrainingConfig convert(ScopedTrainingConfig scopedTrainingConfig) {
    List<VulnerabilityConfig> vulnerabilityConfigs =
        scopedTrainingConfig.getTrainingConfigsList().stream()
            .map(this::convert)
            .flatMap(Optional::stream)
            .collect(toUnmodifiableList());
    return ScopedVulnerabilityTrainingConfig.newBuilder()
        .setConfigScope(scopedTrainingConfig.getConfigScope())
        .addAllVulnerabilityConfigs(vulnerabilityConfigs)
        .build();
  }

  private TrainingConfig convert(VulnerabilityConfig vulnerabilityConfig) {
    return TrainingConfig.newBuilder()
        .setApplicableScopeFilterId(vulnerabilityConfig.getApplicableScopeFilterId())
        .setDisabled(vulnerabilityConfig.getDisabled())
        .setVulnerabilityTrainingConfig(vulnerabilityConfig.getVulnerabilityTrainingConfig())
        .build();
  }

  private Optional<VulnerabilityConfig> convert(TrainingConfig trainingConfig) {
    if (!trainingConfig.getTrainingConfigCase().equals(VULNERABILITY_TRAINING_CONFIG)) {
      return Optional.empty();
    }
    return Optional.of(
        VulnerabilityConfig.newBuilder()
            .setApplicableScopeFilterId(trainingConfig.getApplicableScopeFilterId())
            .setDisabled(trainingConfig.getDisabled())
            .setVulnerabilityTrainingConfig(trainingConfig.getVulnerabilityTrainingConfig())
            .build());
  }
}
