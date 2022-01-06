package ai.traceable.anomaly.config.service.detector.anomalydetection;

import com.google.inject.AbstractModule;

public class AnomalyDetectionConfigModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(AnomalyDetectionConfigManager.class).to(AnomalyDetectionConfigManagerImpl.class);
  }
}
