package ai.traceable.anomaly.config.service.detector.migration;

import com.google.inject.AbstractModule;
import jakarta.inject.Singleton;

public class AnomalyDetectionMigrationModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(AnomalyDetectionMigrationStore.class).in(Singleton.class);
    bind(ApiProtectMigrationProcessor.class).in(Singleton.class);
  }
}
