package ai.traceable.anomaly.config.service.detector;

import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigModule;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import io.grpc.BindableService;

public class DetectorConfigServiceModule extends AbstractModule {
  private final String bindableServiceAnnotation;

  public DetectorConfigServiceModule(String bindableServiceAnnotation) {
    this.bindableServiceAnnotation = bindableServiceAnnotation;
  }

  @Override
  protected void configure() {
    bind(BindableService.class)
        .annotatedWith(Names.named(bindableServiceAnnotation))
        .to(DetectorConfigServiceImpl.class);
    install(new AnomalyDetectionConfigModule());
  }
}
