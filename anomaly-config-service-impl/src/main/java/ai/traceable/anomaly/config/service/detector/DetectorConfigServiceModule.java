package ai.traceable.anomaly.config.service.detector;

import ai.traceable.anomaly.config.service.AnomalyConfigServiceConfig;
import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigModule;
import ai.traceable.anomaly.config.service.detector.migration.AnomalyDetectionMigrationModule;
import ai.traceable.anomaly.config.service.registry.accounttakeover.AccountTakeoverRulesRegistry;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.registry.credentialstuffing.CredentialStuffingRulesRegistry;
import ai.traceable.anomaly.config.service.registry.genai.GenAiRulesRegistry;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.registry.volumetric.VolumetricRulesRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
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
    install(new AnomalyDetectionMigrationModule());
  }

  @Provides
  DetectorConfigServiceConfig providesDetectorConfigServiceConfig(
      AnomalyConfigServiceConfig config,
      ApiDefinitionRegistry apiDefinitionRegistry,
      SessionRulesRegistry sessionDefinitionRegistry,
      VolumetricRulesRegistry volumetricRulesRegistry,
      CredentialStuffingRulesRegistry credentialStuffingRulesRegistry,
      AccountTakeoverRulesRegistry accountTakeoverRulesRegistry,
      GenAiRulesRegistry genAiRulesRegistry) {

    return new DetectorConfigServiceConfig(
        config.getDetectorConfigServiceConfig(),
        apiDefinitionRegistry,
        sessionDefinitionRegistry,
        volumetricRulesRegistry,
        credentialStuffingRulesRegistry,
        accountTakeoverRulesRegistry,
        genAiRulesRegistry);
  }
}
