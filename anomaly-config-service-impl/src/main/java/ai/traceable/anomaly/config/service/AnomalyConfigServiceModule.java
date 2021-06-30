package ai.traceable.anomaly.config.service;

import static ai.traceable.anomaly.config.service.AnomalyConfigServiceFactory.ANOMALY_EXCLUSION_CONFIG_ANNOTATION;
import static ai.traceable.anomaly.config.service.AnomalyConfigServiceFactory.ANOMALY_GLOBAL_CONFIG_ANNOTATION;
import static ai.traceable.anomaly.config.service.AnomalyConfigServiceFactory.ANOMALY_MODSEC_CONFIG_ANNOTATION;

import ai.traceable.anomaly.config.service.exclusion.AnomalyExclusionConfigServiceModule;
import ai.traceable.anomaly.config.service.global.AnomalyGlobalConfigServiceModule;
import ai.traceable.anomaly.config.service.modsec.AnomalyModsecConfigServiceModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class AnomalyConfigServiceModule extends AbstractModule {

  private static final String ANOMALY_CONFIG_SERVICE_CONFIG_PATH = "anomaly.config.service";

  private final AnomalyConfigServiceConfig config;
  private final ManagedChannel channel;

  AnomalyConfigServiceModule(ManagedChannel channel, Config config) {
    this.channel = channel;
    this.config =
        new AnomalyConfigServiceConfig(config.getConfig(ANOMALY_CONFIG_SERVICE_CONFIG_PATH));
  }

  @Override
  protected void configure() {
    install(
        new AnomalyGlobalConfigServiceModule(
            config.getAnomalyGlobalConfig(), ANOMALY_GLOBAL_CONFIG_ANNOTATION));
    install(new AnomalyExclusionConfigServiceModule(ANOMALY_EXCLUSION_CONFIG_ANNOTATION));
    install(new AnomalyModsecConfigServiceModule(ANOMALY_MODSEC_CONFIG_ANNOTATION));
  }

  @Provides
  AnomalyConfigServiceConfig providesAnomalyServiceConfig() {
    return config;
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub providesConfigService() {
    return ConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
