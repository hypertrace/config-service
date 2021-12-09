package ai.traceable.anomaly.config.service;

import static ai.traceable.anomaly.config.service.AnomalyConfigServiceFactory.ANOMALY_EXCLUSION_CONFIG_ANNOTATION;
import static ai.traceable.anomaly.config.service.AnomalyConfigServiceFactory.ANOMALY_GLOBAL_CONFIG_ANNOTATION;
import static ai.traceable.anomaly.config.service.AnomalyConfigServiceFactory.ANOMALY_MODSEC_CONFIG_ANNOTATION;
import static ai.traceable.anomaly.config.service.AnomalyConfigServiceFactory.TRAINER_CONFIG_ANNOTATION;

import ai.traceable.anomaly.config.service.common.license.LicenseMeteringServiceModule;
import ai.traceable.anomaly.config.service.exclusion.AnomalyExclusionConfigServiceModule;
import ai.traceable.anomaly.config.service.global.AnomalyGlobalConfigServiceModule;
import ai.traceable.anomaly.config.service.modsec.AnomalyModsecConfigServiceModule;
import ai.traceable.anomaly.config.service.trainer.TrainerConfigServiceModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class AnomalyConfigServiceModule extends AbstractModule {

  private static final String ANOMALY_CONFIG_SERVICE_CONFIG_PATH = "anomaly.config.service";
  private static final String LICENSE_METERING_SERVICE_CONFIG_PATH = "license.metering.service";

  private final Config config;
  private final GrpcChannelRegistry channelRegistry;
  private final ManagedChannel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  AnomalyConfigServiceModule(
      GrpcChannelRegistry channelRegistry,
      ManagedChannel channel,
      Config config,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.channelRegistry = channelRegistry;
    this.config = config;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    install(
        new LicenseMeteringServiceModule(
            config.getConfig(LICENSE_METERING_SERVICE_CONFIG_PATH), channelRegistry));
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    install(new AnomalyGlobalConfigServiceModule(ANOMALY_GLOBAL_CONFIG_ANNOTATION));
    install(new AnomalyExclusionConfigServiceModule(ANOMALY_EXCLUSION_CONFIG_ANNOTATION));
    install(new AnomalyModsecConfigServiceModule(ANOMALY_MODSEC_CONFIG_ANNOTATION));
    install(new TrainerConfigServiceModule(TRAINER_CONFIG_ANNOTATION));
  }

  @Provides
  AnomalyConfigServiceConfig providesAnomalyServiceConfig() {
    return new AnomalyConfigServiceConfig(config.getConfig(ANOMALY_CONFIG_SERVICE_CONFIG_PATH));
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub providesConfigService() {
    return ConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
