package ai.traceable.threatmanagement.config.service;

import ai.traceable.threatmanagement.config.service.anomalyscore.AnomalyScoreContributionModule;
import ai.traceable.threatmanagement.config.service.eventscore.SecurityEventScoreContributionModule;
import ai.traceable.threatmanagement.config.service.eventtype.SecurityEventTypeContributionModule;
import ai.traceable.threatmanagement.config.service.threatautoblocking.ThreatAutoBlockingModule;
import ai.traceable.threatmanagement.config.service.threatscore.ThreatScoreModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class ThreatManagementConfigServiceModule extends AbstractModule {
  private final ManagedChannel channel;
  private final Config config;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  ThreatManagementConfigServiceModule(
      ManagedChannel channel,
      Config config,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.config = config;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(ThreatManagementConfigServiceImpl.class);
    bind(ThreatManagementConfigServiceConfig.class)
        .toInstance(new ThreatManagementConfigServiceConfig(config));
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    install(new ThreatScoreModule());
    install(new SecurityEventScoreContributionModule());
    install(new AnomalyScoreContributionModule());
    install(new SecurityEventTypeContributionModule());
    install(new ThreatAutoBlockingModule());
  }

  @Provides
  ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
