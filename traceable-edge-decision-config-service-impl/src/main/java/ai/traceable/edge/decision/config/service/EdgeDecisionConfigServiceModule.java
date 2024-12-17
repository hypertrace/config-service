package ai.traceable.edge.decision.config.service;

import static ai.traceable.edge.decision.config.service.VariableConstants.USER_ATTRIBUTION_VARIABLE_NAME;

import ai.traceable.edge.decision.config.service.aggregator.attributes.UserAttributionVariableEnricher;
import ai.traceable.edge.decision.config.service.aggregator.attributes.VariableEnricherBase;
import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.DefaultUserAttributionFetcher;
import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.UserAttributionFetcher;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class EdgeDecisionConfigServiceModule extends AbstractModule {
  private final Config config;
  private final Channel channel;
  private final ConfigChangeEventGenerator changeEventGenerator;

  EdgeDecisionConfigServiceModule(
      Config config, Channel channel, ConfigChangeEventGenerator changeEventGenerator) {
    this.config = config;
    this.channel = channel;
    this.changeEventGenerator = changeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(Config.class).toInstance(config);
    bind(ConfigChangeEventGenerator.class).toInstance(changeEventGenerator);
    bind(BindableService.class).to(EdgeDecisionConfigService.class);
    bind(UserAttributionFetcher.class).to(DefaultUserAttributionFetcher.class);

    MapBinder<VariableConstants, VariableEnricherBase> mapBinder =
        MapBinder.newMapBinder(binder(), VariableConstants.class, VariableEnricherBase.class);
    mapBinder.addBinding(USER_ATTRIBUTION_VARIABLE_NAME).to(UserAttributionVariableEnricher.class);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
