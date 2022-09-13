package ai.traceable.region.config.service;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.region.config.service.regions.RegionStore;
import ai.traceable.region.config.service.regions.RegionStoreModule;
import ai.traceable.region.config.service.rules.RulesManagerModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.name.Names;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

class RegionConfigServiceModule extends AbstractModule {
  static final String NEUSTAR_REGION_STORE = "neustarRegionStore";
  static final String IPQS_REGION_STORE = "ipqsRegionStore";

  private final RegionConfigServiceConfig config;
  private final Channel channel;
  private final ActivityEventProducer activityEventProducer;
  private final ConfigChangeEventGenerator configChangeEventGenerator;
  private final FeatureCachingClient featureCachingClient;

  RegionConfigServiceModule(
      Channel channel,
      Config config,
      ActivityEventProducer activityEventProducer,
      ConfigChangeEventGenerator configChangeEventGenerator,
      FeatureCachingClient featureCachingClient) {
    this.channel = channel;
    this.config = new RegionConfigServiceConfig(config);
    this.activityEventProducer = activityEventProducer;
    this.configChangeEventGenerator = configChangeEventGenerator;
    this.featureCachingClient = featureCachingClient;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(RegionConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(ActivityEventProducer.class).toInstance(activityEventProducer);
    bind(RegionConfigServiceConfig.class).toInstance(this.config);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(FeatureCachingClient.class).toInstance(featureCachingClient);

    bind(RegionStore.class)
        .annotatedWith(Names.named(IPQS_REGION_STORE))
        .toInstance(
            Guice.createInjector(new RegionStoreModule(config.getIpqsCountriesDataPath()))
                .getInstance(RegionStore.class));
    bind(RegionStore.class)
        .annotatedWith(Names.named(NEUSTAR_REGION_STORE))
        .toInstance(
            Guice.createInjector(new RegionStoreModule(config.getNeustarCountriesDataPath()))
                .getInstance(RegionStore.class));
    install(new RulesManagerModule());
  }
}
