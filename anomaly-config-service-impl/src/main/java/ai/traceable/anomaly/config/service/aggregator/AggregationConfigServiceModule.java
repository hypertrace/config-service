package ai.traceable.anomaly.config.service.aggregator;

import ai.traceable.anomaly.config.service.AnomalyConfigServiceConfig;
import ai.traceable.anomaly.config.service.aggregator.config.AggregationConfigServiceConfig;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import io.grpc.BindableService;
import java.time.Clock;

public class AggregationConfigServiceModule extends AbstractModule {

  private final String bindableServiceAnnotation;

  public AggregationConfigServiceModule(String bindableServiceAnnotation) {
    this.bindableServiceAnnotation = bindableServiceAnnotation;
  }

  @Override
  protected void configure() {
    bind(Clock.class).toInstance(Clock.systemUTC());
    bind(BindableService.class)
        .annotatedWith(Names.named(bindableServiceAnnotation))
        .to(AggregationConfigServiceImpl.class);
    bind(AggregationConfigManager.class).to(AggregationConfigManagerImpl.class);
  }

  @Provides
  AggregationConfigServiceConfig providesAggregationConfigServiceConfig(
      AnomalyConfigServiceConfig config) {
    return new AggregationConfigServiceConfig(config.getAggregatorConfigServiceConfig());
  }
}
