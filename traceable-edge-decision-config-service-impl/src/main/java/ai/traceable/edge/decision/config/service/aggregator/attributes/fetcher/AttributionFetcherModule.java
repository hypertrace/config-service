package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher;

import com.google.inject.AbstractModule;

public class AttributionFetcherModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(UserAttributionFetcher.class).to(UserAttributionFetcher.class);
  }
}
