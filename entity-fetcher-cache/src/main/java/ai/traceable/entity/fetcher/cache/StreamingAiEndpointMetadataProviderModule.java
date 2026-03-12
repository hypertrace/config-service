package ai.traceable.entity.fetcher.cache;

import com.google.inject.AbstractModule;

public class StreamingAiEndpointMetadataProviderModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(StreamingAiEndpointMetadataProvider.class)
        .to(DefaultStreamingAiEndpointMetadataProvider.class);
  }
}
