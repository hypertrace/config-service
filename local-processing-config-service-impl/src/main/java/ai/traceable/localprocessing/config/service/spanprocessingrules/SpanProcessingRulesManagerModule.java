package ai.traceable.localprocessing.config.service.spanprocessingrules;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.ManagedChannel;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;

public class SpanProcessingRulesManagerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(SpanProcessingRulesManager.class).to(DefaultSpanProcessingRulesManager.class);
  }

  @Provides
  SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
      providesSpanProcessingConfigServiceBlockingStub(ManagedChannel channel) {
    return SpanProcessingConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
