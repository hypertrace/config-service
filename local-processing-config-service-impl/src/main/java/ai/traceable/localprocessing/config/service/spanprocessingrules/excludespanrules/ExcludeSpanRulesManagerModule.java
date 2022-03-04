package ai.traceable.localprocessing.config.service.spanprocessingrules.excludespanrules;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.ManagedChannel;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;

public class ExcludeSpanRulesManagerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ExcludeSpanRulesManager.class).to(DefaultExcludeSpanRulesManager.class);
  }

  @Provides
  SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
      providesSpanProcessingConfigServiceBlockingStub(ManagedChannel channel) {
    return SpanProcessingConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
