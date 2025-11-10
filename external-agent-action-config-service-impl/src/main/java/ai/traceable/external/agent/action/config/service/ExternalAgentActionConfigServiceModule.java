package ai.traceable.external.agent.action.config.service;

import ai.traceable.agent.action.config.service.v1.AgentActionConfigServiceGrpc;
import ai.traceable.agent.action.config.service.v1.AgentActionConfigServiceGrpc.AgentActionConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.time.Clock;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class ExternalAgentActionConfigServiceModule extends AbstractModule {
  public ExternalAgentActionConfigServiceModule(Channel channel) {
    this.channel = channel;
  }

  private final Channel channel;

  @Override
  protected void configure() {
    bind(BindableService.class).to(ExternalAgentActionConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(Clock.class).toInstance(Clock.systemUTC());
  }

  @Provides
  @Singleton
  AgentActionConfigServiceBlockingStub providesAgentActionConfigServiceBlockingStub(
      Channel channel) {
    return AgentActionConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
