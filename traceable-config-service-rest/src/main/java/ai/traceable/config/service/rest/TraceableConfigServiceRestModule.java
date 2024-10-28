package ai.traceable.config.service.rest;

import ai.traceable.config.service.rest.codegen.DefaultJaxRsResourceCreator;
import ai.traceable.config.service.rest.codegen.JaxRsResourceCreator;
import ai.traceable.config.service.rest.response.ResponseConverterModule;
import ai.traceable.config.service.rest.servlet.ConfigServiceServletModule;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.typesafe.config.Config;
import io.grpc.Channel;
import java.net.http.HttpClient;
import java.time.Clock;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class TraceableConfigServiceRestModule extends AbstractModule {
  private final Channel selfChannel;
  private final TraceableConfigServiceRestConfig restConfig;
  private final GrpcChannelRegistry channelRegistry;

  public TraceableConfigServiceRestModule(
      Config serviceConfig, Channel configChannel, GrpcChannelRegistry channelRegistry) {
    this.selfChannel = configChannel;
    this.channelRegistry = channelRegistry;
    this.restConfig = TraceableConfigServiceRestConfig.from(serviceConfig);
  }

  @Override
  protected void configure() {
    bind(Clock.class).toInstance(Clock.systemUTC());
    bind(HttpClient.class).toInstance(HttpClient.newHttpClient());
    bind(GrpcChannelRegistry.class).toInstance(this.channelRegistry);
    bind(Channel.class).annotatedWith(Names.named("selfChannel")).toInstance(selfChannel);
    install(new ResponseConverterModule());
    install(new ConfigServiceServletModule());
    bind(TraceableConfigServiceRestConfig.class).toInstance(this.restConfig);
    bind(JaxRsResourceCreator.class)
        .toInstance(new DefaultJaxRsResourceCreator(getProvider(RequestContext.class)));
  }
}
