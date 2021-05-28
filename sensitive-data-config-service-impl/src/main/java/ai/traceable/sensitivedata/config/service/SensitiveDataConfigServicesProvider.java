package ai.traceable.sensitivedata.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class SensitiveDataConfigServicesProvider {

  private final Injector injector;

  public SensitiveDataConfigServicesProvider(
      Channel channel, Config config, GrpcChannelRegistry channelRegistry) {
    this.injector = Guice.createInjector(new SensitiveDataModule(channel, config, channelRegistry));
  }

  public BindableService getSensitiveDataConfigService() {
    return this.injector.getInstance(SensitiveDataConfigServiceImpl.class);
  }

  public BindableService getPiiFilterConfigService() {
    return this.injector.getInstance(PiiFilterConfigServiceImpl.class);
  }
}
