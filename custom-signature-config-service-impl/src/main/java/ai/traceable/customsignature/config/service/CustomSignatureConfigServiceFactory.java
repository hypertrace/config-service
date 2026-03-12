package ai.traceable.customsignature.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;

public class CustomSignatureConfigServiceFactory {
  public static BindableService build(
      Channel channel,
      Config config,
      ConfigChangeEventGenerator configChangeEventGenerator,
      FeatureCachingClient featureCachingClient,
      GrpcChannelRegistry grpcChannelRegistry,
      KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue> kafkaLiveEventListener) {
    Injector injector =
        Guice.createInjector(
            new CustomSignatureConfigServiceModule(
                channel,
                config,
                configChangeEventGenerator,
                featureCachingClient,
                grpcChannelRegistry,
                kafkaLiveEventListener));
    return injector.getInstance(BindableService.class);
  }
}
