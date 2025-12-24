package ai.traceable.edge.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.util.Collections;
import java.util.Set;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;
import org.hypertrace.entity.change.event.v1.EntityChangeEventKey;
import org.hypertrace.entity.change.event.v1.EntityChangeEventValue;

public class TraceableEdgeConfigServiceFactory {
  public static Set<BindableService> build(
      Channel channel,
      Config config,
      GrpcChannelRegistry grpcChannelRegistry,
      FeatureCachingClient featureCachingClient,
      KafkaLiveEventListener<EntityChangeEventKey, EntityChangeEventValue> kafkaLiveEventListener) {
    Injector injector =
        Guice.createInjector(
            Stage.PRODUCTION,
            new TraceableEdgeConfigServiceModule(
                channel,
                config,
                grpcChannelRegistry,
                featureCachingClient,
                kafkaLiveEventListener));
    return Collections.singleton(injector.getInstance(BindableService.class));
  }
}
