package ai.traceable.anomaly.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.util.List;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;

public class AnomalyConfigServiceFactory {

  static final String ANOMALY_GLOBAL_CONFIG_ANNOTATION = "anomalyGlobalConfig";
  static final String ANOMALY_EXCLUSION_CONFIG_ANNOTATION = "anomalyExclusionConfig";
  static final String ANOMALY_MODSEC_CONFIG_ANNOTATION = "anomalyModsecConfig";
  static final String ANOMALY_API_PROTECT_CONFIG_ANNOTATION = "anomalyApiProtectConfig";
  static final String TRAINER_CONFIG_ANNOTATION = "trainerConfig";
  static final String DETECTOR_CONFIG_ANNOTATION = "detectorConfig";
  static final String AGGREGATOR_CONFIG_ANNOTATION = "aggregatorConfig";

  public static List<BindableService> build(
      GrpcChannelRegistry channelRegistry,
      Channel channel,
      Config config,
      ConfigChangeEventGenerator configChangeEventGenerator,
      FeatureCachingClient featureCachingClient,
      KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue> kafkaLiveEventListener) {
    Injector injector =
        Guice.createInjector(
            new AnomalyConfigServiceModule(
                channelRegistry,
                channel,
                config,
                configChangeEventGenerator,
                featureCachingClient,
                kafkaLiveEventListener));

    return ImmutableList.of(
        getInjectorInstance(injector, ANOMALY_GLOBAL_CONFIG_ANNOTATION),
        getInjectorInstance(injector, ANOMALY_EXCLUSION_CONFIG_ANNOTATION),
        getInjectorInstance(injector, ANOMALY_MODSEC_CONFIG_ANNOTATION),
        getInjectorInstance(injector, ANOMALY_API_PROTECT_CONFIG_ANNOTATION),
        getInjectorInstance(injector, TRAINER_CONFIG_ANNOTATION),
        getInjectorInstance(injector, DETECTOR_CONFIG_ANNOTATION),
        getInjectorInstance(injector, AGGREGATOR_CONFIG_ANNOTATION));
  }

  private static <T> BindableService getInjectorInstance(Injector injector, String annotation) {
    return injector.getInstance(Key.get(BindableService.class, Names.named(annotation)));
  }
}
