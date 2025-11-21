package ai.traceable.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.typesafe.config.Config;
import io.grpc.Channel;
import java.time.Clock;
import lombok.Value;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.InProcessGrpcChannelRegistry;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;

@Value
class SharedConfigServiceProviders {
  ConfigChangeEventGenerator changeEventGenerator;
  FeatureCachingClient featureCachingClient;
  InProcessGrpcChannelRegistry channelRegistry;
  Config config;
  Channel localChannel;
  Clock clock;
  KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue> kafkaLiveEventListener;
}
