package ai.traceable.config.service;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.typesafe.config.Config;
import io.grpc.Channel;
import java.time.Clock;
import lombok.Value;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.InProcessGrpcChannelRegistry;

@Value
class SharedConfigServiceProviders {
  ConfigChangeEventGenerator changeEventGenerator;
  FeatureCachingClient featureCachingClient;
  ActivityEventProducer activityEventProducer;
  InProcessGrpcChannelRegistry channelRegistry;
  Config config;
  Channel localChannel;
  Clock clock;
}
