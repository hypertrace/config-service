package ai.traceable.alerting.config.service;

import ai.traceable.alerting.config.service.v2.EventCondition;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import io.grpc.Channel;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

@Slf4j
public class EventConditionStore extends IdentifiedObjectStore<EventCondition> {

  public static final String ALERTING_EVENT_CONDITION_CONFIG_RESOURCE_NAME =
      "alertingEventConditionConfig";
  public static final String ALERTING_CONFIG_NAMESPACE = "alerting-v1";

  public EventConditionStore(Channel configChannel) {
    super(
        ConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get()),
        ALERTING_CONFIG_NAMESPACE,
        ALERTING_EVENT_CONDITION_CONFIG_RESOURCE_NAME);
  }

  @Override
  protected Optional<EventCondition> buildDataFromValue(Value value) {
    EventCondition.Builder builder = EventCondition.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Conversion failed. value {}", value, e);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(EventCondition data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected String getContextFromData(EventCondition data) {
    return data.getId();
  }
}
