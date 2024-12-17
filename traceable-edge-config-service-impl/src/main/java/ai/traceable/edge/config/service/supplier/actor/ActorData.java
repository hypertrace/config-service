package ai.traceable.edge.config.service.supplier.actor;

import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_ACTOR_ID;
import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_ENTITY_ID;
import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_ENVIRONMENT;
import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_IP_ADDRESSES;
import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_STATUS;
import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_STATUS_EXPIRY_TIMESTAMP;

import ai.traceable.platform.actor.v1.Status;
import ai.traceable.platform.actor.v1.StatusChangeSource;
import ai.traceable.platform.actor.v1.converter.StatusConverter;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AllArgsConstructor
@Builder
@lombok.Value
class ActorData {
  private static final Logger LOGGER = LoggerFactory.getLogger(ActorData.class);
  private static final StatusChangeSource DEFAULT_STATUS_CHANGE_SOURCE =
      StatusChangeSource.STATUS_CHANGE_SOURCE_UNSPECIFIED;
  private static final Long DEFAULT_EXPIRATION_TIMESTAMP_MILLIS = 0L;

  @NonNull String actorId;
  @NonNull String entityId;
  String envId;
  @NonNull List<String> ipAddresses;
  @NonNull Status status;
  Long expirationTimestampMillis;

  boolean isActive(Clock clock) {
    return expirationTimestampMillis == 0
        || expirationTimestampMillis > clock.instant().toEpochMilli();
  }

  private ActorData(Map<String, Value> actorFieldsMap) {
    this(
        parseActorId(actorFieldsMap),
        parseEntityId(actorFieldsMap),
        parseEnvId(actorFieldsMap),
        parseIpAddresses(actorFieldsMap),
        parseStatus(actorFieldsMap),
        parseExpirationTimestampMillis(actorFieldsMap));
  }

  static Optional<ActorData> getActorData(Struct actor) {
    try {
      Map<String, Value> actorFieldsMap = actor.getFieldsMap();
      return Optional.of(new ActorData(actorFieldsMap));
    } catch (Exception e) {
      LOGGER.error("Cannot parse actor-details from actor-query struct - {}, error - ", actor, e);
      return Optional.empty();
    }
  }

  private static String parseActorId(Map<String, Value> actorFieldsMap) {
    return actorFieldsMap.get(ACTOR_FIELD_ACTOR_ID.name()).getStringValue();
  }

  private static String parseEntityId(Map<String, Value> actorFieldsMap) {
    return actorFieldsMap.get(ACTOR_FIELD_ENTITY_ID.name()).getStringValue();
  }

  private static String parseEnvId(Map<String, Value> actorFieldsMap) {
    return actorFieldsMap.get(ACTOR_FIELD_ENVIRONMENT.name()).getStringValue();
  }

  private static List<String> parseIpAddresses(Map<String, Value> actorFieldsMap) {
    return actorFieldsMap
        .get(ACTOR_FIELD_IP_ADDRESSES.name())
        .getListValue()
        .getValuesList()
        .stream()
        .map(Value::getStringValue)
        .collect(Collectors.toUnmodifiableList());
  }

  private static Status parseStatus(Map<String, Value> actorFieldsMap) {
    return StatusConverter.convert(actorFieldsMap.get(ACTOR_FIELD_STATUS.name()).getStringValue());
  }

  private static Long parseExpirationTimestampMillis(Map<String, Value> actorFieldsMap) {
    try {
      return (long) actorFieldsMap.get(ACTOR_FIELD_STATUS_EXPIRY_TIMESTAMP.name()).getNumberValue();
    } catch (Exception e) {
      LOGGER.warn(
          "Cannot parse expiration timestamp from actor-query map - {}, error - ",
          actorFieldsMap,
          e);
      return DEFAULT_EXPIRATION_TIMESTAMP_MILLIS;
    }
  }
}
