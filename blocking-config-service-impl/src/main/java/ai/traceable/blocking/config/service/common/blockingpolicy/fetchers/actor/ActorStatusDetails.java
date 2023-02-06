package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor;

import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_ACTOR_ID;
import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_ENTITY_ID;
import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_IP_ADDRESSES;
import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_STATUS;
import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_STATUS_CHANGE_DETAILS;
import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_STATUS_CHANGE_SOURCE;
import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_STATUS_EXPIRY_TIMESTAMP;

import ai.traceable.platform.actor.v1.MaliciousSourcesDetails;
import ai.traceable.platform.actor.v1.RateLimitDetails;
import ai.traceable.platform.actor.v1.Status;
import ai.traceable.platform.actor.v1.StatusChangeDetails;
import ai.traceable.platform.actor.v1.StatusChangeDetails.Builder;
import ai.traceable.platform.actor.v1.StatusChangeSource;
import ai.traceable.platform.actor.v1.converter.StatusChangeSourceConverter;
import ai.traceable.platform.actor.v1.converter.StatusConverter;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AllArgsConstructor
@lombok.Value
class ActorStatusDetails {
  private static final Logger LOGGER = LoggerFactory.getLogger(ActorStatusDetails.class);
  private static final StatusChangeSource DEFAULT_STATUS_CHANGE_SOURCE =
      StatusChangeSource.STATUS_CHANGE_SOURCE_UNSPECIFIED;
  private static final Long DEFAULT_EXPIRATION_TIMESTAMP_MILLIS = 0L;

  @NonNull String actorId;
  @NonNull String entityId;
  @NonNull List<String> ipAddresses;
  @NonNull Status status;
  StatusChangeSource statusChangeSource;
  StatusChangeDetails statusChangeDetails;
  Long expirationTimestampMillis;

  private ActorStatusDetails(Map<String, Value> actorFieldsMap) {
    this(
        parseActorId(actorFieldsMap),
        parseEntityId(actorFieldsMap),
        parseIpAddresses(actorFieldsMap),
        parseStatus(actorFieldsMap),
        parseStatusChangeSource(actorFieldsMap),
        parseStatusChangeDetails(actorFieldsMap),
        parseExpirationTimestampMillis(actorFieldsMap));
  }

  static Optional<ActorStatusDetails> getActorStatusDetails(Struct actor) {
    try {
      Map<String, Value> actorFieldsMap = actor.getFieldsMap();
      return Optional.of(new ActorStatusDetails(actorFieldsMap));
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

  private static StatusChangeSource parseStatusChangeSource(Map<String, Value> actorFieldsMap) {
    try {
      return StatusChangeSourceConverter.convert(
          actorFieldsMap.get(ACTOR_FIELD_STATUS_CHANGE_SOURCE.name()).getStringValue());
    } catch (Exception e) {
      LOGGER.warn(
          "Cannot parse actor status change source from actor-query map - {}, error - ",
          actorFieldsMap,
          e);
      return DEFAULT_STATUS_CHANGE_SOURCE;
    }
  }

  private static StatusChangeDetails parseStatusChangeDetails(Map<String, Value> actorFieldsMap) {
    Builder statusChangeDetailsBuilder = StatusChangeDetails.newBuilder();
    try {
      Value statusChangeDetailsValue = actorFieldsMap.get(ACTOR_FIELD_STATUS_CHANGE_DETAILS.name());
      if (statusChangeDetailsValue != null && statusChangeDetailsValue.hasStructValue()) {
        switch (parseStatusChangeSource(actorFieldsMap)) {
          case STATUS_CHANGE_SOURCE_RATE_LIMIT:
            RateLimitDetails.Builder rateLimitDetailsValue = RateLimitDetails.newBuilder();
            ConfigProtoConverter.mergeFromValue(statusChangeDetailsValue, rateLimitDetailsValue);
            statusChangeDetailsBuilder.setRateLimitDetails(rateLimitDetailsValue.build());
            break;
          case STATUS_CHANGE_SOURCE_MALICIOUS_SOURCES:
            MaliciousSourcesDetails.Builder maliciousSourcesDetailsValue =
                MaliciousSourcesDetails.newBuilder();
            ConfigProtoConverter.mergeFromValue(
                statusChangeDetailsValue, maliciousSourcesDetailsValue);
            statusChangeDetailsBuilder.setMaliciousSourcesDetails(
                maliciousSourcesDetailsValue.build());
            break;
          default:
            ConfigProtoConverter.mergeFromValue(
                statusChangeDetailsValue, statusChangeDetailsBuilder);
        }
      }
    } catch (Exception e) {
      LOGGER.warn(
          "Cannot parse status-change-value from actor-query struct - {} to proto StatusChangeDetails, error - ",
          actorFieldsMap,
          e);
    }
    return statusChangeDetailsBuilder.build();
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
