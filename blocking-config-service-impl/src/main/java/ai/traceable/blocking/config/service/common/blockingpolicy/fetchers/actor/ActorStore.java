package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor;

import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_ACTOR_ID;
import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_BLOCKED_EVENT_LABELS;
import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_ENTITY_ID;
import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_ENVIRONMENT;
import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_IP_ADDRESSES;
import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_STATUS;
import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_STATUS_CHANGE_DETAILS;
import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_STATUS_CHANGE_SOURCE;
import static ai.traceable.platform.actor.v1.ActorField.ACTOR_FIELD_STATUS_EXPIRY_TIMESTAMP;
import static ai.traceable.platform.actor.v1.Status.STATUS_ALWAYS_ALLOWED;
import static ai.traceable.platform.actor.v1.Status.STATUS_ALWAYS_DENIED;
import static ai.traceable.platform.actor.v1.Status.STATUS_SNOOZED;
import static ai.traceable.platform.actor.v1.Status.STATUS_SUSPENDED;

import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor.config.ActorServiceConfig;
import ai.traceable.platform.actor.v1.ActorField;
import ai.traceable.platform.actor.v1.ActorServiceGrpc.ActorServiceBlockingStub;
import ai.traceable.platform.actor.v1.Filter;
import ai.traceable.platform.actor.v1.LiteralConstant;
import ai.traceable.platform.actor.v1.LogicalFilterExpression;
import ai.traceable.platform.actor.v1.LogicalOperator;
import ai.traceable.platform.actor.v1.QueryActorsRequest;
import ai.traceable.platform.actor.v1.QueryActorsResponse;
import ai.traceable.platform.actor.v1.RelationalFilterExpression;
import ai.traceable.platform.actor.v1.RelationalOperator;
import ai.traceable.platform.actor.v1.Selection;
import ai.traceable.platform.actor.v1.Status;
import ai.traceable.platform.actor.v1.converter.StatusConverter;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ActorStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(ActorStore.class);

  private static final List<ActorField> SELECTED_ACTOR_FIELDS =
      ImmutableList.of(
          ACTOR_FIELD_ACTOR_ID,
          ACTOR_FIELD_ENTITY_ID,
          ACTOR_FIELD_IP_ADDRESSES,
          ACTOR_FIELD_STATUS,
          ACTOR_FIELD_STATUS_CHANGE_SOURCE,
          ACTOR_FIELD_STATUS_CHANGE_DETAILS,
          ACTOR_FIELD_STATUS_EXPIRY_TIMESTAMP,
          ACTOR_FIELD_BLOCKED_EVENT_LABELS);

  private static final List<Selection> SELECTION_LIST =
      SELECTED_ACTOR_FIELDS.stream()
          .map(field -> Selection.newBuilder().setFieldSelection(field).build())
          .collect(Collectors.toUnmodifiableList());

  private static final List<Status> STATUS_LIST =
      ImmutableList.of(
          STATUS_SNOOZED, STATUS_SUSPENDED, STATUS_ALWAYS_ALLOWED, STATUS_ALWAYS_DENIED);
  private static final Filter STATUS_FILTER = getActorsByStatusFilter(STATUS_LIST);

  private final ActorServiceBlockingStub actorServiceStub;
  private final Duration callTimeout;
  private final int actorLimit;
  private final Clock clock;

  @Inject
  public ActorStore(
      ActorServiceBlockingStub actorServiceStub,
      ActorServiceConfig actorServiceConfig,
      Clock clock) {
    this.actorServiceStub = actorServiceStub;
    this.clock = clock;
    this.callTimeout = actorServiceConfig.getCallTimeoutDuration();
    this.actorLimit = actorServiceConfig.getMaxNumberOfActors();
  }

  List<ActorStatusDetails> getActiveThreatActors(
      RequestContext requestContext, Optional<String> environmentId) {
    // Get actors with status filter, timestamp filer and environment filter
    List<Filter> filters =
        new ArrayList<>(
            List.of(STATUS_FILTER, getActiveActorFilter(clock.instant().toEpochMilli())));

    // TODO: Add emptyEnvironmentFilter once older agents are migrated to v1.24
    environmentId.ifPresent(id -> filters.add(ActorStore.getEnvironmentFilter(id)));

    QueryActorsResponse queryActorsResponse =
        requestContext.call(
            () ->
                actorServiceStub
                    .withDeadlineAfter(callTimeout.toMillis(), TimeUnit.MILLISECONDS)
                    .queryActors(
                        QueryActorsRequest.newBuilder()
                            .setFilter(
                                Filter.newBuilder()
                                    .setLogicalFilter(
                                        LogicalFilterExpression.newBuilder()
                                            .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                                            .addAllOperands(filters)))
                            .addAllSelection(SELECTION_LIST)
                            .setLimit(actorLimit)
                            .build()));

    LOGGER.debug(
        String.format(
            "For request context - %s, environment - %s, actor query response is - %s",
            requestContext, environmentId, queryActorsResponse));

    return queryActorsResponse.getResultSet().getRowsList().stream()
        .map(ActorStatusDetails::getActorStatusDetails)
        .filter(Objects::nonNull)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toUnmodifiableList());
  }

  private static Filter getActorsByStatusFilter(List<Status> statusList) {
    return Filter.newBuilder()
        .setRelationalFilter(
            RelationalFilterExpression.newBuilder()
                .setOperator(RelationalOperator.RELATIONAL_OPERATOR_IN)
                .setLeftOperand(ACTOR_FIELD_STATUS)
                .setLiteralRightOperand(
                    LiteralConstant.newBuilder()
                        .setValue(
                            Value.newBuilder()
                                .setListValue(
                                    ListValue.newBuilder()
                                        .addAllValues(
                                            statusList.stream()
                                                .map(
                                                    status ->
                                                        Value.newBuilder()
                                                            .setStringValue(
                                                                StatusConverter.convert(status)
                                                                    .name())
                                                            .build())
                                                .collect(Collectors.toUnmodifiableList()))))))
        .build();
  }

  private static Filter getActiveActorFilter(Long timestamp) {
    // Returns a filter which ensures actor.getStatusExpiryTimestamp either is 0 or greater than
    // timestamp
    return Filter.newBuilder()
        .setLogicalFilter(
            LogicalFilterExpression.newBuilder()
                .setOperator(LogicalOperator.LOGICAL_OPERATOR_OR)
                .addOperands(
                    Filter.newBuilder()
                        .setRelationalFilter(
                            RelationalFilterExpression.newBuilder()
                                .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQ)
                                .setLeftOperand(ACTOR_FIELD_STATUS_EXPIRY_TIMESTAMP)
                                .setLiteralRightOperand(
                                    LiteralConstant.newBuilder()
                                        .setValue(Value.newBuilder().setNumberValue(0)))))
                .addOperands(
                    Filter.newBuilder()
                        .setRelationalFilter(
                            RelationalFilterExpression.newBuilder()
                                .setOperator(RelationalOperator.RELATIONAL_OPERATOR_GT)
                                .setLeftOperand(ACTOR_FIELD_STATUS_EXPIRY_TIMESTAMP)
                                .setLiteralRightOperand(
                                    LiteralConstant.newBuilder()
                                        .setValue(Value.newBuilder().setNumberValue(timestamp))))))
        .build();
  }

  private static Filter getEnvironmentFilter(String environmentId) {
    // Returns a filter which ensures actor.environment either is environmentId or empty
    return Filter.newBuilder()
        .setLogicalFilter(
            LogicalFilterExpression.newBuilder()
                .setOperator(LogicalOperator.LOGICAL_OPERATOR_OR)
                .addOperands(
                    Filter.newBuilder()
                        .setRelationalFilter(
                            RelationalFilterExpression.newBuilder()
                                .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQ)
                                .setLeftOperand(ACTOR_FIELD_ENVIRONMENT)
                                .setLiteralRightOperand(
                                    LiteralConstant.newBuilder()
                                        .setValue(
                                            Value.newBuilder().setStringValue(environmentId)))))
                .addOperands(emptyEnvironmentFilter))
        .build();
  }

  private static final Filter emptyEnvironmentFilter =
      Filter.newBuilder()
          .setRelationalFilter(
              RelationalFilterExpression.newBuilder()
                  .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQ)
                  .setLeftOperand(ACTOR_FIELD_ENVIRONMENT)
                  .setLiteralRightOperand(
                      LiteralConstant.newBuilder().setValue(Value.newBuilder().setStringValue(""))))
          .build();
}
