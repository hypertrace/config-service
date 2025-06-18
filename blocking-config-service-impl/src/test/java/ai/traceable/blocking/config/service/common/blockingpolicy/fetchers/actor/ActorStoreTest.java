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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor.config.ActorServiceConfig;
import ai.traceable.platform.actor.v1.ActorField;
import ai.traceable.platform.actor.v1.ActorServiceGrpc.ActorServiceBlockingStub;
import ai.traceable.platform.actor.v1.ActorsResultSet;
import ai.traceable.platform.actor.v1.Filter;
import ai.traceable.platform.actor.v1.LiteralConstant;
import ai.traceable.platform.actor.v1.LogicalFilterExpression;
import ai.traceable.platform.actor.v1.LogicalOperator;
import ai.traceable.platform.actor.v1.QueryActorsRequest;
import ai.traceable.platform.actor.v1.QueryActorsResponse;
import ai.traceable.platform.actor.v1.RateLimitCategory;
import ai.traceable.platform.actor.v1.RateLimitDetails;
import ai.traceable.platform.actor.v1.RelationalFilterExpression;
import ai.traceable.platform.actor.v1.RelationalOperator;
import ai.traceable.platform.actor.v1.Selection;
import ai.traceable.platform.actor.v1.Status;
import ai.traceable.platform.actor.v1.StatusChangeDetails;
import ai.traceable.platform.actor.v1.StatusChangeSource;
import ai.traceable.platform.actor.v1.converter.StatusChangeSourceConverter;
import ai.traceable.platform.actor.v1.converter.StatusConverter;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Struct.Builder;
import com.google.protobuf.Value;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActorStoreTest {
  private static final String TENANT_ID = "tenant-id";
  private static final String ENVIRONMENT_ID = "environment-id";
  private static final Long TEST_TIMESTAMP = 10000L;
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private static final int ACTOR_LIMIT = 5;
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

  private ActorServiceBlockingStub actorServiceBlockingStub;
  private ActorStore actorStore;
  private Filter requestFilterWithEnvironment;

  @BeforeEach
  void setUp() {
    Clock clock = mock(Clock.class, RETURNS_DEEP_STUBS);
    actorServiceBlockingStub = mock(ActorServiceBlockingStub.class, RETURNS_DEEP_STUBS);
    ActorServiceConfig actorServiceConfig = mock(ActorServiceConfig.class);
    doReturn(Duration.ofSeconds(30)).when(actorServiceConfig).getCallTimeoutDuration();
    doReturn(5).when(actorServiceConfig).getMaxNumberOfActors();

    when(clock.instant().toEpochMilli()).thenReturn(TEST_TIMESTAMP);

    actorStore = new ActorStore(actorServiceBlockingStub, actorServiceConfig, clock);

    requestFilterWithEnvironment =
        Filter.newBuilder()
            .setLogicalFilter(
                LogicalFilterExpression.newBuilder()
                    .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                    .addOperands(EXPECTED_ACTORS_STATUS_FILTER)
                    .addOperands(EXPECTED_ACTIVE_ACTOR_FILTER)
                    .addOperands(EXPECTED_ENVIRONMENT_FILTER))
            .build();
  }

  @Test
  void getActorQueryResponseEmpty() {
    when(actorServiceBlockingStub
            .withDeadlineAfter(30000L, TimeUnit.MILLISECONDS)
            .queryActors(
                QueryActorsRequest.newBuilder()
                    .setFilter(requestFilterWithEnvironment)
                    .addAllSelection(SELECTION_LIST)
                    .setLimit(ACTOR_LIMIT)
                    .build()))
        .thenReturn(QueryActorsResponse.getDefaultInstance());

    List<ActorStatusDetails> response =
        actorStore.getActiveThreatActors(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    assertEquals(0, response.size());
  }

  @Test
  void getActorQueryResponseNormal() {
    when(actorServiceBlockingStub
            .withDeadlineAfter(30000L, TimeUnit.MILLISECONDS)
            .queryActors(
                QueryActorsRequest.newBuilder()
                    .setFilter(requestFilterWithEnvironment)
                    .addAllSelection(SELECTION_LIST)
                    .setLimit(ACTOR_LIMIT)
                    .build()))
        .thenReturn(createQueryActorsResponse(List.of(1, 2), false));

    List<ActorStatusDetails> response =
        actorStore.getActiveThreatActors(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    assertEquals(2, response.size());
    assertEquals(createActorStatusDetails(1, false), response.get(0));
    assertEquals(createActorStatusDetails(2, false), response.get(1));
  }

  @Test
  void getActorQueryResponseMissing() {
    // This test checks the handling of cases when there are fields missing in the query response
    when(actorServiceBlockingStub
            .withDeadlineAfter(30000L, TimeUnit.MILLISECONDS)
            .queryActors(
                QueryActorsRequest.newBuilder()
                    .setFilter(requestFilterWithEnvironment)
                    .addAllSelection(SELECTION_LIST)
                    .setLimit(ACTOR_LIMIT)
                    .build()))
        .thenReturn(createQueryActorsResponse(List.of(1, 2), true));

    List<ActorStatusDetails> response =
        actorStore.getActiveThreatActors(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    // If any core field is missing the actor is skipped
    // If any optional field is missing actor is still there with default values
    assertEquals(1, response.size());
    assertEquals(createActorStatusDetails(2, true), response.get(0));
  }

  private static QueryActorsResponse createQueryActorsResponse(
      List<Integer> ids, Boolean missingFields) {
    return QueryActorsResponse.newBuilder()
        .setResultSet(
            ActorsResultSet.newBuilder()
                .addAllRows(
                    ids.stream()
                        .map(id -> createRow(id, missingFields))
                        .collect(Collectors.toUnmodifiableList())))
        .build();
  }

  private static Struct createRow(int n, Boolean missingFields) {
    String id = String.valueOf(n);
    List<String> ipAddresses = List.of("1.2.3." + id, "11.22.33." + id);
    StatusChangeSource statusChangeSource =
        (n % 2 == 0)
            ? StatusChangeSource.STATUS_CHANGE_SOURCE_RATE_LIMIT
            : StatusChangeSource.STATUS_CHANGE_SOURCE_SYSTEM;
    Status status = (n % 2 == 0) ? STATUS_ALWAYS_DENIED : STATUS_ALWAYS_ALLOWED;
    Builder rowBuilder =
        Struct.newBuilder()
            .putFields(
                ACTOR_FIELD_ENTITY_ID.name(),
                Value.newBuilder().setStringValue("entity-" + id).build());

    if (!missingFields || n % 2 == 0) {
      rowBuilder.putFields(
          ACTOR_FIELD_ACTOR_ID.name(), Value.newBuilder().setStringValue("actor-" + id).build());
      rowBuilder.putFields(
          ACTOR_FIELD_STATUS.name(),
          Value.newBuilder().setStringValue(StatusConverter.convert(status).name()).build());
      rowBuilder.putFields(
          ACTOR_FIELD_IP_ADDRESSES.name(),
          Value.newBuilder()
              .setListValue(
                  ListValue.newBuilder()
                      .addAllValues(
                          ipAddresses.stream()
                              .map(
                                  stringVal -> Value.newBuilder().setStringValue(stringVal).build())
                              .collect(Collectors.toUnmodifiableList())))
              .build());

      if (!missingFields) {
        rowBuilder.putFields(
            ACTOR_FIELD_STATUS_CHANGE_SOURCE.name(),
            Value.newBuilder()
                .setStringValue(
                    StatusChangeSourceConverter.convert(statusChangeSource).get().name())
                .build());
        rowBuilder.putFields(
            ACTOR_FIELD_STATUS_EXPIRY_TIMESTAMP.name(),
            Value.newBuilder().setNumberValue(1000 + n).build());
        if (n % 2 == 0) {
          Value statusChangeDetailsValue = null;
          try {
            statusChangeDetailsValue =
                ConfigProtoConverter.convertToValue(
                    RateLimitDetails.newBuilder()
                        .setRuleId("rate-limit-" + id)
                        .setRuleName("Name: Rule - " + id)
                        .setRuleCategory(RateLimitCategory.RATE_LIMIT_CATEGORY_DATA_EXFILTRATION)
                        .setRuleSeverity("LOW")
                        .build());
          } catch (Exception ignored) {
          }
          assert statusChangeDetailsValue != null;
          rowBuilder.putFields(ACTOR_FIELD_STATUS_CHANGE_DETAILS.name(), statusChangeDetailsValue);
        }
      } else {
        rowBuilder.putFields(ACTOR_FIELD_STATUS_CHANGE_SOURCE.name(), Value.newBuilder().build());
        rowBuilder.putFields(ACTOR_FIELD_STATUS_CHANGE_DETAILS.name(), Value.newBuilder().build());
        rowBuilder.putFields(
            ACTOR_FIELD_STATUS_EXPIRY_TIMESTAMP.name(), Value.newBuilder().build());
      }
    }
    rowBuilder.putFields(
        ACTOR_FIELD_BLOCKED_EVENT_LABELS.name(),
        Value.newBuilder()
            .setStructValue(
                Struct.newBuilder()
                    .putFields("key", Value.newBuilder().setStringValue("value").build()))
            .build());
    return rowBuilder.build();
  }

  private static ActorStatusDetails createActorStatusDetails(int n, boolean missingFields) {
    String id = String.valueOf(n);
    String actorId = "actor-" + id;
    List<String> ipAddresses = List.of("1.2.3." + id, "11.22.33." + id);
    StatusChangeSource statusChangeSource =
        (n % 2 == 0)
            ? StatusChangeSource.STATUS_CHANGE_SOURCE_RATE_LIMIT
            : StatusChangeSource.STATUS_CHANGE_SOURCE_SYSTEM;
    Status status = (n % 2 == 0) ? STATUS_ALWAYS_DENIED : STATUS_ALWAYS_ALLOWED;
    StatusChangeDetails statusChangeDetails =
        (n % 2 == 0)
            ? StatusChangeDetails.newBuilder()
                .setRateLimitDetails(
                    RateLimitDetails.newBuilder()
                        .setRuleId("rate-limit-" + id)
                        .setRuleName("Name: Rule - " + id)
                        .setRuleCategory(RateLimitCategory.RATE_LIMIT_CATEGORY_DATA_EXFILTRATION)
                        .setRuleSeverity("LOW"))
                .build()
            : StatusChangeDetails.getDefaultInstance();
    long expirationTimestampMillis = 1000L + n;

    if (missingFields) {
      statusChangeSource = StatusChangeSource.STATUS_CHANGE_SOURCE_UNSPECIFIED;
      statusChangeDetails = StatusChangeDetails.getDefaultInstance();
      expirationTimestampMillis = 0L;
    }

    return new ActorStatusDetails(
        actorId,
        "entity-" + id,
        ipAddresses,
        status,
        statusChangeSource,
        statusChangeDetails,
        expirationTimestampMillis,
        Map.of("key", "value"));
  }

  private static final Filter EXPECTED_ACTORS_STATUS_FILTER =
      Filter.newBuilder()
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
                                          .addValues(
                                              Value.newBuilder()
                                                  .setStringValue(
                                                      StatusConverter.convert(STATUS_SNOOZED)
                                                          .name()))
                                          .addValues(
                                              Value.newBuilder()
                                                  .setStringValue(
                                                      StatusConverter.convert(STATUS_SUSPENDED)
                                                          .name()))
                                          .addValues(
                                              Value.newBuilder()
                                                  .setStringValue(
                                                      StatusConverter.convert(STATUS_ALWAYS_ALLOWED)
                                                          .name()))
                                          .addValues(
                                              Value.newBuilder()
                                                  .setStringValue(
                                                      StatusConverter.convert(STATUS_ALWAYS_DENIED)
                                                          .name()))))))
          .build();
  private static final Filter EXPECTED_ACTIVE_ACTOR_FILTER =
      Filter.newBuilder()
          .setLogicalFilter(
              LogicalFilterExpression.newBuilder()
                  .setOperator(LogicalOperator.LOGICAL_OPERATOR_OR)
                  .addOperands(
                      Filter.newBuilder()
                          .setRelationalFilter(
                              RelationalFilterExpression.newBuilder()
                                  .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQ)
                                  .setLeftOperand(ActorField.ACTOR_FIELD_STATUS_EXPIRY_TIMESTAMP)
                                  .setLiteralRightOperand(
                                      LiteralConstant.newBuilder()
                                          .setValue(Value.newBuilder().setNumberValue(0)))))
                  .addOperands(
                      Filter.newBuilder()
                          .setRelationalFilter(
                              RelationalFilterExpression.newBuilder()
                                  .setOperator(RelationalOperator.RELATIONAL_OPERATOR_GT)
                                  .setLeftOperand(ActorField.ACTOR_FIELD_STATUS_EXPIRY_TIMESTAMP)
                                  .setLiteralRightOperand(
                                      LiteralConstant.newBuilder()
                                          .setValue(
                                              Value.newBuilder().setNumberValue(TEST_TIMESTAMP))))))
          .build();

  private static final Filter EXPECTED_ENVIRONMENT_FILTER =
      Filter.newBuilder()
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
                                              Value.newBuilder().setStringValue(ENVIRONMENT_ID)))))
                  .addOperands(
                      Filter.newBuilder()
                          .setRelationalFilter(
                              RelationalFilterExpression.newBuilder()
                                  .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQ)
                                  .setLeftOperand(ACTOR_FIELD_ENVIRONMENT)
                                  .setLiteralRightOperand(
                                      LiteralConstant.newBuilder()
                                          .setValue(Value.newBuilder().setStringValue(""))))))
          .build();
}
