package ai.traceable.saved.query.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.saved.query.config.service.store.SavedQueryConfigStore;
import ai.traceable.saved.query.config.service.store.SavedQueryStoreManager;
import ai.traceable.saved.query.config.service.v1.CreateSavedQueryRequest;
import ai.traceable.saved.query.config.service.v1.DeleteSavedQueryRequest;
import ai.traceable.saved.query.config.service.v1.GetSavedQueriesFilter;
import ai.traceable.saved.query.config.service.v1.GetSavedQueriesRequest;
import ai.traceable.saved.query.config.service.v1.GetSavedQueriesResponse;
import ai.traceable.saved.query.config.service.v1.QueryClauses;
import ai.traceable.saved.query.config.service.v1.SavedQuery;
import ai.traceable.saved.query.config.service.v1.SavedQueryServiceGrpc;
import ai.traceable.saved.query.config.service.v1.UpdateSavedQueryRequest;
import ai.traceable.saved.query.config.service.validation.SavedQueryRequestValidator;
import com.google.protobuf.Timestamp;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SavedQueryConfigServiceImplTest {

  private static final String UUID_1 = "uuid-1";
  private static final String TRACES_SCOPE = "traces";
  private static final String QUERY_NAME_1 = "query-1";
  private static final String QUERY_NAME_2 = "query-2";
  private static final String SEC_EVENTS_SCOPE = "security-events";

  private SavedQueryServiceGrpc.SavedQueryServiceBlockingStub savedQueryServiceBlockingStub;
  private MockGenericConfigService mockGenericConfigService;
  @Mock private ConfigChangeEventGenerator eventGenerator;
  @Mock private TimestampConverter timestampConverter;
  @Mock private UuidGenerator uuidGenerator;

  @BeforeEach
  void beforeEach() {
    this.mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    this.mockGenericConfigService
        .addService(
            new SavedQueryConfigServiceImpl(
                new SavedQueryStoreManager(
                    new SavedQueryConfigStore(genericStub, eventGenerator),
                    this.timestampConverter,
                    uuidGenerator),
                new SavedQueryRequestValidator()))
        .start();

    this.savedQueryServiceBlockingStub =
        SavedQueryServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    when(this.timestampConverter.convert(any()))
        .thenReturn(Timestamp.newBuilder().setSeconds(100).build());
    when(uuidGenerator.generateRandomId()).thenReturn(UUID_1);
  }

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  @Test
  void testSavedQueryCRUD() {
    QueryClauses queryClauses1 =
        QueryClauses.newBuilder()
            .addSelection("column:count(calls)")
            .addFilter("ipIsBot_eq_true")
            .addGroupBy("apiId")
            .addOrderBy("column:count(calls):desc")
            .setGroupLimit("5")
            .setInterval("1m")
            .setIncludeOthersGroups("true")
            .build();
    RequestContext requestContext = buildRequestContext();
    SavedQuery createdSavedQuery =
        requestContext.call(
            () ->
                this.savedQueryServiceBlockingStub
                    .createSavedQuery(
                        CreateSavedQueryRequest.newBuilder()
                            .setName(QUERY_NAME_1)
                            .setScope(TRACES_SCOPE)
                            .setQueryClauses(queryClauses1)
                            .build())
                    .getSavedQuery());

    assertEquals(UUID_1, createdSavedQuery.getId());
    assertEquals(QUERY_NAME_1, createdSavedQuery.getName());
    assertEquals(TRACES_SCOPE, createdSavedQuery.getScope());
    assertEquals(queryClauses1, createdSavedQuery.getQueryClauses());
    assertEquals("Favian.Reynolds@example.com", createdSavedQuery.getCreatedByUserId());
    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(expectedTimestamp, createdSavedQuery.getCreatedTimestamp());
    assertEquals(expectedTimestamp, createdSavedQuery.getUpdatedTimestamp());

    QueryClauses queryClauses2 =
        QueryClauses.newBuilder()
            .addSelection("column:discountcount(userId)")
            .addFilter("apidId_eq_fcgve45678ef")
            .addGroupBy("country")
            .addOrderBy("column:discountcount(userId):asc")
            .setGroupLimit("100")
            .setInterval("15m")
            .setIncludeOthersGroups("false")
            .build();
    SavedQuery updatedSavedQuery =
        requestContext.call(
            () ->
                this.savedQueryServiceBlockingStub
                    .updateSavedQuery(
                        UpdateSavedQueryRequest.newBuilder()
                            .setId(UUID_1)
                            .setName(QUERY_NAME_2)
                            .setQueryClauses(queryClauses2)
                            .build())
                    .getSavedQuery());

    assertEquals(QUERY_NAME_2, updatedSavedQuery.getName());
    assertEquals(TRACES_SCOPE, updatedSavedQuery.getScope());
    assertEquals(queryClauses2, updatedSavedQuery.getQueryClauses());

    GetSavedQueriesResponse getSavedQueriesResponse =
        requestContext.call(
            () ->
                this.savedQueryServiceBlockingStub.getSavedQueries(
                    GetSavedQueriesRequest.newBuilder()
                        .setFilter(
                            GetSavedQueriesFilter.newBuilder().setScope(TRACES_SCOPE).build())
                        .build()));
    assertEquals(1, getSavedQueriesResponse.getSavedQueriesCount());
    assertEquals(QUERY_NAME_2, updatedSavedQuery.getName());
    assertEquals(TRACES_SCOPE, updatedSavedQuery.getScope());
    assertEquals(queryClauses2, updatedSavedQuery.getQueryClauses());

    getSavedQueriesResponse =
        requestContext.call(
            () ->
                this.savedQueryServiceBlockingStub.getSavedQueries(
                    GetSavedQueriesRequest.newBuilder()
                        .setFilter(GetSavedQueriesFilter.newBuilder().build())
                        .build()));
    assertEquals(1, getSavedQueriesResponse.getSavedQueriesCount());
    assertEquals(QUERY_NAME_2, updatedSavedQuery.getName());
    assertEquals(TRACES_SCOPE, updatedSavedQuery.getScope());
    assertEquals(queryClauses2, updatedSavedQuery.getQueryClauses());

    getSavedQueriesResponse =
        requestContext.call(
            () ->
                this.savedQueryServiceBlockingStub.getSavedQueries(
                    GetSavedQueriesRequest.newBuilder()
                        .setFilter(
                            GetSavedQueriesFilter.newBuilder().setScope(SEC_EVENTS_SCOPE).build())
                        .build()));
    assertEquals(0, getSavedQueriesResponse.getSavedQueriesCount());

    requestContext.call(
        () ->
            this.savedQueryServiceBlockingStub
                .withCallCredentials(
                    RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get())
                .deleteSavedQuery(DeleteSavedQueryRequest.newBuilder().setId(UUID_1).build()));

    getSavedQueriesResponse =
        requestContext.call(
            () ->
                this.savedQueryServiceBlockingStub.getSavedQueries(
                    GetSavedQueriesRequest.newBuilder().build()));
    assertEquals(0, getSavedQueriesResponse.getSavedQueriesCount());
  }

  private static RequestContext buildRequestContext() {
    return RequestContext.forTenantId("t1")
        .put(
            "authorization",
            "Bearer eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJGYXZpYW4uUmV5bm9sZHNAZXhhbXBsZS5jb20iLCJyb2xlIjoidXNlciIsImlhdCI6MTY4NDM0NzcxOSwiZXhwIjoxNjg0OTUyNTE5fQ.cjyK-u3j7K5sHNSf7SG0oGe6xKCV0jOTm9kqN68JmFkdxUx5Dvd1WuF7Zg7uM-8sNBgxmtPg-bxJ2Ddnmb4Fd3IhSe1L-1dbZi3-dJldlK6i9m3O5pp3ivoQvobk1mfh2jCdbZ3tLcLF7t5OLDnqK_9COQ4plTMlmwX8Jr1L8C1kfzYHfHa91Rc9lDGjSJnxaAwfXAgqBhOSZCdX-EgYRyINnF6elLibLnL8J_PP50RLctNnZuEznImvPXQ6twl8A6JJmCkJYUp0HP9mdBJIz6RcuHYt0QU5avZprKQ6p_23WhuNzSvPRSjD0l9RGR8uQHU8LWVuu9i0L8Sfyr3TrA");
  }
}
