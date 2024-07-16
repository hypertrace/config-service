package ai.traceable.saved.query.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.saved.query.config.service.store.DeletedSavedQueryConfigStore;
import ai.traceable.saved.query.config.service.store.SavedQueryConfigStore;
import ai.traceable.saved.query.config.service.store.SavedQueryStoreManager;
import ai.traceable.saved.query.config.service.v1.CreateSavedQueryRequest;
import ai.traceable.saved.query.config.service.v1.DeleteSavedQueryRequest;
import ai.traceable.saved.query.config.service.v1.GetAllSavedQueryUsersRequest;
import ai.traceable.saved.query.config.service.v1.GetAllSavedQueryUsersResponse;
import ai.traceable.saved.query.config.service.v1.GetSavedQueriesFilter;
import ai.traceable.saved.query.config.service.v1.GetSavedQueriesRequest;
import ai.traceable.saved.query.config.service.v1.GetSavedQueriesResponse;
import ai.traceable.saved.query.config.service.v1.QueryClauses;
import ai.traceable.saved.query.config.service.v1.SavedQuery;
import ai.traceable.saved.query.config.service.v1.SavedQueryServiceGrpc;
import ai.traceable.saved.query.config.service.v1.UpdateSavedQueryRequest;
import ai.traceable.saved.query.config.service.v1.User;
import ai.traceable.saved.query.config.service.validation.SavedQueryRequestValidator;
import com.google.protobuf.Timestamp;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SavedQueryConfigServiceImplTest {

  private static final String UUID_1 = "uuid-1";
  private static final String TRACES_SCOPE = "endpoint-traces";
  private static final String QUERY_NAME_1 = "query-1";
  private static final String QUERY_NAME_2 = "query-2";
  private static final String SEC_EVENTS_SCOPE = "security-events";
  private static final String SAVED_QUERY_CONFIG_SERVICE = "saved.query.config.service";

  private SavedQueryServiceGrpc.SavedQueryServiceBlockingStub savedQueryServiceBlockingStub;
  private MockGenericConfigService mockGenericConfigService;
  @Mock private ConfigChangeEventGenerator eventGenerator;
  @Mock private TimestampConverter timestampConverter;
  @Mock private UuidGenerator uuidGenerator;
  @Mock private Config mockConfig;

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  @Test
  void testSavedQueryCRUD() {
    String jsonString =
        "\"default\": {\n"
            + "    \"saved\": {\n"
            + "      \"queries\": [\n"
            + "      ]\n"
            + "    }\n"
            + "  }";
    when(mockConfig.getConfig(SAVED_QUERY_CONFIG_SERVICE))
        .thenReturn(ConfigFactory.parseString(jsonString));
    this.mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    this.mockGenericConfigService
        .addService(
            new SavedQueryConfigServiceImpl(
                new SavedQueryStoreManager(
                    new DeletedSavedQueryConfigStore(genericStub, eventGenerator),
                    new DefaultSavedQueryConfig(mockConfig),
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
    QueryClauses queryClauses1 =
        QueryClauses.newBuilder()
            .addSelection("column:count(calls)")
            .addFilter("ipIsBot_eq_true")
            .addGroupBy("apiId")
            .addOrderBy("column:count(calls):desc")
            .setGroupLimit("5")
            .setInterval("1m")
            .setIncludeOthersGroups("true")
            .setTimeRange("1w")
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
    assertEquals("google-oauth2|107056790216294270188", createdSavedQuery.getCreatedByUserId());
    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(expectedTimestamp, createdSavedQuery.getCreatedTimestamp());
    assertEquals(expectedTimestamp, createdSavedQuery.getUpdatedTimestamp());

    GetAllSavedQueryUsersResponse getAllSavedQueryUsersResponse =
        requestContext.call(
            () ->
                this.savedQueryServiceBlockingStub.getAllSavedQueryUsers(
                    GetAllSavedQueryUsersRequest.newBuilder().build()));
    assertEquals(1, getAllSavedQueryUsersResponse.getUsersCount());

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

  @Test
  void testDefaultSavedQueryCRUD() {
    String uuid = "b592ec8e-0de6-4fd4-96aa-0449ef995192";
    String name = "Top APIs seeing bot traffic with poor IP reputation from Data Center";
    String jsonString =
        "\"default\": {\n"
            + "    \"saved\": {\n"
            + "      \"queries\": [\n"
            + "        {\n"
            + "          \"id\": \""
            + uuid
            + "\",\n"
            + "          \"name\": \""
            + name
            + "\",\n"
            + "          \"scope\": \"endpoint-traces\",\n"
            + "          \"query_clauses\": {\n"
            + "            \"selection\": [\n"
            + "              \"column:count(calls)\"\n"
            + "            ],\n"
            + "            \"filter\": [\n"
            + "              \"ipIsBot_eq_true\",\n"
            + "              \"ipReputationLevel_eq_HIGH\",\n"
            + "              \"ipConnectionType_eq_Data%20Center\"\n"
            + "            ],\n"
            + "            \"group_by\": [\n"
            + "              \"apiName\"\n"
            + "            ],\n"
            + "            \"order_by\": [],\n"
            + "            \"group_limit\": \"5\",\n"
            + "            \"interval\": \"5m\"\n"
            + "          }\n"
            + "          \"author\": {\n"
            + "            \"id\": \"ce16162f-cc9b-4a2c-9374-172566d6d393\",\n"
            + "            \"name\": \"Traceable\",\n"
            + "            \"email_id\": \"raj.patel@traceable.ai\"\n"
            + "          }\n"
            + "        }\n"
            + "      ]\n"
            + "    }\n"
            + "  }";

    QueryClauses queryClauses1 =
        QueryClauses.newBuilder()
            .addSelection("column:count(calls)")
            .addAllFilter(
                List.of(
                    "ipIsBot_eq_true",
                    "ipReputationLevel_eq_HIGH",
                    "ipConnectionType_eq_Data%20Center"))
            .addAllGroupBy(List.of("apiName"))
            .setInterval("5m")
            .setGroupLimit("5")
            .build();
    User user =
        User.newBuilder()
            .setId("ce16162f-cc9b-4a2c-9374-172566d6d393")
            .setName("Traceable")
            .setEmailId("raj.patel@traceable.ai")
            .build();
    SavedQuery expectedSavedQuery =
        SavedQuery.newBuilder()
            .setId(uuid)
            .setName(name)
            .setScope(TRACES_SCOPE)
            .setQueryClauses(queryClauses1)
            .setAuthor(user)
            .build();
    Config savedQueryConfig = ConfigFactory.parseString(jsonString);
    when(mockConfig.getConfig(SAVED_QUERY_CONFIG_SERVICE)).thenReturn(savedQueryConfig);

    this.mockGenericConfigService = new MockGenericConfigService().mockGetAll();
    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    this.mockGenericConfigService
        .addService(
            new SavedQueryConfigServiceImpl(
                new SavedQueryStoreManager(
                    new DeletedSavedQueryConfigStore(genericStub, eventGenerator),
                    new DefaultSavedQueryConfig(mockConfig),
                    new SavedQueryConfigStore(genericStub, eventGenerator),
                    this.timestampConverter,
                    uuidGenerator),
                new SavedQueryRequestValidator()))
        .start();

    this.savedQueryServiceBlockingStub =
        SavedQueryServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    RequestContext requestContext = buildRequestContext();
    GetSavedQueriesResponse getSavedQueriesResponse =
        requestContext.call(
            () ->
                this.savedQueryServiceBlockingStub.getSavedQueries(
                    GetSavedQueriesRequest.newBuilder()
                        .setFilter(
                            GetSavedQueriesFilter.newBuilder().setScope(TRACES_SCOPE).build())
                        .build()));
    assertEquals(1, getSavedQueriesResponse.getSavedQueriesCount());
    assertEquals(expectedSavedQuery, getSavedQueriesResponse.getSavedQueries(0));

    getSavedQueriesResponse =
        requestContext.call(
            () ->
                this.savedQueryServiceBlockingStub.getSavedQueries(
                    GetSavedQueriesRequest.newBuilder()
                        .setFilter(
                            GetSavedQueriesFilter.newBuilder().setScope(SEC_EVENTS_SCOPE).build())
                        .build()));
    assertEquals(0, getSavedQueriesResponse.getSavedQueriesCount());

    getSavedQueriesResponse =
        requestContext.call(
            () ->
                this.savedQueryServiceBlockingStub.getSavedQueries(
                    GetSavedQueriesRequest.newBuilder()
                        .setFilter(GetSavedQueriesFilter.newBuilder().build())
                        .build()));
    assertEquals(1, getSavedQueriesResponse.getSavedQueriesCount());

    QueryClauses updatedQueryClause =
        QueryClauses.newBuilder(queryClauses1).setGroupLimit("100").build();
    expectedSavedQuery =
        SavedQuery.newBuilder(expectedSavedQuery)
            .setQueryClauses(updatedQueryClause)
            .setCreatedTimestamp(Timestamp.newBuilder().setSeconds(1697479787).build())
            .setUpdatedTimestamp(Timestamp.newBuilder().setSeconds(1697479787).build())
            .build();

    StatusRuntimeException editException =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                requestContext.call(
                    () ->
                        this.savedQueryServiceBlockingStub
                            .updateSavedQuery(
                                UpdateSavedQueryRequest.newBuilder()
                                    .setId(uuid)
                                    .setName(name)
                                    .setQueryClauses(updatedQueryClause)
                                    .build())
                            .getSavedQuery()));
    assertEquals(Status.UNIMPLEMENTED.getCode(), editException.getStatus().getCode());

    StatusRuntimeException deleteException =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                requestContext.call(
                    () ->
                        this.savedQueryServiceBlockingStub
                            .withCallCredentials(
                                RequestContextClientCallCredsProviderFactory
                                    .getClientCallCredsProvider()
                                    .get())
                            .deleteSavedQuery(
                                DeleteSavedQueryRequest.newBuilder().setId(uuid).build())));
    assertEquals(Status.UNIMPLEMENTED.getCode(), deleteException.getStatus().getCode());

    getSavedQueriesResponse =
        requestContext.call(
            () ->
                this.savedQueryServiceBlockingStub.getSavedQueries(
                    GetSavedQueriesRequest.newBuilder()
                        .setFilter(
                            GetSavedQueriesFilter.newBuilder()
                                .addAllEmailIds(List.of("raj.patel@traceable.ai"))
                                .build())
                        .build()));
    assertEquals(1, getSavedQueriesResponse.getSavedQueriesCount());
  }

  private static RequestContext buildRequestContext() {
    return RequestContext.forTenantId("t1")
        .put(
            "authorization",
            "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InpMOFcyU3lTTWk0Ujh4MUM4b1NjbiJ9.eyJodHRwczovL3RyYWNlYWJsZS5haS9yb2xlc192MiI6WyJ0cmFjZWFibGUiXSwiaHR0cHM6Ly90cmFjZWFibGUuYWkvY3VzdG9tZXJfaWQiOiIzZTc2MTg3OS1jNzdiLTRkOGYtYTA3NS02MmZmMjhlOGZhOGEiLCJodHRwczovL3RyYWNlYWJsZS5haS9yb2xlcyI6WyJ0cmFjZWFibGUiXSwiaHR0cHM6Ly90cmFjZWFibGUuYWkvanRpIjoiODJjYTllYzctODkwMS00YTJhLWI2NWYtZTA5Y2IwOGQwY2Y0IiwiaHR0cHM6Ly90cmFjZWFibGUuYWkvcmljaF9yb2xlcyI6W3siZW52cyI6W10sImlkIjoidHJhY2VhYmxlIn1dLCJnaXZlbl9uYW1lIjoiUmFqIiwiZmFtaWx5X25hbWUiOiJQYXRlbCIsIm5pY2tuYW1lIjoicmFqLnBhdGVsIiwibmFtZSI6IlJhaiBQYXRlbCIsInBpY3R1cmUiOiJodHRwczovL2xoMy5nb29nbGV1c2VyY29udGVudC5jb20vYS9BQ2c4b2NLRXdDZEVqajNoUVlsWk9JMHBTaVNyVWxKeFlKQUJCcjBHRW5vLXR6R21QUHJxOXc9czk2LWMiLCJ1cGRhdGVkX2F0IjoiMjAyNC0wNy0wNFQxNDowMDo0OS40OTBaIiwiZW1haWwiOiJyYWoucGF0ZWxAdHJhY2VhYmxlLmFpIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImlzcyI6Imh0dHBzOi8vdHJhY2VhYmxlLXNhbmRib3gudXMuYXV0aDAuY29tLyIsImF1ZCI6IjY2bHNCNWFnaGxHemFNbnFubkdubUFkc0E3WHh0d1d0IiwiaWF0IjoxNzIwMTAxNjUyLCJleHAiOjE3MjAxMzc2NTIsInN1YiI6Imdvb2dsZS1vYXV0aDJ8MTA3MDU2NzkwMjE2Mjk0MjcwMTg4Iiwic2lkIjoibllpTWRDMDJOOERQSk5DQjFlNUVtVkFGRG5RNm9ULWwifQ.xOCCsRsSH6gb-zPdnfdr0goOQeIk66M_HruaVulZ1xciGTUjvW9b7tl113xjkTxvHYghXlEq5XkcU2K_MTIw-Y5jtCtvxHKTyat3E6KSTIVjl-COhArDmlSMhqefpNxY2ew9VFiODP5vfq1JS6rx-q3anVhWV1X36k5D2Fpd4lASokdrJ0SpyjRF05Bv0hDJmkPjKbOKE3pVNX4WCG377hN9VbAxtITkTteLbHkeEIYH0xH8f95qlpTm9i5xpYttCl6K_GvKOdwws0IKlhntrsfDPQj7T4IyHoCCMtZkPLLNaIyoVk8LI3CC-OEBFypjedN_FG4QOR8Clt7A_MN0Dw");
  }
}
