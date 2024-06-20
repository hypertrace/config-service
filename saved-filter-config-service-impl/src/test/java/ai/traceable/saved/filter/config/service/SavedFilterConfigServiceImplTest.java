package ai.traceable.saved.filter.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.saved.filter.config.service.store.SavedFilterConfigStore;
import ai.traceable.saved.filter.config.service.store.SavedFilterStoreManager;
import ai.traceable.saved.filter.config.service.v1.CreateSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.DeleteSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.FilterCriteria;
import ai.traceable.saved.filter.config.service.v1.GetSavedFiltersRequest;
import ai.traceable.saved.filter.config.service.v1.GetSavedFiltersResponse;
import ai.traceable.saved.filter.config.service.v1.PrivateVisibility;
import ai.traceable.saved.filter.config.service.v1.PublicVisibility;
import ai.traceable.saved.filter.config.service.v1.RelationalFilterCondition;
import ai.traceable.saved.filter.config.service.v1.RelationalOperator;
import ai.traceable.saved.filter.config.service.v1.SavedFilter;
import ai.traceable.saved.filter.config.service.v1.SavedFilterServiceGrpc;
import ai.traceable.saved.filter.config.service.v1.UpdateSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.UpdateSavedFilterResponse;
import ai.traceable.saved.filter.config.service.v1.Visibility;
import ai.traceable.saved.filter.config.service.validation.SavedFilterRequestValidator;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.StatusRuntimeException;
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
class SavedFilterConfigServiceImplTest {

  private static final String UUID_1 = "uuid-1";
  private static final String TRACES_SCOPE = "traces";
  private static final String FILTER_NAME_1 = "filter-1";
  private static final String FILTER_NAME_2 = "filter-2";
  private static final String ENDPOINTS_SCOPE = "api-endpoints";
  private static final String SYSTEM_DEFAULT_FILTER_ID = "0433fd25-da97-4cb1-b2f9-1ebb7bb39f21";
  private static final String MOCK_CONFIG =
      "saved.filter.config.service {\n"
          + "  default.saved.filters = [\n"
          + "    {\n"
          + "      \"id\": \"0433fd25-da97-4cb1-b2f9-1ebb7bb39f21\",\n"
          + "      \"name\": \"DNA Is Learnt\",\n"
          + "      \"scope\": \"API_SETTINGS\",\n"
          + "      \"disabled\": false,\n"
          + "      \"visibility\": {\n"
          + "          \"public\": {}\n"
          + "      },\n"
          + "      \"filter_criteria\": {\n"
          + "        \"logical_filter\": {\n"
          + "          \"operator\": \"LOGICAL_OPERATOR_AND\",\n"
          + "          \"filter_criteria\": [\n"
          + "            {\n"
          + "              \"relational_filter\": {\n"
          + "                \"field_name\": \"isLearnt\",\n"
          + "                \"operator\": \"RELATIONAL_OPERATOR_EQ\",\n"
          + "                \"field_value\": true\n"
          + "              }\n"
          + "            }\n"
          + "          ]\n"
          + "        }\n"
          + "      },\n"
          + "    }\n"
          + "  ]\n"
          + "}";

  private SavedFilterServiceGrpc.SavedFilterServiceBlockingStub savedFilterServiceBlockingStub;
  private MockGenericConfigService mockGenericConfigService;
  private Config config;
  @Mock private ConfigChangeEventGenerator eventGenerator;
  @Mock private TimestampConverter timestampConverter;
  @Mock private UuidGenerator uuidGenerator;

  @BeforeEach
  void beforeEach() {
    this.config = ConfigFactory.parseString(MOCK_CONFIG);
    when(this.timestampConverter.convert(any()))
        .thenReturn(Timestamp.newBuilder().setSeconds(100).build());
  }

  @AfterEach
  void afterEach() {}

  @Test
  void testSavedFilterCRUD() {
    when(uuidGenerator.generateRandomId()).thenReturn(UUID_1);
    this.mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    setupSavedFilterService();

    FilterCriteria filterCriteria1 =
        buildRelationalFilterCriteria("c1", RelationalOperator.RELATIONAL_OPERATOR_EQ, "v1");
    Visibility publicVisibility =
        Visibility.newBuilder().setPublic(PublicVisibility.newBuilder().build()).build();
    Visibility privateVisibility =
        Visibility.newBuilder().setPrivate(PrivateVisibility.newBuilder().build()).build();
    RequestContext requestContext = buildRequestContext();
    SavedFilter createdSavedFilter =
        requestContext.call(
            () ->
                this.savedFilterServiceBlockingStub
                    .createSavedFilter(
                        CreateSavedFilterRequest.newBuilder()
                            .setName(FILTER_NAME_1)
                            .setScope(TRACES_SCOPE)
                            .setVisibility(privateVisibility)
                            .setFilterCriteria(filterCriteria1)
                            .build())
                    .getSavedFilter());

    assertEquals(UUID_1, createdSavedFilter.getId());
    assertEquals(FILTER_NAME_1, createdSavedFilter.getName());
    assertEquals(TRACES_SCOPE, createdSavedFilter.getScope());
    assertEquals(privateVisibility, createdSavedFilter.getVisibility());
    assertEquals(filterCriteria1, createdSavedFilter.getFilterCriteria());
    assertEquals("Favian.Reynolds@example.com", createdSavedFilter.getCreatedByUserId());
    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(expectedTimestamp, createdSavedFilter.getCreatedTimestamp());
    assertEquals(expectedTimestamp, createdSavedFilter.getUpdatedTimestamp());

    FilterCriteria filterCriteria2 =
        buildRelationalFilterCriteria("c2", RelationalOperator.RELATIONAL_OPERATOR_NEQ, "v1");
    SavedFilter updatedSavedFilter =
        requestContext.call(
            () ->
                this.savedFilterServiceBlockingStub
                    .updateSavedFilter(
                        UpdateSavedFilterRequest.newBuilder()
                            .setId(UUID_1)
                            .setName(FILTER_NAME_2)
                            .setVisibility(publicVisibility)
                            .setFilterCriteria(filterCriteria2)
                            .build())
                    .getSavedFilter());

    assertEquals(FILTER_NAME_2, updatedSavedFilter.getName());
    assertEquals(TRACES_SCOPE, updatedSavedFilter.getScope());
    assertEquals(publicVisibility, updatedSavedFilter.getVisibility());
    assertEquals(filterCriteria2, updatedSavedFilter.getFilterCriteria());

    GetSavedFiltersResponse getSavedFiltersResponse =
        requestContext.call(
            () ->
                this.savedFilterServiceBlockingStub.getSavedFilters(
                    GetSavedFiltersRequest.newBuilder().setScope(TRACES_SCOPE).build()));
    assertEquals(1, getSavedFiltersResponse.getSavedFiltersCount());
    assertEquals(FILTER_NAME_2, updatedSavedFilter.getName());
    assertEquals(TRACES_SCOPE, updatedSavedFilter.getScope());
    assertEquals(publicVisibility, updatedSavedFilter.getVisibility());
    assertEquals(filterCriteria2, updatedSavedFilter.getFilterCriteria());

    getSavedFiltersResponse =
        requestContext.call(
            () ->
                this.savedFilterServiceBlockingStub.getSavedFilters(
                    GetSavedFiltersRequest.newBuilder().setScope(ENDPOINTS_SCOPE).build()));
    assertEquals(0, getSavedFiltersResponse.getSavedFiltersCount());

    requestContext.call(
        () ->
            this.savedFilterServiceBlockingStub
                .withCallCredentials(
                    RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get())
                .deleteSavedFilter(DeleteSavedFilterRequest.newBuilder().setId(UUID_1).build()));

    getSavedFiltersResponse =
        requestContext.call(
            () ->
                this.savedFilterServiceBlockingStub.getSavedFilters(
                    GetSavedFiltersRequest.newBuilder().setScope(TRACES_SCOPE).build()));
    assertEquals(0, getSavedFiltersResponse.getSavedFiltersCount());
    this.mockGenericConfigService.shutdown();
  }

  @Test
  void testSystemSavedFilterCRUD() {
    this.mockGenericConfigService = new MockGenericConfigService().mockUpsert().mockGetAll();
    setupSavedFilterService();

    RequestContext requestContext = buildRequestContext();

    // Get Default Saved Filter in SETTINGS scope
    GetSavedFiltersResponse getSavedFiltersResponse =
        requestContext.call(
            () ->
                this.savedFilterServiceBlockingStub.getSavedFilters(
                    GetSavedFiltersRequest.newBuilder().setScope("SETTINGS").build()));
    assertNotNull(getSavedFiltersResponse);

    // Update Default Saved Filter
    FilterCriteria filterCriteria =
        buildRelationalFilterCriteria("c1", RelationalOperator.RELATIONAL_OPERATOR_EQ, "v1");
    UpdateSavedFilterResponse updateSavedFilterResponse =
        requestContext.call(
            () ->
                this.savedFilterServiceBlockingStub.updateSavedFilter(
                    UpdateSavedFilterRequest.newBuilder()
                        .setId(SYSTEM_DEFAULT_FILTER_ID)
                        .setName("DNA Is Learnt")
                        .setFilterCriteria(filterCriteria)
                        .setVisibility(
                            Visibility.newBuilder()
                                .setPublic(PublicVisibility.getDefaultInstance())
                                .build())
                        .build()));
    SavedFilter updatedSavedFilter = updateSavedFilterResponse.getSavedFilter();
    assertEquals(updatedSavedFilter.getId(), SYSTEM_DEFAULT_FILTER_ID);

    // Delete Operation on Default Saved Filter should not be allowed
    assertThrows(
        StatusRuntimeException.class,
        () ->
            requestContext.call(
                () ->
                    this.savedFilterServiceBlockingStub
                        .withCallCredentials(
                            RequestContextClientCallCredsProviderFactory
                                .getClientCallCredsProvider()
                                .get())
                        .deleteSavedFilter(
                            DeleteSavedFilterRequest.newBuilder()
                                .setId(SYSTEM_DEFAULT_FILTER_ID)
                                .build())));
  }

  private void setupSavedFilterService() {
    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    this.mockGenericConfigService
        .addService(
            new SavedFilterConfigServiceImpl(
                new SavedFilterStoreManager(
                    new SavedFilterConfigStore(genericStub, eventGenerator),
                    this.timestampConverter,
                    this.uuidGenerator),
                new SavedFilterRequestValidator(),
                this.config))
        .start();
    this.savedFilterServiceBlockingStub =
        SavedFilterServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  private static FilterCriteria buildRelationalFilterCriteria(
      String ColName, RelationalOperator op, String ColValue) {
    return FilterCriteria.newBuilder()
        .setRelationalFilter(
            RelationalFilterCondition.newBuilder()
                .setFieldName(ColName)
                .setOperator(op)
                .setFieldValue(Value.newBuilder().setStringValue(ColValue).build())
                .build())
        .build();
  }

  private static RequestContext buildRequestContext() {
    return RequestContext.forTenantId("t1")
        .put(
            "authorization",
            "Bearer eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJGYXZpYW4uUmV5bm9sZHNAZXhhbXBsZS5jb20iLCJyb2xlIjoidXNlciIsImlhdCI6MTY4NDM0NzcxOSwiZXhwIjoxNjg0OTUyNTE5fQ.cjyK-u3j7K5sHNSf7SG0oGe6xKCV0jOTm9kqN68JmFkdxUx5Dvd1WuF7Zg7uM-8sNBgxmtPg-bxJ2Ddnmb4Fd3IhSe1L-1dbZi3-dJldlK6i9m3O5pp3ivoQvobk1mfh2jCdbZ3tLcLF7t5OLDnqK_9COQ4plTMlmwX8Jr1L8C1kfzYHfHa91Rc9lDGjSJnxaAwfXAgqBhOSZCdX-EgYRyINnF6elLibLnL8J_PP50RLctNnZuEznImvPXQ6twl8A6JJmCkJYUp0HP9mdBJIz6RcuHYt0QU5avZprKQ6p_23WhuNzSvPRSjD0l9RGR8uQHU8LWVuu9i0L8Sfyr3TrA");
  }
}
