package ai.traceable.saved.filter.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import ai.traceable.saved.filter.config.service.v1.Visibility;
import ai.traceable.saved.filter.config.service.validation.SavedFilterRequestValidator;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
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

  private SavedFilterServiceGrpc.SavedFilterServiceBlockingStub savedFilterServiceBlockingStub;
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
            new SavedFilterConfigServiceImpl(
                new SavedFilterStoreManager(
                    new SavedFilterConfigStore(genericStub, eventGenerator),
                    this.timestampConverter,
                    uuidGenerator),
                new SavedFilterRequestValidator()))
        .start();

    this.savedFilterServiceBlockingStub =
        SavedFilterServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel())
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
  void testSavedFilterCRUD() {
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
