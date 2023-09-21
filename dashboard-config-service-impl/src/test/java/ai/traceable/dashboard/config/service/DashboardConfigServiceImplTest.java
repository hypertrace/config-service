package ai.traceable.dashboard.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.dashboard.config.service.v1.CreateDashboardRequest;
import ai.traceable.dashboard.config.service.v1.Dashboard;
import ai.traceable.dashboard.config.service.v1.DashboardConfigServiceGrpc;
import ai.traceable.dashboard.config.service.v1.DeleteDashboardRequest;
import ai.traceable.dashboard.config.service.v1.GetDashboardsRequest;
import ai.traceable.dashboard.config.service.v1.UpdateDashboardRequest;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
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

@ExtendWith({MockitoExtension.class})
class DashboardConfigServiceImplTest {
  MockGenericConfigService mockGenericConfigService;
  @Mock ConfigChangeEventGenerator mockConfigChangeEventGenerator;
  @Mock DashboardConfigServiceValidator mockValidator;
  @Mock UuidGenerator mockUuidGenerator;
  TimestampConverter timestampConverter;
  @Mock Clock mockClock;
  RequestContext testRequestContext;

  DashboardConfigServiceGrpc.DashboardConfigServiceBlockingStub stub;

  static Dashboard SAMPLE_DASHBOARD =
      Dashboard.newBuilder()
          .setId("id")
          .setName("new-test-dashboard")
          .setDescription("test dashboard")
          .setAuthor("tester12@new.com")
          .setJson("json-data-blob")
          .setUiReference("home")
          .build();

  static Dashboard SAMPLE_DASHBOARD_2 =
      Dashboard.newBuilder()
          .setId("id-2")
          .setName("dashboard-2")
          .setAuthor("dev@xyz.com")
          .setDescription("another dashboard to test")
          .setUiReference("protection")
          .setJson("json-data-blob")
          .build();

  @BeforeEach
  void setUp() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    timestampConverter = new TimestampConverter();
    this.testRequestContext =
        RequestContext.forTenantId("test-tenant")
            .put(
                "authorization",
                "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiZW1haWwiOiJ0ZXN0ZXIxMkBuZXcuY29tIiwiaWF0IjoxNTE2MjM5MDIyfQ._d3eAN5xGYsl25yqwdgwbWUwyP0pTNHrBco0YBHmF2s");
    mockGenericConfigService
        .addService(
            new DashboardConfigServiceImpl(
                mockValidator,
                new DashboardStore(
                    ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel()),
                    mockConfigChangeEventGenerator),
                mockUuidGenerator,
                timestampConverter,
                mockClock))
        .start();
    stub =
        DashboardConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void testCreateReadUpdateDelete() {
    Instant testInstant = Clock.systemUTC().instant();
    // create
    CreateDashboardRequest request =
        CreateDashboardRequest.newBuilder()
            .setName(SAMPLE_DASHBOARD.getName())
            .setDescription(SAMPLE_DASHBOARD.getDescription())
            .setJson(SAMPLE_DASHBOARD.getJson())
            .setUiReference(SAMPLE_DASHBOARD.getUiReference())
            .build();

    SAMPLE_DASHBOARD =
        SAMPLE_DASHBOARD.toBuilder()
            .setCreationTimestamp(timestampConverter.convert(testInstant))
            .setLastUpdatedTimestamp(timestampConverter.convert(testInstant))
            .build();

    when(this.mockUuidGenerator.generateRandomId())
        .thenReturn(SAMPLE_DASHBOARD.getId(), SAMPLE_DASHBOARD_2.getId());
    when(this.mockClock.instant()).thenReturn(testInstant);
    Dashboard dashboard1 =
        this.testRequestContext.call(() -> this.stub.createDashboard(request)).getDashboard();

    assertEquals(SAMPLE_DASHBOARD, dashboard1);

    // get
    CreateDashboardRequest createRequest =
        CreateDashboardRequest.newBuilder()
            .setName(SAMPLE_DASHBOARD_2.getName())
            .setDescription(SAMPLE_DASHBOARD_2.getDescription())
            .setJson(SAMPLE_DASHBOARD_2.getJson())
            .setUiReference(SAMPLE_DASHBOARD_2.getUiReference())
            .build();

    Dashboard dashboard2 =
        this.testRequestContext.call(() -> this.stub.createDashboard(createRequest)).getDashboard();
    GetDashboardsRequest getRequest = GetDashboardsRequest.getDefaultInstance();
    assertEquals(
        List.of(dashboard2, dashboard1),
        this.testRequestContext
            .call(() -> this.stub.getDashboards(getRequest))
            .getDashboardsList());
    // test filter
    GetDashboardsRequest getRequestWithFilter =
        GetDashboardsRequest.newBuilder()
            .setFilter(
                GetDashboardsRequest.DashboardFilter.newBuilder().setUiReference("protection"))
            .build();
    assertEquals(
        List.of(dashboard2),
        this.testRequestContext
            .call(() -> this.stub.getDashboards(getRequestWithFilter))
            .getDashboardsList());

    // update
    UpdateDashboardRequest updateRequest =
        UpdateDashboardRequest.newBuilder()
            .setId(SAMPLE_DASHBOARD.getId())
            .setName("updated-name")
            .setDescription("new des")
            .setJson("updated_blob")
            .build();

    Instant updateInstant = Clock.systemUTC().instant();
    Dashboard updatedDashboard =
        SAMPLE_DASHBOARD.toBuilder()
            .setName("updated-name")
            .setDescription("new des")
            .setJson("updated_blob")
            .setLastUpdatedTimestamp(timestampConverter.convert(updateInstant))
            .build();

    when(this.mockClock.instant()).thenReturn(updateInstant);
    assertEquals(
        updatedDashboard,
        this.testRequestContext
            .call(() -> this.stub.updateDashboard(updateRequest))
            .getDashboard());

    // delete
    this.testRequestContext.call(
        () ->
            this.stub.deleteDashboard(
                DeleteDashboardRequest.newBuilder().setId(SAMPLE_DASHBOARD.getId()).build()));
    assertEquals(
        List.of(dashboard2),
        this.testRequestContext
            .call(() -> this.stub.getDashboards(getRequest))
            .getDashboardsList());
  }
}
