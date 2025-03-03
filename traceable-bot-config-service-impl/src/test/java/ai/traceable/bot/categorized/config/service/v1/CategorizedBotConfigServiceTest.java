package ai.traceable.bot.categorized.config.service.v1;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class CategorizedBotConfigServiceTest {

  private static CategorizedBotConfigServiceGrpc.CategorizedBotConfigServiceBlockingStub stub;
  private static MockGenericConfigService mockGenericConfigService;

  @BeforeAll
  static void before() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockGenericConfigService
        .addService(new CategorizedBotConfigService(new CategorizedBotDetailsConfig()))
        .start();
    stub =
        CategorizedBotConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @AfterAll
  static void afterAll() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void testGetAllBots() {
    final RequestContext requestContext = RequestContext.forTenantId("t1");
    assertEquals(
        0,
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigs(
                        GetCategorizedBotConfigsRequest.newBuilder()
                            .setBotRequestFilter(
                                CategorizedBotRequestFilter.newBuilder().addBotIds("1").build())
                            .build()))
            .getBotConfigsCount());
    // This test will break everytime we add new bots, this is intentional.
    final int botConfigsCount =
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigs(
                        GetCategorizedBotConfigsRequest.newBuilder().build()))
            .getBotConfigsCount();
    assertEquals(1, botConfigsCount);
  }

  @Test
  void testGetInvalidIdFilterBots() {
    final RequestContext requestContext = RequestContext.forTenantId("t1");
    assertEquals(
        0,
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigs(
                        GetCategorizedBotConfigsRequest.newBuilder()
                            .setBotRequestFilter(
                                CategorizedBotRequestFilter.newBuilder().addBotIds("1").build())
                            .build()))
            .getBotConfigsCount());
  }

  @Test
  void testGetValidIdFilterBots() {
    final RequestContext requestContext = RequestContext.forTenantId("t1");
    assertEquals(
        1,
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigs(
                        GetCategorizedBotConfigsRequest.newBuilder()
                            .setBotRequestFilter(
                                CategorizedBotRequestFilter.newBuilder()
                                    .addBotIds("550e8400-e29b-41d4-a716-446655440000")
                                    .build())
                            .build()))
            .getBotConfigsCount());
  }

  @Test
  void testGetInvalidCategoryFilterBots() {
    final RequestContext requestContext = RequestContext.forTenantId("t1");
    assertEquals(
        0,
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigs(
                        GetCategorizedBotConfigsRequest.newBuilder()
                            .setBotRequestFilter(
                                CategorizedBotRequestFilter.newBuilder()
                                    .addCategory("DUMMY")
                                    .build())
                            .build()))
            .getBotConfigsCount());
  }

  @Test
  void testGetValidCategoryFilterBots() {
    final RequestContext requestContext = RequestContext.forTenantId("t1");
    assertEquals(
        1,
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigs(
                        GetCategorizedBotConfigsRequest.newBuilder()
                            .setBotRequestFilter(
                                CategorizedBotRequestFilter.newBuilder()
                                    .addCategory("CRAWLERS")
                                    .build())
                            .build()))
            .getBotConfigsCount());
  }

  @Test
  void testGetInvalidSubCategoryFilterBots() {
    final RequestContext requestContext = RequestContext.forTenantId("t1");
    assertEquals(
        0,
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigs(
                        GetCategorizedBotConfigsRequest.newBuilder()
                            .setBotRequestFilter(
                                CategorizedBotRequestFilter.newBuilder()
                                    .addSubCategory("DUMMY")
                                    .build())
                            .build()))
            .getBotConfigsCount());
  }

  @Test
  void testGetValidSubCategoryFilterBots() {
    final RequestContext requestContext = RequestContext.forTenantId("t1");
    assertEquals(
        1,
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigs(
                        GetCategorizedBotConfigsRequest.newBuilder()
                            .setBotRequestFilter(
                                CategorizedBotRequestFilter.newBuilder()
                                    .addSubCategory("SEARCH_BOTS")
                                    .build())
                            .build()))
            .getBotConfigsCount());
  }
}
