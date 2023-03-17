package ai.traceable.api.gateway.config.service.filter;

import static ai.traceable.api.gateway.config.service.testutil.MockApiRouteFactory.API_PATH1;
import static ai.traceable.api.gateway.config.service.testutil.MockApiRouteFactory.ROUTE_WITHOUT_METHOD;
import static ai.traceable.api.gateway.config.service.testutil.MockApiRouteFactory.ROUTE_WITHOUT_SERVICE;
import static ai.traceable.api.gateway.config.service.testutil.MockApiRouteFactory.ROUTE_WITH_ALL_FIELDS;
import static ai.traceable.api.gateway.config.service.testutil.MockApiRouteFactory.SERVICE1;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.api.gateway.config.service.v1.ApiInfo;
import ai.traceable.api.gateway.config.service.v1.ApiRoute;
import ai.traceable.api.gateway.config.service.v1.ApiRouteFilter;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
class ApiInfoFilterToPredicateConverterTest {
  private ApiInfoFilterToPredicateConverter apiInfoFilterToPredicateConverter;

  @BeforeEach
  void setUp() {
    apiInfoFilterToPredicateConverter = new ApiInfoFilterToPredicateConverter();
  }

  @Test
  void testConvertWithoutFilter() {
    final ApiRouteFilter filter =
        ApiRouteFilter.newBuilder().setApiInfo(ApiInfo.newBuilder()).build();
    final Predicate<ApiRoute> result = apiInfoFilterToPredicateConverter.convert(filter);
    assertTrue(result.test(ROUTE_WITH_ALL_FIELDS));
    assertTrue(result.test(ROUTE_WITHOUT_METHOD));
    assertTrue(result.test(ROUTE_WITHOUT_SERVICE));
  }

  @Test
  void testConvertPathExactFilter() {
    final ApiRouteFilter filter =
        ApiRouteFilter.newBuilder().setApiInfo(ApiInfo.newBuilder().setPath(API_PATH1)).build();
    final Predicate<ApiRoute> result = apiInfoFilterToPredicateConverter.convert(filter);
    assertTrue(result.test(ROUTE_WITH_ALL_FIELDS));
    assertFalse(result.test(ROUTE_WITHOUT_METHOD));
    assertTrue(result.test(ROUTE_WITHOUT_SERVICE));
  }

  @Test
  void testConvertPathPrefixFilter() {
    final ApiRouteFilter filter =
        ApiRouteFilter.newBuilder()
            .setApiInfo(ApiInfo.newBuilder().setPath(API_PATH1 + "/south_pole"))
            .build();
    final Predicate<ApiRoute> result = apiInfoFilterToPredicateConverter.convert(filter);
    assertTrue(result.test(ROUTE_WITH_ALL_FIELDS));
    assertFalse(result.test(ROUTE_WITHOUT_METHOD));
    assertTrue(result.test(ROUTE_WITHOUT_SERVICE));
  }

  @Test
  void testConvertServiceNameFilter() {
    final ApiRouteFilter filter =
        ApiRouteFilter.newBuilder()
            .setApiInfo(ApiInfo.newBuilder().setServiceName(SERVICE1))
            .build();
    final Predicate<ApiRoute> result = apiInfoFilterToPredicateConverter.convert(filter);
    assertTrue(result.test(ROUTE_WITH_ALL_FIELDS));
    assertFalse(result.test(ROUTE_WITHOUT_METHOD));
    assertTrue(result.test(ROUTE_WITHOUT_SERVICE));
  }

  @Test
  void testConvertMethodFilter() {
    final ApiRouteFilter filter =
        ApiRouteFilter.newBuilder().setApiInfo(ApiInfo.newBuilder().setHttpMethod("get")).build();
    final Predicate<ApiRoute> result = apiInfoFilterToPredicateConverter.convert(filter);
    assertTrue(result.test(ROUTE_WITH_ALL_FIELDS));
    assertTrue(result.test(ROUTE_WITHOUT_METHOD));
    assertFalse(result.test(ROUTE_WITHOUT_SERVICE));
  }

  @Test
  void testConvertWithAllFilters() {
    final ApiRouteFilter filter =
        ApiRouteFilter.newBuilder()
            .setApiInfo(
                ApiInfo.newBuilder()
                    .setServiceName(SERVICE1)
                    .setPath(API_PATH1)
                    .setHttpMethod("get"))
            .build();
    final Predicate<ApiRoute> result = apiInfoFilterToPredicateConverter.convert(filter);
    assertTrue(result.test(ROUTE_WITH_ALL_FIELDS));
    assertFalse(result.test(ROUTE_WITHOUT_METHOD));
    assertFalse(result.test(ROUTE_WITHOUT_SERVICE));
  }
}
