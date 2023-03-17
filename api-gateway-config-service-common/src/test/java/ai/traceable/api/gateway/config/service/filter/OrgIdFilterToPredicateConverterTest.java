package ai.traceable.api.gateway.config.service.filter;

import static ai.traceable.api.gateway.config.service.testutil.MockApiRouteFactory.ORG_ID1;
import static ai.traceable.api.gateway.config.service.testutil.MockApiRouteFactory.ROUTE_WITHOUT_METHOD;
import static ai.traceable.api.gateway.config.service.testutil.MockApiRouteFactory.ROUTE_WITHOUT_SERVICE;
import static ai.traceable.api.gateway.config.service.testutil.MockApiRouteFactory.ROUTE_WITH_ALL_FIELDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.api.gateway.config.service.v1.ApiRoute;
import ai.traceable.api.gateway.config.service.v1.ApiRouteFilter;
import ai.traceable.api.gateway.config.service.v1.OrgIds;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OrgIdFilterToPredicateConverterTest {

  private OrgIdFilterToPredicateConverter orgIdFilterToPredicateConverter;

  @BeforeEach
  void setUp() {
    orgIdFilterToPredicateConverter = new OrgIdFilterToPredicateConverter();
  }

  @Test
  void testConvertWithoutOrgIds() {
    final ApiRouteFilter filter =
        ApiRouteFilter.newBuilder().setOrgIds(OrgIds.newBuilder()).build();
    final Predicate<ApiRoute> result = orgIdFilterToPredicateConverter.convert(filter);
    assertFalse(result.test(ROUTE_WITH_ALL_FIELDS));
    assertFalse(result.test(ROUTE_WITHOUT_METHOD));
    assertFalse(result.test(ROUTE_WITHOUT_SERVICE));
  }

  @Test
  void testConvertWithOrgIds() {
    final ApiRouteFilter filter =
        ApiRouteFilter.newBuilder().setOrgIds(OrgIds.newBuilder().addOrgId(ORG_ID1)).build();
    final Predicate<ApiRoute> result = orgIdFilterToPredicateConverter.convert(filter);
    assertTrue(result.test(ROUTE_WITH_ALL_FIELDS));
    assertTrue(result.test(ROUTE_WITHOUT_METHOD));
    assertFalse(result.test(ROUTE_WITHOUT_SERVICE));
  }
}
