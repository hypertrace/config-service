package ai.traceable.api.gateway.config.service.filter;

import static ai.traceable.api.gateway.config.service.filter.ApiRouteFilterToPredicateConverter.API_ROUTE_TAUTOLOGY;
import static org.junit.jupiter.api.Assertions.assertSame;

import ai.traceable.api.gateway.config.service.v1.ApiRouteFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EmptyFilterToPredicateConverterTest {

  private EmptyFilterToPredicateConverter emptyFilterToPredicateConverter;

  @BeforeEach
  void setUp() {
    emptyFilterToPredicateConverter = new EmptyFilterToPredicateConverter();
  }

  @Test
  void testConvert() {
    assertSame(
        API_ROUTE_TAUTOLOGY,
        emptyFilterToPredicateConverter.convert(ApiRouteFilter.newBuilder().build()));
  }
}
