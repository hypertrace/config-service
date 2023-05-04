package ai.traceable.api.gateway.config.service.filter;

import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.api.gateway.config.service.v1.ApiRouteFilter;
import com.google.inject.Guice;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApiRouteFilterModuleTest {

  private ApiGatewayFilterModule apiRouteFilterModule;

  @BeforeEach
  void setUp() {
    apiRouteFilterModule = new ApiGatewayFilterModule();
  }

  @Test
  void testFilterMapCoverage() {
    assertEquals(
        Arrays.stream(ApiRouteFilter.TypeCase.values()).collect(toUnmodifiableSet()),
        Guice.createInjector(apiRouteFilterModule)
            .getInstance(
                Key.get(
                    new TypeLiteral<
                        Map<ApiRouteFilter.TypeCase, ApiRouteFilterToPredicateConverter>>() {}))
            .keySet());
  }
}
