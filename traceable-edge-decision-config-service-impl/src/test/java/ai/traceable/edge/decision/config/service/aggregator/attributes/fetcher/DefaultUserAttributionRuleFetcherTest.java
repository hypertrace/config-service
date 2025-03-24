package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultUserAttributionRuleFetcherTest {

  private Config config;

  @BeforeEach
  void setup() {
    File configFile = new File(ClassLoader.getSystemResource("test_application.conf").getPath());
    config = ConfigFactory.parseFile(configFile);
  }

  @Test
  void testGetConfigsForTenant_validTenant() {
    DefaultUserAttributionRuleFetcher fetchers = new DefaultUserAttributionRuleFetcher(config);

    List<DerivationRule> tenant1Configs =
        fetchers.getUserAttributionRules(RequestContext.forTenantId("tenant1"));
    List<DerivationRule> tenant2Configs =
        fetchers.getUserAttributionRules(RequestContext.forTenantId("tenant2"));

    assertEquals(2, tenant1Configs.size(), "Tenant1 should have 2 configs");
    assertEquals(
        "$s.getHeaders().get('authorization')",
        tenant1Configs.get(0).getTransformationConfig().getJexlExpression().getJexlExpression());
    assertEquals(1, tenant2Configs.size(), "Tenant2 should have 1 config");
  }

  @Test
  void testGetConfigsForTenant_missingTenant() {
    DefaultUserAttributionRuleFetcher fetchers = new DefaultUserAttributionRuleFetcher(config);

    List<DerivationRule> missingTenantConfigs =
        fetchers.getUserAttributionRules(RequestContext.forTenantId("missingTenant"));

    assertTrue(missingTenantConfigs.isEmpty(), "Missing tenant should return an empty list");
  }

  @Test
  void testGetConfigsForTenant_invalidConfig() {
    Config invalidConfig =
        ConfigFactory.parseString(
            "traceable.edge.config.service {\n"
                + "default.variables.user-attribution {\n"
                + "          tenant1 = [\n"
                + "            { \"transformation_config\": { \"unknown_field\": \"invalid\" } },\n"
                + "            { \"transformation_config\": { static_value {\n"
                + "  string_value: \"value1\"\n"
                + "} } }\n"
                + "          ]\n"
                + "        }}");

    DefaultUserAttributionRuleFetcher fetchers =
        new DefaultUserAttributionRuleFetcher(invalidConfig);
    List<DerivationRule> tenant1Configs =
        fetchers.getUserAttributionRules(RequestContext.forTenantId("tenant1"));
    // Tenant1 should skip invalid rules and only return valid configs
    assertEquals(1, tenant1Configs.size());
  }

  @Test
  void testNoConfigPath() {
    Config emptyConfig = ConfigFactory.empty();
    DefaultUserAttributionRuleFetcher fetchers = new DefaultUserAttributionRuleFetcher(emptyConfig);
    List<DerivationRule> tenantConfigs =
        fetchers.getUserAttributionRules(RequestContext.forTenantId("tenant1"));
    assertTrue(tenantConfigs.isEmpty(), "No configurations should return an empty list");
  }
}
