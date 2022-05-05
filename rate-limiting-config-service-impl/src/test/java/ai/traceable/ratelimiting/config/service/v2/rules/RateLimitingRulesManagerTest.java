package ai.traceable.ratelimiting.config.service.v2.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.GetRulesByCategoryFilter;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.service.v2.rules.RateLimitingRulesManager;
import ai.traceable.ratelimiting.service.v2.rules.RateLimitingRulesStore;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RateLimitingRulesManagerTest {
  private RequestContext requestContext;
  private MockGenericConfigService mockConfigService;
  private UuidGenerator uuidGenerator;
  private RateLimitingRulesManager rulesManager;

  @BeforeEach
  void setUp() {
    requestContext = RequestContext.forTenantId("default tenant");
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    RateLimitingRulesStore rulesStore = new RateLimitingRulesStore(configServiceBlockingStub);
    uuidGenerator = mock(UuidGenerator.class);
    rulesManager = new RateLimitingRulesManager(rulesStore, uuidGenerator);
  }

  @AfterEach
  void tearDown() {
    mockConfigService.shutdown();
  }

  @Test
  void testCreateRateLimitingRule() {
    List<RateLimitingRuleData> ruleDataList =
        List.of(
            buildRateLimitingRuleData("rule1", Category.CATEGORY_RATE_LIMITING),
            buildRateLimitingRuleData("rule2", Category.CATEGORY_RATE_LIMITING),
            buildRateLimitingRuleData("rule3", Category.CATEGORY_DATA_EXFILTRATION));
    when(uuidGenerator.generateRandomId()).thenReturn("id1").thenReturn("id2").thenReturn("id3");
    List<RateLimitingRule> rules =
        ruleDataList.stream()
            .map(ruleData -> rulesManager.createRateLimitingRule(requestContext, ruleData))
            .collect(Collectors.toList());
    List<RateLimitingRule> expectedRules =
        List.of(
            buildRateLimitingRule("id1", "rule1", Category.CATEGORY_RATE_LIMITING),
            buildRateLimitingRule("id2", "rule2", Category.CATEGORY_RATE_LIMITING),
            buildRateLimitingRule("id3", "rule3", Category.CATEGORY_DATA_EXFILTRATION));
    assertEquals(3, rules.size());
    assertEquals(new HashSet<>(expectedRules), new HashSet<>(rules));
  }

  @Test
  void testGetRateLimitingRules() {
    List<RateLimitingRuleData> ruleDataList =
        List.of(
            buildRateLimitingRuleData("rule1", Category.CATEGORY_RATE_LIMITING),
            buildRateLimitingRuleData("rule2", Category.CATEGORY_RATE_LIMITING),
            buildRateLimitingRuleData("rule3", Category.CATEGORY_DATA_EXFILTRATION));
    when(uuidGenerator.generateRandomId()).thenReturn("id1").thenReturn("id2").thenReturn("id3");
    List<RateLimitingRule> expectedRules =
        ruleDataList.stream()
            .map(ruleData -> rulesManager.createRateLimitingRule(requestContext, ruleData))
            .collect(Collectors.toList());
    List<RateLimitingRule> rules;
    rules =
        rulesManager.getRateLimitingRules(
            requestContext, GetRulesByCategoryFilter.getDefaultInstance());
    assertEquals(3, rules.size());
    assertEquals(new HashSet<>(expectedRules), new HashSet<>(rules));
    rules =
        rulesManager.getRateLimitingRules(
            requestContext,
            GetRulesByCategoryFilter.newBuilder()
                .addCategories(Category.CATEGORY_RATE_LIMITING)
                .build());
    assertEquals(2, rules.size());
    assertEquals(new HashSet<>(expectedRules.subList(0, 2)), new HashSet<>(rules));
    rules =
        rulesManager.getRateLimitingRules(
            requestContext,
            GetRulesByCategoryFilter.newBuilder()
                .addCategories(Category.CATEGORY_DATA_EXFILTRATION)
                .build());
    assertEquals(1, rules.size());
    assertEquals(new HashSet<>(expectedRules.subList(2, 3)), new HashSet<>(rules));
  }

  @Test
  void testUpdateRateLimitingRule() {
    List<RateLimitingRuleData> ruleDataList =
        List.of(
            buildRateLimitingRuleData("rule1", Category.CATEGORY_RATE_LIMITING),
            buildRateLimitingRuleData("rule2", Category.CATEGORY_RATE_LIMITING),
            buildRateLimitingRuleData("rule3", Category.CATEGORY_DATA_EXFILTRATION));
    when(uuidGenerator.generateRandomId()).thenReturn("id1").thenReturn("id2").thenReturn("id3");
    ruleDataList.stream()
        .map(ruleData -> rulesManager.createRateLimitingRule(requestContext, ruleData))
        .collect(Collectors.toList());
    rulesManager.updateRateLimitingRule(
        requestContext,
        "id1",
        buildRateLimitingRuleData("rule1Updated", Category.CATEGORY_RATE_LIMITING));
    rulesManager.updateRateLimitingRule(
        requestContext,
        "id3",
        buildRateLimitingRuleData("rule3Updated", Category.CATEGORY_DATA_EXFILTRATION));
    List<RateLimitingRule> expectedRules =
        List.of(
            buildRateLimitingRule("id1", "rule1Updated", Category.CATEGORY_RATE_LIMITING),
            buildRateLimitingRule("id2", "rule2", Category.CATEGORY_RATE_LIMITING),
            buildRateLimitingRule("id3", "rule3Updated", Category.CATEGORY_DATA_EXFILTRATION));
    List<RateLimitingRule> rules =
        rulesManager.getRateLimitingRules(
            requestContext, GetRulesByCategoryFilter.getDefaultInstance());
    assertEquals(3, rules.size());
    assertEquals(new HashSet<>(expectedRules), new HashSet<>(rules));

    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                rulesManager.updateRateLimitingRule(
                    requestContext,
                    "id",
                    buildRateLimitingRuleData("ruleUpdated", Category.CATEGORY_RATE_LIMITING)));
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(throwable));
  }

  @Test
  void testDeleteRateLimitingRule() {
    List<RateLimitingRuleData> ruleDataList =
        List.of(
            buildRateLimitingRuleData("rule1", Category.CATEGORY_RATE_LIMITING),
            buildRateLimitingRuleData("rule2", Category.CATEGORY_RATE_LIMITING),
            buildRateLimitingRuleData("rule3", Category.CATEGORY_DATA_EXFILTRATION));
    when(uuidGenerator.generateRandomId()).thenReturn("id1").thenReturn("id2").thenReturn("id3");
    ruleDataList.stream()
        .map(ruleData -> rulesManager.createRateLimitingRule(requestContext, ruleData))
        .collect(Collectors.toList());
    RateLimitingRule deletedRule;
    deletedRule = rulesManager.deleteRateLimitingRule(requestContext, "id1");
    assertEquals(
        buildRateLimitingRule("id1", "rule1", Category.CATEGORY_RATE_LIMITING), deletedRule);
    deletedRule = rulesManager.deleteRateLimitingRule(requestContext, "id3");
    assertEquals(
        buildRateLimitingRule("id3", "rule3", Category.CATEGORY_DATA_EXFILTRATION), deletedRule);

    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesManager.deleteRateLimitingRule(requestContext, "id"));
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(throwable));
  }

  private RateLimitingRule buildRateLimitingRule(String id, String name, Category category) {
    return RateLimitingRule.newBuilder()
        .setId(id)
        .setData(buildRateLimitingRuleData(name, category))
        .build();
  }

  private RateLimitingRuleData buildRateLimitingRuleData(String name, Category category) {
    return RateLimitingRuleData.newBuilder().setName(name).setCategory(category).build();
  }
}
