package ai.traceable.ratelimiting.config.service.v2.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition.LogicalOperator;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.EnvironmentScope;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.IpAddressCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RegionCondition;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import ai.traceable.ratelimiting.config.service.v2.RuleStatus;
import ai.traceable.ratelimiting.service.v2.RateLimitingConfigServiceConfig;
import ai.traceable.ratelimiting.service.v2.rules.RateLimitingRulesManager;
import ai.traceable.ratelimiting.service.v2.rules.RateLimitingRulesStore;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
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
  private RateLimitingConfigServiceConfig rateLimitingConfigServiceConfig;

  @BeforeEach
  void setUp() {
    rateLimitingConfigServiceConfig = mock(RateLimitingConfigServiceConfig.class);
    requestContext = RequestContext.forTenantId("default tenant");
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    ConfigChangeEventGenerator mockConfigChangeEventGenerator =
        mock(ConfigChangeEventGenerator.class);
    RateLimitingRulesStore rulesStore =
        new RateLimitingRulesStore(
            configServiceBlockingStub,
            mockConfigChangeEventGenerator,
            rateLimitingConfigServiceConfig);
    uuidGenerator = mock(UuidGenerator.class);
    rulesManager =
        new RateLimitingRulesManager(rulesStore, uuidGenerator, rateLimitingConfigServiceConfig);
  }

  @AfterEach
  void tearDown() {
    mockConfigService.shutdown();
  }

  @Test
  void testProcessRateLimitRuleData() {
    LeafCondition nonIpRuleLeaf =
        LeafCondition.newBuilder()
            .setRegionCondition(RegionCondition.newBuilder().addRegions("region").build())
            .build();
    LeafCondition ipRuleLeaf =
        LeafCondition.newBuilder()
            .setIpAddressCondition(
                IpAddressCondition.newBuilder()
                    .addAllIpAddresses(List.of("8.8.8.8")) // Shouldn't appear in processed
                    .addAllCidrIpRanges(List.of("3.3.3.3/31")) // Shouldn't appear in processed
                    .addAllRawInputIpData(List.of("1.2.3.4", "192.168.100.14/24", "127.0.0.1"))
                    .setExclude(true)
                    .build())
            .build();
    LeafCondition processedIpRuleLeaf =
        LeafCondition.newBuilder()
            .setIpAddressCondition(
                IpAddressCondition.newBuilder()
                    .addAllRawInputIpData(List.of("1.2.3.4", "192.168.100.14/24", "127.0.0.1"))
                    .addAllIpAddresses(List.of("1.2.3.4", "127.0.0.1"))
                    .addCidrIpRanges("192.168.100.14/24")
                    .setExclude(true)
                    .build())
            .build();
    RateLimitingRuleData nonIpAddressRuleData =
        buildRateLimitingRuleData("nonip", Category.CATEGORY_RATE_LIMITING, nonIpRuleLeaf);
    assertEquals(nonIpAddressRuleData, rulesManager.processRateLimitRuleData(nonIpAddressRuleData));
    RateLimitingRuleData ipAddressRuleData =
        buildRateLimitingRuleData("iprule", Category.CATEGORY_DATA_EXFILTRATION, ipRuleLeaf);
    RateLimitingRuleData processedIpAddressRuleData =
        buildRateLimitingRuleData(
            "iprule", Category.CATEGORY_DATA_EXFILTRATION, processedIpRuleLeaf);
    assertEquals(
        processedIpAddressRuleData, rulesManager.processRateLimitRuleData(ipAddressRuleData));
    RateLimitingRuleData compositeRuleData =
        buildRateLimitingRuleData(
            "compositerule",
            Category.CATEGORY_ENUMERATION,
            CompositeCondition.newBuilder()
                .addAllChildren(
                    List.of(
                        Condition.newBuilder().setLeafCondition(ipRuleLeaf).build(),
                        Condition.newBuilder().setLeafCondition(nonIpRuleLeaf).build()))
                .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                .build());
    RateLimitingRuleData processedCompositeRuleData =
        rulesManager.processRateLimitRuleData(compositeRuleData);
    List<Condition> processedChildren =
        processedCompositeRuleData.getCondition().getCompositeCondition().getChildrenList();
    assertEquals(
        Condition.newBuilder().setLeafCondition(processedIpRuleLeaf).build(),
        processedChildren.get(0));
    assertEquals(
        Condition.newBuilder().setLeafCondition(nonIpRuleLeaf).build(), processedChildren.get(1));
  }

  @Test
  void testCreateRateLimitingRule() {
    RuleConfigScope scope =
        RuleConfigScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("dev")))
            .build();

    List<RateLimitingRuleData> ruleDataList =
        List.of(
            buildRateLimitingRuleData("rule1", Category.CATEGORY_RATE_LIMITING),
            buildRateLimitingRuleData(
                "rule2", Category.CATEGORY_RATE_LIMITING, RuleConfigScope.newBuilder().build()),
            buildRateLimitingRuleData("rule3", Category.CATEGORY_RATE_LIMITING, scope),
            buildRateLimitingRuleData("rule4", Category.CATEGORY_DATA_EXFILTRATION));

    when(uuidGenerator.generateRandomId())
        .thenReturn("id1")
        .thenReturn("id2")
        .thenReturn("id3")
        .thenReturn("id4");

    List<RateLimitingRule> rules =
        ruleDataList.stream()
            .map(ruleData -> rulesManager.createRateLimitingRule(requestContext, ruleData))
            .collect(Collectors.toList());

    List<RateLimitingRule> expectedRules =
        List.of(
            buildRateLimitingRule("id1", "rule1", Category.CATEGORY_RATE_LIMITING),
            buildRateLimitingRule(
                "id2",
                "rule2",
                Category.CATEGORY_RATE_LIMITING,
                RuleConfigScope.newBuilder().build()),
            buildRateLimitingRule("id3", "rule3", Category.CATEGORY_RATE_LIMITING, scope),
            buildRateLimitingRule("id4", "rule4", Category.CATEGORY_DATA_EXFILTRATION));

    assertEquals(4, rules.size());
    assertEquals(new HashSet<>(expectedRules), new HashSet<>(rules));
  }

  @Test
  void testGetRateLimitingRules() {
    RuleConfigScope scope =
        RuleConfigScope.newBuilder()
            .setEnvironmentScope(
                EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("dev", "prod")))
            .build();

    List<RateLimitingRuleData> ruleDataList =
        List.of(
            buildRateLimitingRuleData("rule1", Category.CATEGORY_RATE_LIMITING),
            buildRateLimitingRuleData(
                "rule2", Category.CATEGORY_RATE_LIMITING, RuleConfigScope.newBuilder().build()),
            buildRateLimitingRuleData("rule3", Category.CATEGORY_RATE_LIMITING, scope),
            buildRateLimitingRuleData("rule4", Category.CATEGORY_DATA_EXFILTRATION));

    when(uuidGenerator.generateRandomId())
        .thenReturn("id1")
        .thenReturn("id2")
        .thenReturn("id3")
        .thenReturn("id4");

    List<RateLimitingRule> expectedRules =
        ruleDataList.stream()
            .map(ruleData -> rulesManager.createRateLimitingRule(requestContext, ruleData))
            .collect(Collectors.toList());

    // default filter will return all rules
    List<RateLimitingRule> rules =
        rulesManager.getRateLimitingRules(
            requestContext, GetRateLimitingRulesFilter.getDefaultInstance());
    assertEquals(4, rules.size());
    assertEquals(new HashSet<>(expectedRules), new HashSet<>(rules));

    // filter on category (deprecated flow)
    rules =
        rulesManager.getRateLimitingRules(
            requestContext,
            GetRateLimitingRulesFilter.newBuilder()
                .addCategories(Category.CATEGORY_RATE_LIMITING)
                .build());
    assertEquals(3, rules.size());
    assertEquals(new HashSet<>(expectedRules.subList(0, 3)), new HashSet<>(rules));

    // filter on category and scope
    RuleConfigScope filterScope =
        RuleConfigScope.newBuilder()
            .setEnvironmentScope(
                EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("staging")))
            .build();

    rules =
        rulesManager.getRateLimitingRules(
            requestContext,
            GetRateLimitingRulesFilter.newBuilder()
                .addCategories(Category.CATEGORY_RATE_LIMITING)
                .setScope(filterScope)
                .build());
    assertEquals(2, rules.size());
    assertEquals(new HashSet<>(expectedRules.subList(0, 2)), new HashSet<>(rules));

    // filter on category and scope
    filterScope =
        RuleConfigScope.newBuilder()
            .setEnvironmentScope(
                EnvironmentScope.newBuilder()
                    .addAllEnvironmentIds(List.of("prod", "dev", "staging")))
            .build();

    rules =
        rulesManager.getRateLimitingRules(
            requestContext,
            GetRateLimitingRulesFilter.newBuilder()
                .addCategories(Category.CATEGORY_RATE_LIMITING)
                .setScope(filterScope)
                .build());
    assertEquals(3, rules.size());
    assertEquals(new HashSet<>(expectedRules.subList(0, 3)), new HashSet<>(rules));

    // filter on category and scope [default rule scope should return all rules]
    filterScope = RuleConfigScope.getDefaultInstance();

    rules =
        rulesManager.getRateLimitingRules(
            requestContext,
            GetRateLimitingRulesFilter.newBuilder()
                .addCategories(Category.CATEGORY_RATE_LIMITING)
                .setScope(filterScope)
                .build());
    assertEquals(3, rules.size());
    assertEquals(new HashSet<>(expectedRules.subList(0, 3)), new HashSet<>(rules));

    // filter on category and scope [rule scope with env scope with no envs should only return rules
    // with no envs]
    filterScope =
        RuleConfigScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.getDefaultInstance())
            .build();
    rules =
        rulesManager.getRateLimitingRules(
            requestContext,
            GetRateLimitingRulesFilter.newBuilder()
                .addCategories(Category.CATEGORY_RATE_LIMITING)
                .setScope(filterScope)
                .build());
    assertEquals(2, rules.size());
    assertEquals(new HashSet<>(expectedRules.subList(0, 2)), new HashSet<>(rules));
  }

  @Test
  void testMergedStatusOnUpdateRateLimitingRule() {
    RateLimitingRule rule =
        RateLimitingRule.newBuilder()
            .setId("id-1")
            .setData(
                RateLimitingRuleData.newBuilder()
                    .setCategory(Category.CATEGORY_RATE_LIMITING)
                    .setRuleStatus(
                        RuleStatus.newBuilder()
                            .setRuleCreationSource(RuleStatus.RuleSource.RULE_SOURCE_CUSTOMER)
                            .setHidden(false)
                            .setGenerateInternalEvents(true)
                            .build())
                    .build())
            .build();
    when(rateLimitingConfigServiceConfig.getDefaultRateLimitingRules()).thenReturn(List.of(rule));
    RateLimitingRule updatedRule =
        rulesManager.updateRateLimitingRule(
            requestContext,
            "id-1",
            RateLimitingRuleData.newBuilder()
                .setRuleStatus(
                    RuleStatus.newBuilder()
                        .setRuleCreationSource(RuleStatus.RuleSource.RULE_SOURCE_UNSPECIFIED)
                        .setHidden(true)
                        .build())
                .build());
    assertEquals(
        updatedRule.getData().getRuleStatus().getRuleCreationSource(),
        RuleStatus.RuleSource.RULE_SOURCE_CUSTOMER);
    assertEquals(updatedRule.getData().getRuleStatus().getHidden(), true);
    assertEquals(updatedRule.getData().getRuleStatus().getGenerateInternalEvents(), true);
  }

  @Test
  void testUpdateRateLimitingRule() {
    RuleConfigScope scope =
        RuleConfigScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("dev")))
            .build();

    List<RateLimitingRuleData> ruleDataList =
        List.of(
            buildRateLimitingRuleData("rule1", Category.CATEGORY_RATE_LIMITING),
            buildRateLimitingRuleData(
                "rule2", Category.CATEGORY_RATE_LIMITING, RuleConfigScope.newBuilder().build()),
            buildRateLimitingRuleData("rule3", Category.CATEGORY_RATE_LIMITING, scope),
            buildRateLimitingRuleData("rule4", Category.CATEGORY_DATA_EXFILTRATION));

    when(uuidGenerator.generateRandomId())
        .thenReturn("id1")
        .thenReturn("id2")
        .thenReturn("id3")
        .thenReturn("id4");

    ruleDataList.stream()
        .map(ruleData -> rulesManager.createRateLimitingRule(requestContext, ruleData))
        .collect(Collectors.toList());

    rulesManager.updateRateLimitingRule(
        requestContext,
        "id1",
        buildRateLimitingRuleData("rule1Updated", Category.CATEGORY_RATE_LIMITING));
    rulesManager.updateRateLimitingRule(
        requestContext,
        "id2",
        buildRateLimitingRuleData("rule2Updated", Category.CATEGORY_RATE_LIMITING, scope));
    rulesManager.updateRateLimitingRule(
        requestContext,
        "id3",
        buildRateLimitingRuleData(
            "rule3Updated", Category.CATEGORY_RATE_LIMITING, RuleConfigScope.newBuilder().build()));
    rulesManager.updateRateLimitingRule(
        requestContext,
        "id4",
        buildRateLimitingRuleData("rule4Updated", Category.CATEGORY_DATA_EXFILTRATION));

    List<RateLimitingRule> rules =
        rulesManager.getRateLimitingRules(
            requestContext, GetRateLimitingRulesFilter.getDefaultInstance());

    List<RateLimitingRule> expectedRules =
        List.of(
            buildRateLimitingRule("id1", "rule1Updated", Category.CATEGORY_RATE_LIMITING),
            buildRateLimitingRule("id2", "rule2Updated", Category.CATEGORY_RATE_LIMITING, scope),
            buildRateLimitingRule(
                "id3",
                "rule3Updated",
                Category.CATEGORY_RATE_LIMITING,
                RuleConfigScope.newBuilder().build()),
            buildRateLimitingRule("id4", "rule4Updated", Category.CATEGORY_DATA_EXFILTRATION));

    assertEquals(4, rules.size());
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
    Optional<RateLimitingRule> deletedRule =
        rulesManager.deleteRateLimitingRule(requestContext, "id1");
    assertEquals(
        Optional.of(buildRateLimitingRule("id1", "rule1", Category.CATEGORY_RATE_LIMITING)),
        deletedRule);
    deletedRule = rulesManager.deleteRateLimitingRule(requestContext, "id3");
    assertEquals(
        Optional.of(buildRateLimitingRule("id3", "rule3", Category.CATEGORY_DATA_EXFILTRATION)),
        deletedRule);

    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> rulesManager.deleteRateLimitingRule(requestContext, "id"));
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(throwable));
  }

  private RateLimitingRule buildRateLimitingRule(
      String id, String name, Category category, RuleStatus ruleStatus) {
    return RateLimitingRule.newBuilder()
        .setId(id)
        .setData(buildRateLimitingRuleData(name, category, ruleStatus))
        .build();
  }

  private RateLimitingRuleData buildRateLimitingRuleData(
      String name, Category category, RuleStatus ruleStatus) {
    return RateLimitingRuleData.newBuilder()
        .setName(name)
        .setCategory(category)
        .setRuleStatus(ruleStatus)
        .build();
  }

  private RateLimitingRule buildRateLimitingRule(String id, String name, Category category) {
    return RateLimitingRule.newBuilder()
        .setId(id)
        .setData(buildRateLimitingRuleData(name, category))
        .build();
  }

  private RateLimitingRuleData buildRateLimitingRuleData(String name, Category category) {
    return RateLimitingRuleData.newBuilder()
        .setName(name)
        .setCategory(category)
        .setRuleStatus(RuleStatus.newBuilder().build())
        .build();
  }

  private RateLimitingRuleData buildRateLimitingRuleData(
      String name, Category category, LeafCondition leafCondition) {
    return RateLimitingRuleData.newBuilder()
        .setName(name)
        .setCategory(category)
        .setCondition(Condition.newBuilder().setLeafCondition(leafCondition).build())
        .build();
  }

  private RateLimitingRuleData buildRateLimitingRuleData(
      String name, Category category, CompositeCondition compositeCondition) {
    return RateLimitingRuleData.newBuilder()
        .setName(name)
        .setCategory(category)
        .setCondition(Condition.newBuilder().setCompositeCondition(compositeCondition).build())
        .build();
  }

  private RateLimitingRule buildRateLimitingRule(
      String id, String name, Category category, RuleConfigScope scope) {
    return RateLimitingRule.newBuilder()
        .setId(id)
        .setData(buildRateLimitingRuleData(name, category, scope))
        .build();
  }

  private RateLimitingRuleData buildRateLimitingRuleData(
      String name, Category category, RuleConfigScope scope) {
    return RateLimitingRuleData.newBuilder()
        .setName(name)
        .setCategory(category)
        .setRuleConfigScope(scope)
        .setRuleStatus(RuleStatus.newBuilder().build())
        .build();
  }
}
