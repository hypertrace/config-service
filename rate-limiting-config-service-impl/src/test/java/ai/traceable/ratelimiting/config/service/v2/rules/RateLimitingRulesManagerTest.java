package ai.traceable.ratelimiting.config.service.v2.rules;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.Action.Allow;
import ai.traceable.ratelimiting.config.service.v2.Action.Block;
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
import ai.traceable.ratelimiting.config.service.v2.TransactionActionConfig;
import ai.traceable.ratelimiting.service.v2.RateLimitingConfigServiceConfig;
import ai.traceable.ratelimiting.service.v2.rules.RateLimitingRulesManager;
import ai.traceable.ratelimiting.service.v2.rules.RateLimitingRulesStore;
import ai.traceable.ratelimiting.service.v2.rules.modsec.RateLimitingModsecRulesManager;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Clock;
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
import org.mockito.Mock;

public class RateLimitingRulesManagerTest {
  private RequestContext requestContext;
  private MockGenericConfigService mockConfigService;
  private UuidGenerator uuidGenerator;
  private RateLimitingRulesManager rulesManager;
  private RateLimitingConfigServiceConfig rateLimitingConfigServiceConfig;
  @Mock private RateLimitingModsecRulesManager rateLimitingModsecRulesManager;
  private static final RateLimitingRule DEFAULT_RULE =
      RateLimitingRule.newBuilder()
          .setId("defaultRuleId1")
          .setData(
              RateLimitingRuleData.newBuilder()
                  .setRuleStatus(
                      RuleStatus.newBuilder()
                          .setRuleCreationSource(RuleStatus.RuleSource.RULE_SOURCE_DEFAULT)))
          .build();

  @BeforeEach
  void setUp() {
    rateLimitingConfigServiceConfig = mock(RateLimitingConfigServiceConfig.class);
    when(rateLimitingConfigServiceConfig.getDefaultRateLimitingRules())
        .thenReturn(List.of(DEFAULT_RULE));
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
    Clock clock = mock(Clock.class);
    doReturn(1000000L).when(clock).millis();
    rulesManager =
        new RateLimitingRulesManager(
            rulesStore,
            uuidGenerator,
            rateLimitingConfigServiceConfig,
            rateLimitingModsecRulesManager,
            clock);
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
                    .addAllIpAddresses(List.of("8.8.8.8"))
                    .addAllCidrIpRanges(List.of("3.3.3.3/31"))
                    .addAllRawInputIpData(List.of("1.2.3.4", "192.168.100.14/24", "127.0.0.1"))
                    .setExclude(true)
                    .build())
            .build();
    LeafCondition processedIpRuleLeaf =
        LeafCondition.newBuilder()
            .setIpAddressCondition(
                IpAddressCondition.newBuilder()
                    .addAllRawInputIpData(List.of("1.2.3.4", "192.168.100.14/24", "127.0.0.1"))
                    .addAllIpAddresses(List.of("8.8.8.8", "1.2.3.4", "127.0.0.1"))
                    .addAllCidrIpRanges(List.of("3.3.3.3/31", "192.168.100.14/24"))
                    .setExclude(true)
                    .build())
            .build();
    RateLimitingRuleData nonIpAddressRuleData =
        buildRateLimitingRuleData("nonip", Category.CATEGORY_RATE_LIMITING, nonIpRuleLeaf);
    assertEquals(
        nonIpAddressRuleData,
        rulesManager.applyRateLimitRuleDataTransformations(nonIpAddressRuleData));
    RateLimitingRuleData ipAddressRuleData =
        buildRateLimitingRuleData("iprule", Category.CATEGORY_DATA_EXFILTRATION, ipRuleLeaf);
    RateLimitingRuleData processedIpAddressRuleData =
        buildRateLimitingRuleData(
            "iprule", Category.CATEGORY_DATA_EXFILTRATION, processedIpRuleLeaf);
    assertEquals(
        processedIpAddressRuleData,
        rulesManager.applyRateLimitRuleDataTransformations(ipAddressRuleData));
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
        rulesManager.applyRateLimitRuleDataTransformations(compositeRuleData);
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
            buildRateLimitingRuleData("rule1", Category.CATEGORY_RATE_LIMITING, false),
            buildRateLimitingRuleData(
                "rule2", Category.CATEGORY_RATE_LIMITING, RuleConfigScope.newBuilder().build()),
            buildRateLimitingRuleData("rule3", Category.CATEGORY_RATE_LIMITING, scope),
            buildRateLimitingRuleData("rule4", Category.CATEGORY_DATA_EXFILTRATION, false));

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
            buildRateLimitingRuleData("rule1", Category.CATEGORY_RATE_LIMITING, false),
            buildRateLimitingRuleData(
                "rule2", Category.CATEGORY_RATE_LIMITING, RuleConfigScope.newBuilder().build()),
            buildRateLimitingRuleData("rule3", Category.CATEGORY_RATE_LIMITING, scope),
            buildRateLimitingRuleData("rule4", Category.CATEGORY_DATA_EXFILTRATION, true));

    when(uuidGenerator.generateRandomId())
        .thenReturn("id1")
        .thenReturn("id2")
        .thenReturn("id3")
        .thenReturn("id4");

    List<RateLimitingRule> expectedRules =
        ruleDataList.stream()
            .map(ruleData -> rulesManager.createRateLimitingRule(requestContext, ruleData))
            .collect(Collectors.toList());
    expectedRules.add(DEFAULT_RULE);

    // default filter will return all rules
    List<RateLimitingRule> rules =
        rulesManager.getRateLimitingRules(
            requestContext, GetRateLimitingRulesFilter.getDefaultInstance());
    assertEquals(5, rules.size());
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
                .setDisabled(true)
                .build());
    assertEquals(1, rules.size());
    assertEquals(new HashSet<>(expectedRules.subList(0, 1)), new HashSet<>(rules));

    rules =
        rulesManager.getRateLimitingRules(
            requestContext,
            GetRateLimitingRulesFilter.newBuilder()
                .addCategories(Category.CATEGORY_RATE_LIMITING)
                .setScope(filterScope)
                .setDisabled(false)
                .build());
    assertEquals(1, rules.size());
    assertEquals(new HashSet<>(expectedRules.subList(1, 2)), new HashSet<>(rules));

    rules =
        rulesManager.getRateLimitingRules(
            requestContext,
            GetRateLimitingRulesFilter.newBuilder()
                .addCategories(Category.CATEGORY_RATE_LIMITING)
                .setScope(filterScope)
                .setDisabled(false)
                .build());
    assertEquals(1, rules.size());
    assertEquals(new HashSet<>(expectedRules.subList(1, 2)), new HashSet<>(rules));
  }

  @Test
  void testMergeRateLimitRuleData() {
    RateLimitingRule oldRule =
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
    when(rateLimitingConfigServiceConfig.getDefaultRateLimitingRules())
        .thenReturn(List.of(oldRule));

    // Update case where RuleStatus is absent
    RateLimitingRuleData newRuleData1 = RateLimitingRuleData.newBuilder().build();
    RateLimitingRule updatedRule1 =
        rulesManager.updateRateLimitingRule(requestContext, "id-1", newRuleData1);
    assertEquals(
        RuleStatus.RuleSource.RULE_SOURCE_CUSTOMER,
        updatedRule1.getData().getRuleStatus().getRuleCreationSource());
    assertFalse(updatedRule1.getData().getRuleStatus().getHidden());
    assertTrue(updatedRule1.getData().getRuleStatus().getGenerateInternalEvents());

    // Update case where RuleStatus is present
    RateLimitingRuleData newRuleData2 =
        RateLimitingRuleData.newBuilder()
            .setRuleStatus(RuleStatus.newBuilder().setHidden(true).build())
            .build();
    RateLimitingRule updatedRule2 =
        rulesManager.updateRateLimitingRule(requestContext, "id-1", newRuleData2);
    assertEquals(
        RuleStatus.RuleSource.RULE_SOURCE_CUSTOMER,
        updatedRule2.getData().getRuleStatus().getRuleCreationSource());
    assertTrue(updatedRule2.getData().getRuleStatus().getHidden());
    assertTrue(updatedRule2.getData().getRuleStatus().getGenerateInternalEvents());
  }

  @Test
  void testUpdateRateLimitingRule() {
    RuleConfigScope scope =
        RuleConfigScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("dev")))
            .build();

    List<RateLimitingRuleData> ruleDataList =
        List.of(
            buildRateLimitingRuleData("rule1", Category.CATEGORY_RATE_LIMITING, false),
            buildRateLimitingRuleData(
                "rule2", Category.CATEGORY_RATE_LIMITING, RuleConfigScope.newBuilder().build()),
            buildRateLimitingRuleData("rule3", Category.CATEGORY_RATE_LIMITING, scope),
            buildRateLimitingRuleData("rule4", Category.CATEGORY_DATA_EXFILTRATION, false));

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
        buildRateLimitingRuleData("rule1Updated", Category.CATEGORY_RATE_LIMITING, false));
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
        buildRateLimitingRuleData("rule4Updated", Category.CATEGORY_DATA_EXFILTRATION, true));

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
            buildRateLimitingRule("id4", "rule4Updated", Category.CATEGORY_DATA_EXFILTRATION),
            DEFAULT_RULE);

    assertEquals(5, rules.size());
    assertEquals(new HashSet<>(expectedRules), new HashSet<>(rules));

    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                rulesManager.updateRateLimitingRule(
                    requestContext,
                    "id",
                    buildRateLimitingRuleData(
                        "ruleUpdated", Category.CATEGORY_RATE_LIMITING, false)));
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(throwable));
  }

  @Test
  void testDeleteRateLimitingRule() {
    List<RateLimitingRuleData> ruleDataList =
        List.of(
            buildRateLimitingRuleData("rule1", Category.CATEGORY_RATE_LIMITING, false),
            buildRateLimitingRuleData("rule2", Category.CATEGORY_RATE_LIMITING, false),
            buildRateLimitingRuleData("rule3", Category.CATEGORY_DATA_EXFILTRATION, true));
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

    // deleting default rule does not throw an exception
    assertDoesNotThrow(() -> rulesManager.deleteRateLimitingRule(requestContext, "defaultRuleId1"));
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
        .setData(buildRateLimitingRuleData(name, category, true))
        .build();
  }

  private RateLimitingRuleData buildRateLimitingRuleData(
      String name, Category category, boolean setExpiry) {
    TransactionActionConfig.Builder transactionActionConfigBuilder =
        TransactionActionConfig.newBuilder()
            .setAction(
                Action.newBuilder().setBlock(Block.newBuilder().setDurationIso("PT2H")).build());
    if (setExpiry) {
      transactionActionConfigBuilder.setExpirationTimestampMillis(8200000L);
    }
    return RateLimitingRuleData.newBuilder()
        .setName(name)
        .setCategory(category)
        .setRuleStatus(RuleStatus.newBuilder().setInternal(true).build())
        .setEnabled(false)
        .setTransactionActionConfig(transactionActionConfigBuilder)
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
        .setRuleStatus(RuleStatus.newBuilder().setInternal(true).build())
        .setEnabled(true)
        .setTransactionActionConfig(
            TransactionActionConfig.newBuilder()
                .setAction(Action.newBuilder().setAllow(Allow.getDefaultInstance()).build()))
        .build();
  }
}
