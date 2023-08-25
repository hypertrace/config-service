package ai.traceable.ratelimiting.config.service.v2.rules.modsec;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.customsignature.config.service.modsec.ModsecRuleConversion;
import ai.traceable.customsignature.config.service.modsec.registry.ModsecRuleMappings;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.GlobalScope;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.KeyValuePattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Location;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Operator;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.StringPattern;
import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.Action.Allow;
import ai.traceable.ratelimiting.config.service.v2.Action.Block;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition.LogicalOperator;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.DataLocation;
import ai.traceable.ratelimiting.config.service.v2.DatatypeCondition;
import ai.traceable.ratelimiting.config.service.v2.DatatypeCondition.DatatypeMatching;
import ai.traceable.ratelimiting.config.service.v2.DatatypeCondition.RegexBasedMatching;
import ai.traceable.ratelimiting.config.service.v2.EnvironmentScope;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingModsecRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingModsecRulesFilter.RuleAction;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRuleModsecRulesResponse;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.MatchOperator;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.StringCondition;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.Type;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.ModsecRuleIdInfo;
import ai.traceable.ratelimiting.config.service.v2.ModsecRuleIdInfo.IdType;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingModsecRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RegionCondition;
import ai.traceable.ratelimiting.config.service.v2.RegionCondition.Region;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition.EntityScope;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition.EntityType;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition.UrlScope;
import ai.traceable.ratelimiting.config.service.v2.ThresholdActionConfig;
import ai.traceable.ratelimiting.config.service.v2.TransactionActionConfig;
import ai.traceable.ratelimiting.service.v2.rules.modsec.RateLimitingModsecRulesManager;
import ai.traceable.ratelimiting.service.v2.rules.modsec.converters.DataTypeRuleModsecConverter;
import ai.traceable.ratelimiting.service.v2.rules.modsec.converters.ModsecBlobConverterUtils;
import ai.traceable.ratelimiting.service.v2.rules.modsec.converters.ModsecBlobDataConverter;
import ai.traceable.ratelimiting.service.v2.rules.modsec.converters.ScopedPatternConverter;
import ai.traceable.ratelimiting.service.v2.rules.modsec.converters.ScopedPatternWithCustomLocationConverter;
import ai.traceable.ratelimiting.service.v2.rules.modsec.datatype.DataClassificationInfoProvider;
import ai.traceable.ratelimiting.service.v2.rules.modsec.datatype.DataClassificationInfoProvider.DataClassificationInfo;
import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RateLimitingModsecRulesManagerTest {
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("tenant");
  private static final String DIRECTIVES = "SecRuleEngine DetectionOnly";
  private static final String ENVIRONMENT = "env";

  RateLimitingModsecRulesManager rateLimitingModsecRulesManager;
  @Mock Clock clock;
  @Mock Function<GetRateLimitingRulesFilter, List<RateLimitingRule>> rateLimitingRulesSupplier;
  @Mock ModsecRulesRegistry modsecRulesRegistry;
  @Mock DataClassificationInfoProvider dataClassificationInfoProvider;
  private static final UuidGenerator uuidGenerator = new UuidGenerator();

  @BeforeEach
  void setup() {
    when(modsecRulesRegistry.getModsecHeader(
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE))
        .thenReturn(DIRECTIVES);
    when(clock.millis()).thenReturn(1000L);

    DataClassificationInfo dataClassificationInfo = mock(DataClassificationInfo.class);
    doReturn(dataClassificationInfo)
        .when(dataClassificationInfoProvider)
        .fetchDataClassificationInfo(REQUEST_CONTEXT);

    doReturn(List.of("PAN")).when(dataClassificationInfo).getDataTypeIdsForDataSet("PII-Codex");

    doReturn(
            DataTypeRule.newBuilder()
                .setName("Credit Card")
                .addScopedPatterns(buildScopedPatternCreditCard())
                .build())
        .when(dataClassificationInfo)
        .getDataTypeRule("credit-card");

    doReturn(
            DataTypeRule.newBuilder()
                .setName("PAN Card")
                .addScopedPatterns(buildScopedPatternPANCard())
                .build())
        .when(dataClassificationInfo)
        .getDataTypeRule("PAN");

    ModsecBlobConverterUtils blobConverterUtils =
        new ModsecBlobConverterUtils(new ModsecRuleConversion(new ModsecRuleMappings()));

    rateLimitingModsecRulesManager =
        new RateLimitingModsecRulesManager(
            new ModsecBlobDataConverter(
                blobConverterUtils,
                new DataTypeRuleModsecConverter(
                    blobConverterUtils,
                    new ScopedPatternConverter(),
                    new ScopedPatternWithCustomLocationConverter(blobConverterUtils))),
            modsecRulesRegistry,
            dataClassificationInfoProvider,
            clock);
  }

  @Test
  void testFinalModsecRule() {
    doReturn(rateLimitingRules)
        .when(rateLimitingRulesSupplier)
        .apply(
            GetRateLimitingRulesFilter.newBuilder()
                .addCategories(Category.CATEGORY_DATA_EXFILTRATION)
                .setScope(
                    RuleConfigScope.newBuilder()
                        .setEnvironmentScope(
                            EnvironmentScope.newBuilder().addEnvironmentIds(ENVIRONMENT)))
                .setDisabled(false)
                .build());

    GetRateLimitingRuleModsecRulesResponse response =
        rateLimitingModsecRulesManager.getRateLimitingModsecRules(
            REQUEST_CONTEXT,
            GetRateLimitingModsecRulesFilter.newBuilder()
                .addCategories(Category.CATEGORY_DATA_EXFILTRATION)
                .setScope(
                    RuleConfigScope.newBuilder()
                        .setEnvironmentScope(
                            EnvironmentScope.newBuilder().addEnvironmentIds(ENVIRONMENT)))
                .setDisabled(false)
                .addRuleActions(RuleAction.RULE_ACTION_TRANSACTION_BLOCKED)
                .addRuleActions(RuleAction.RULE_ACTION_TRANSACTION_ALLOWED)
                .addServiceNames("serviceName1")
                .addServiceNames("serviceName2")
                .addServiceNames("serviceName3")
                .build(),
            rateLimitingRulesSupplier);

    assertEquals(DIRECTIVES, response.getModsecDirectivesBlob());

    assertEquals(2, response.getModsecBlobsDataCount());
    assertContainsModsec(expectedModsecBlobRuleId1, response.getModsecBlobsData(0).getModsecBlob());
    assertContainsModsec(expectedModsecBlobRuleId4, response.getModsecBlobsData(0).getModsecBlob());
    assertContainsModsec(
        expectedCreditCardWithoutCustomLocation, response.getModsecBlobsData(0).getModsecBlob());
    assertContainsModsec(
        expectedCreditCardWithCustomLocation("/order/.*|/myOrders|/pastOrders"),
        response.getModsecBlobsData(0).getModsecBlob());

    assertContainsModsec(expectedModsecBlobRuleId4, response.getModsecBlobsData(1).getModsecBlob());
    assertContainsModsec(
        expectedCreditCardWithCustomLocation("/order/.*|/pastOrders"),
        response.getModsecBlobsData(1).getModsecBlob());

    assertEquals(List.of("serviceName1"), response.getModsecBlobsData(0).getServiceNamesList());
    assertEqualsIgnoringOrder(
        List.of("rule-id-1", "rule-id-4"), response.getModsecBlobsData(0).getRuleIdsList());
    assertEquals(
        List.of("serviceName2", "serviceName3"),
        response.getModsecBlobsData(1).getServiceNamesList());
    assertEqualsIgnoringOrder(
        List.of("rule-id-4"), response.getModsecBlobsData(1).getRuleIdsList());

    // Match rules
    assertEquals(2, response.getRulesCount());

    assertEqualsIgnoringOrder(
        List.of(
            buildRateLimitingModsecRule(
                rateLimitingRules.get(0),
                List.of("credit-card:" + EMPTY_LOCATION_HASH, "PAN:" + EMPTY_LOCATION_HASH)),
            buildRateLimitingModsecRule(
                rateLimitingRules.get(3),
                List.of("credit-card:" + PROMPT_LOCATION_HASH, "PAN:" + PROMPT_LOCATION_HASH))),
        response.getRulesList());
  }

  private static void assertContainsModsec(List<String> expectedRuleParts, String modsecBlob) {
    assertDoesNotThrow(
        () ->
            expectedRuleParts.forEach(
                expectedRulePart -> assertTrue(modsecBlob.contains(expectedRulePart))),
        String.format("Unable to find %s in blob %s", expectedRuleParts, modsecBlob));
  }

  private static <T> void assertEqualsIgnoringOrder(Collection<T> expected, Collection<T> actual) {
    assertEquals(
        expected.stream().collect(Collectors.toUnmodifiableSet()),
        actual.stream().collect(Collectors.toUnmodifiableSet()));
  }

  private static final RegexBasedMatching PROMPT_LOCATION =
      RegexBasedMatching.newBuilder()
          .setCustomMatchingLocation(
              KeyValueCondition.newBuilder()
                  .setType(Type.TYPE_REQUEST_BODY_PARAMETER)
                  .setKeyCondition(
                      StringCondition.newBuilder()
                          .setValue("prompt")
                          .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)))
          .build();
  private static final List<RateLimitingRule> rateLimitingRules =
      List.of(
          RateLimitingRule.newBuilder()
              .setId("rule-id-1")
              .setData(
                  RateLimitingRuleData.newBuilder()
                      .setName("rule-name-1")
                      .setCategory(Category.CATEGORY_DATA_EXFILTRATION)
                      .setCondition(
                          Condition.newBuilder()
                              .setCompositeCondition(
                                  CompositeCondition.newBuilder()
                                      .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                                      .addChildren(
                                          buildKeyValueCondition(Type.TYPE_REQUEST_BODY_PARAMETER))
                                      .addChildren(
                                          buildUrlCondition(List.of("/order/.*", "/myOrders")))
                                      .addChildren(buildDataTypeCondition(false))
                                      .addChildren(buildServiceCondition(List.of("serviceName1")))))
                      .setTransactionActionConfig(
                          TransactionActionConfig.newBuilder()
                              .setAction(
                                  Action.newBuilder().setAllow(Allow.getDefaultInstance()).build())
                              .setExpirationTimestampMillis(1001L)))
              .build(),
          // Expired rule
          RateLimitingRule.newBuilder()
              .setId("rule-id-2")
              .setData(
                  RateLimitingRuleData.newBuilder()
                      .setName("rule-name-2")
                      .setCategory(Category.CATEGORY_DATA_EXFILTRATION)
                      .setTransactionActionConfig(
                          TransactionActionConfig.newBuilder()
                              .setAction(
                                  Action.newBuilder().setAllow(Allow.getDefaultInstance()).build())
                              .setExpirationTimestampMillis(999L)))
              .build(),
          RateLimitingRule.newBuilder()
              .setId("rule-id-3")
              .setData(
                  RateLimitingRuleData.newBuilder()
                      .setName("rule-name-3")
                      .setCategory(Category.CATEGORY_DATA_EXFILTRATION)
                      .addThresholdActionConfigs(ThresholdActionConfig.getDefaultInstance()))
              .build(),
          RateLimitingRule.newBuilder()
              .setId("rule-id-4")
              .setData(
                  RateLimitingRuleData.newBuilder()
                      .setName("rule-name-4")
                      .setCategory(Category.CATEGORY_DATA_EXFILTRATION)
                      .setCondition(
                          Condition.newBuilder()
                              .setCompositeCondition(
                                  CompositeCondition.newBuilder()
                                      .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                                      .addChildren(buildRegionCondition("Nepal"))
                                      .addChildren(
                                          buildKeyValueCondition(Type.TYPE_QUERY_PARAMETER))
                                      .addChildren(buildDataTypeCondition(true))
                                      .addChildren(
                                          buildUrlCondition(List.of("/order/.*", "/pastOrders")))))
                      .setTransactionActionConfig(
                          TransactionActionConfig.newBuilder()
                              .setAction(
                                  Action.newBuilder()
                                      .setBlock(Block.getDefaultInstance())
                                      .build())))
              .build());

  private static RateLimitingModsecRule buildRateLimitingModsecRule(
      RateLimitingRule rule, List<String> dataTypeModsecIds) {
    return RateLimitingModsecRule.newBuilder()
        .setId(rule.getId())
        .setData(rule.getData())
        .addAssociatedModsecRuleIds(
            ModsecRuleIdInfo.newBuilder()
                .addMatchingIds(rule.getId())
                .setType(IdType.ID_TYPE_KEY_VALUE_CONDITION_URL_REGEXES))
        .addAssociatedModsecRuleIds(
            ModsecRuleIdInfo.newBuilder()
                .addAllMatchingIds(dataTypeModsecIds)
                .setType(IdType.ID_TYPE_DATA_TYPE_CUSTOM_LOCATION))
        .build();
  }

  private static Condition buildServiceCondition(List<String> serviceNames) {
    return Condition.newBuilder()
        .setLeafCondition(
            LeafCondition.newBuilder()
                .setScopeCondition(
                    ScopeCondition.newBuilder()
                        .setEntityScope(
                            EntityScope.newBuilder()
                                .addAllEntityIds(serviceNames)
                                .setEntityType(EntityType.ENTITY_TYPE_SERVICE))))
        .build();
  }

  private static Condition buildDataTypeCondition(boolean customLocation) {
    DatatypeCondition.Builder datatypeConditionBuilder =
        DatatypeCondition.newBuilder()
            .addDatasetIds("PII-Codex")
            .addDatatypeIds("credit-card")
            .setDataLocation(DataLocation.DATA_LOCATION_REQUEST);

    if (customLocation) {
      datatypeConditionBuilder.setDatatypeMatching(
          DatatypeMatching.newBuilder().setRegexBasedMatching(PROMPT_LOCATION).build());
    }

    return Condition.newBuilder()
        .setLeafCondition(LeafCondition.newBuilder().setDatatypeCondition(datatypeConditionBuilder))
        .build();
  }

  private static Condition buildUrlCondition(List<String> urlRegexes) {
    return Condition.newBuilder()
        .setLeafCondition(
            LeafCondition.newBuilder()
                .setScopeCondition(
                    ScopeCondition.newBuilder()
                        .setUrlScope(UrlScope.newBuilder().addAllUrlRegexes(urlRegexes))))
        .build();
  }

  private static Condition buildKeyValueCondition(Type type) {
    return Condition.newBuilder()
        .setLeafCondition(
            LeafCondition.newBuilder()
                .setKeyValueCondition(
                    KeyValueCondition.newBuilder()
                        .setType(type)
                        .setKeyCondition(
                            StringCondition.newBuilder()
                                .setValue("orderId")
                                .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS))
                        .setValueCondition(
                            StringCondition.newBuilder()
                                .setValue("123.*")
                                .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX))))
        .build();
  }

  private static Condition buildRegionCondition(String region) {
    return Condition.newBuilder()
        .setLeafCondition(
            LeafCondition.newBuilder()
                .setRegionCondition(
                    RegionCondition.newBuilder()
                        .addRegionIdentifiers(Region.newBuilder().setCountryIsoCode(region))))
        .build();
  }

  private static ScopedPattern buildScopedPatternCreditCard() {
    return ScopedPattern.newBuilder()
        .setGlobalScope(GlobalScope.getDefaultInstance())
        .addLocations(Location.LOCATION_REQUEST_BODY)
        .setAction(DataTypeRule.Action.ACTION_MATCH)
        .setKeyValuePattern(
            KeyValuePattern.newBuilder()
                .setKeyPattern(
                    StringPattern.newBuilder().setValue("cc").setOperator(Operator.OPERATOR_EQUALS))
                .setValuePattern(
                    StringPattern.newBuilder()
                        .setValue("^5554$")
                        .setOperator(Operator.OPERATOR_MATCHES_REGEX)))
        .build();
  }

  private static ScopedPattern buildScopedPatternPANCard() {
    return ScopedPattern.newBuilder()
        .setGlobalScope(GlobalScope.getDefaultInstance())
        .addLocations(Location.LOCATION_ANY)
        .setAction(DataTypeRule.Action.ACTION_MATCH)
        .setKeyValuePattern(
            KeyValuePattern.newBuilder()
                .setKeyPattern(
                    StringPattern.newBuilder()
                        .setValue(
                            "^(?i)(?:Permanent[ \\-_\\+]?Account[ \\-_\\+]?Number|pan[ \\-_\\+]?n(?:umber|o))$")
                        .setOperator(Operator.OPERATOR_MATCHES_REGEX))
                .setValuePattern(
                    StringPattern.newBuilder()
                        .setValue("^(?i)[A-Z]{3}[ABCFGHLJPTF]{1}[A-Z]{1}[0-9]{4}[A-Z]{1}$")
                        .setOperator(Operator.OPERATOR_MATCHES_REGEX)))
        .build();
  }

  private static final String EMPTY_LOCATION_HASH =
      uuidGenerator.generateId(RegexBasedMatching.getDefaultInstance());
  private static final String PROMPT_LOCATION_HASH = uuidGenerator.generateId(PROMPT_LOCATION);

  private static final List<String> expectedModsecBlobRuleId1 =
      List.of(
          "SecRule REQUEST_URI_RAW \"@rx /order/.*|/myOrders\" " + "\"id:200000",
          ",phase:2,capture,t:none,"
              + "msg:'URL Regex and key-value conditions corresponding to DLP Rule - rule-id-1',"
              + "logdata:'Matched URL and request criteria corresponding to DLP Rule',"
              + "tag:'CUSTOM_SIGNATURE',tag:'paranoia-level/1',tag:'rule-uuid/rule-id-1',severity:'CRITICAL',chain\"\n"
              + "SecRule ARGS_POST:orderId \"@rx 123.*\" \"capture,block,t:none\"");

  private static final List<String> expectedModsecBlobRuleId4 =
      List.of(
          "SecRule REQUEST_URI_RAW \"@rx /order/.*|/pastOrders\" " + "\"id:200000",
          ",phase:2,capture,t:none,"
              + "msg:'URL Regex and key-value conditions corresponding to DLP Rule - rule-id-4',"
              + "logdata:'Matched URL and request criteria corresponding to DLP Rule',"
              + "tag:'CUSTOM_SIGNATURE',tag:'paranoia-level/1',tag:'rule-uuid/rule-id-4',severity:'CRITICAL',chain\"\n"
              + "SecRule ARGS_GET:orderId \"@rx 123.*\" \"capture,block,t:none\"");

  private static final List<String> expectedCreditCardWithoutCustomLocation =
      List.of(
          "SecRule REQUEST_URI_RAW \"@rx /order/.*|/myOrders|/pastOrders\" \"id:200000",
          ",phase:2,capture,t:none,"
              + "msg:'Data type rule - credit-card ',"
              + "logdata:'Matched data-type - Credit Card',"
              + "tag:'CUSTOM_SIGNATURE',tag:'paranoia-level/1',tag:'rule-uuid/credit-card:"
              + EMPTY_LOCATION_HASH
              + "',severity:'CRITICAL',chain\"\n"
              + "SecRule ARGS_POST:/^(.*[.])?cc([.].*)?$/ \"@rx ^5554$\" \"capture,block,t:none\"");

  private static List<String> expectedCreditCardWithCustomLocation(String urlRegexes) {
    return List.of(
        String.format("SecRule REQUEST_URI_RAW \"@rx %s\" \"id:200000", urlRegexes),
        ",phase:2,capture,t:none,"
            + "msg:'Data type rule - credit-card at custom location',"
            + "logdata:'Matched data-type - Credit Card',"
            + "tag:'CUSTOM_SIGNATURE',tag:'paranoia-level/1',tag:'rule-uuid/credit-card:"
            + PROMPT_LOCATION_HASH
            + "',severity:'CRITICAL',chain\"\n"
            + "SecRule ARGS_POST:prompt \"@rx cc.*5554|5554.*cc\" \"capture,block,t:none\"");
  }
}
