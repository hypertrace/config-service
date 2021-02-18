package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.ratelimiting.config.service.v1.CreateRateLimitingRuleConfig;
import ai.traceable.ratelimiting.config.service.v1.CreateRuleConfigRequest;
import ai.traceable.ratelimiting.config.service.v1.CreateRuleRateLimitedEntityAssociationRequest;
import ai.traceable.ratelimiting.config.service.v1.DeleteRuleConfigRequest;
import ai.traceable.ratelimiting.config.service.v1.DeleteRuleRateLimitedEntityAssociationRequest;
import ai.traceable.ratelimiting.config.service.v1.GetAllRateLimitingRulesRequest;
import ai.traceable.ratelimiting.config.service.v1.GetRateLimitingConfigsForEntityRequest;
import ai.traceable.ratelimiting.config.service.v1.RateLimitedEntity;
import ai.traceable.ratelimiting.config.service.v1.RateLimitedEntityType;
import ai.traceable.ratelimiting.config.service.v1.RateLimitingConfigServiceGrpc;
import ai.traceable.ratelimiting.config.service.v1.RateLimitingConfigServiceGrpc.RateLimitingConfigServiceBlockingStub;
import ai.traceable.ratelimiting.config.service.v1.RateLimitingRuleConfig;
import ai.traceable.ratelimiting.config.service.v1.RateLimitingRuleWithRateLimitedEntities;
import ai.traceable.ratelimiting.config.service.v1.RuleViolationAction;
import ai.traceable.ratelimiting.config.service.v1.UpdateRuleConfigRequest;
import ai.traceable.sensitivedata.config.service.v1.CreateRedactionRuleRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAutomaticSecretRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.GetPiiFilterConfigRequest;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyForTypeRequest;
import ai.traceable.sensitivedata.config.service.v1.MatchType;
import ai.traceable.sensitivedata.config.service.v1.NewRedactionRule;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.PiiElement;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfigServiceGrpc.PiiFilterConfigServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.UpdateAutomaticSecretRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionStrategyForTypeRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.serviceframework.IntegrationTestServerUtil;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.IntegrationTestConfigClientFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration test for {@link TraceableConfigService} */
public class TraceableConfigServiceIntegrationTest {

  private static final String SERVICE_NAME = "traceable-config-service";
  private static final String DEFAULT_PII_FILTER_CONFIG =
      "sensitive.data.config.service.default.pii.filter.config";
  private static final String DATA_STORE_COLLECTION = "configurations";
  private static final Collection CONFIGURATIONS_COLLECTION = getConfigurationsCollection();

  private static SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceStub;
  private static PiiFilterConfigServiceBlockingStub piiFilterConfigServiceStub;
  private static RateLimitingConfigServiceBlockingStub rateLimitingConfigServiceStub;
  private static Server mockInsightsServer;
  private static ManagedChannel managedChannelForInternalServices;
  private static ManagedChannel managedChannelForExternalServices;

  @BeforeAll
  public static void setup() throws IOException {
    System.out.println("Starting Config Service E2E Test");
    IntegrationTestServerUtil.startServices(new String[] {SERVICE_NAME});

    managedChannelForInternalServices =
        ManagedChannelBuilder.forAddress("localhost", 50101).usePlaintext().build();
    managedChannelForExternalServices =
        ManagedChannelBuilder.forAddress("localhost", 50102).usePlaintext().build();

    sensitiveDataConfigServiceStub =
        SensitiveDataConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    piiFilterConfigServiceStub =
        PiiFilterConfigServiceGrpc.newBlockingStub(managedChannelForExternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    rateLimitingConfigServiceStub =
        RateLimitingConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    mockInsightsServer =
        ServerBuilder.forPort(50098).addService(new MockInsightsService()).build().start();
  }

  @AfterAll
  public static void teardown() {
    managedChannelForInternalServices.shutdown();
    managedChannelForExternalServices.shutdown();
    IntegrationTestServerUtil.shutdownServices();
    mockInsightsServer.shutdown();
  }

  // Need to delete the collection after each test for stateless integration testing
  @AfterEach
  public void delete() {
    CONFIGURATIONS_COLLECTION.deleteAll();
  }

  private static Collection getConfigurationsCollection() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("host", "localhost");
    configMap.put("port", "27017");
    Datastore datastore =
        DatastoreProvider.getDatastore("mongo", ConfigFactory.parseMap(configMap));
    return datastore.getCollection(DATA_STORE_COLLECTION);
  }

  @Test
  void testSensitiveDataConfigService() {
    assertEquals(
        RedactionStrategy.REDACTION_STRATEGY_RAW,
        getRedactionStrategyForType(ParamType.PARAM_TYPE_HEADER));
    updateRedactionStrategyForType(
        ParamType.PARAM_TYPE_HEADER, RedactionStrategy.REDACTION_STRATEGY_HASH);
    assertEquals(
        RedactionStrategy.REDACTION_STRATEGY_HASH,
        getRedactionStrategyForType(ParamType.PARAM_TYPE_HEADER));

    assertEquals(true, getAutomaticSecretRedactionStrategy());
    updateAutomaticSecretRedactionStrategy(false);
    assertEquals(false, getAutomaticSecretRedactionStrategy());
  }

  @Test
  void testPiiFilterConfigService() throws InvalidProtocolBufferException {
    // automatic secret redaction is disabled and redaction strategy is set to RAW(default)
    updateAutomaticSecretRedactionStrategy(false);
    assertEquals(getExpectedPiiFilterConfig(List.of(), false), getPiiFilterConfig());

    // set redaction strategy to HASH
    updateRedactionStrategyForType(
        ParamType.PARAM_TYPE_HEADER, RedactionStrategy.REDACTION_STRATEGY_HASH);
    PiiElement piiElement1 =
        getPiiElement(
            "http.request.header.h1", "", RedactionStrategy.REDACTION_STRATEGY_HASH, true);
    PiiElement piiElement2 =
        getPiiElement(
            "http.request.header.h2", "", RedactionStrategy.REDACTION_STRATEGY_HASH, true);
    PiiFilterConfig expected = getExpectedPiiFilterConfig(List.of(piiElement1, piiElement2), false);
    PiiFilterConfig actual = getPiiFilterConfig();
    assertEquals(expected, actual);

    // enable automatic secret redaction
    updateAutomaticSecretRedactionStrategy(true);
    expected = getExpectedPiiFilterConfig(List.of(piiElement1, piiElement2), true);
    actual = getPiiFilterConfig();
    assertEquals(expected, actual);

    // add redaction rule
    createRedactionRule(
        getNewRedactionRule("rule-1", RedactionStrategy.REDACTION_STRATEGY_REDACT, "^name"));
    createRedactionRule(
        getNewRedactionRule("rule-2", RedactionStrategy.REDACTION_STRATEGY_HASH, "^address"));
    PiiElement piiElement3 =
        getPiiElement("^name", "pii", RedactionStrategy.REDACTION_STRATEGY_REDACT, false);
    PiiElement piiElement4 =
        getPiiElement("^address", "pii", RedactionStrategy.REDACTION_STRATEGY_HASH, false);
    expected =
        getExpectedPiiFilterConfig(
            List.of(piiElement4, piiElement3, piiElement1, piiElement2), true);
    actual = getPiiFilterConfig();
    assertEquals(expected, actual);
  }

  private void updateRedactionStrategyForType(
      ParamType paramType, RedactionStrategy redactionStrategy) {
    UpdateRedactionStrategyForTypeRequest request =
        UpdateRedactionStrategyForTypeRequest.newBuilder()
            .setParamType(paramType)
            .setRedactionStrategy(redactionStrategy)
            .build();
    GrpcClientRequestContextUtil.executeInTenantContext(
        "tenant1", () -> sensitiveDataConfigServiceStub.updateRedactionStrategyForType(request));
  }

  private RedactionStrategy getRedactionStrategyForType(ParamType paramType) {
    GetRedactionStrategyForTypeRequest request =
        GetRedactionStrategyForTypeRequest.newBuilder().setParamType(paramType).build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
            "tenant1", () -> sensitiveDataConfigServiceStub.getRedactionStrategyForType(request))
        .getRedactionStrategy();
  }

  private void updateAutomaticSecretRedactionStrategy(boolean enabled) {
    UpdateAutomaticSecretRedactionStrategyRequest request =
        UpdateAutomaticSecretRedactionStrategyRequest.newBuilder().setEnabled(enabled).build();
    GrpcClientRequestContextUtil.executeInTenantContext(
        "tenant1",
        () -> sensitiveDataConfigServiceStub.updateAutomaticSecretRedactionStrategy(request));
  }

  private boolean getAutomaticSecretRedactionStrategy() {
    GetAutomaticSecretRedactionStrategyRequest request =
        GetAutomaticSecretRedactionStrategyRequest.newBuilder().build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
            "tenant1",
            () -> sensitiveDataConfigServiceStub.getAutomaticSecretRedactionStrategy(request))
        .getEnabled();
  }

  private RedactionRule createRedactionRule(NewRedactionRule newRedactionRule) {
    CreateRedactionRuleRequest request =
        CreateRedactionRuleRequest.newBuilder().setNewRedactionRule(newRedactionRule).build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
            "tenant1", () -> sensitiveDataConfigServiceStub.createRedactionRule(request))
        .getRedactionRule();
  }

  private NewRedactionRule getNewRedactionRule(
      String name, RedactionStrategy redactionStrategy, String regex) {
    return NewRedactionRule.newBuilder()
        .setName(name)
        .setDescription("sample rule")
        .setCategory("pii")
        .setRedactionStrategy(redactionStrategy)
        .setMatchType(MatchType.MATCH_TYPE_KEY)
        .setRegex(regex)
        .build();
  }

  private PiiFilterConfig getPiiFilterConfig() {
    GetPiiFilterConfigRequest request = GetPiiFilterConfigRequest.newBuilder().build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
            "tenant1", () -> piiFilterConfigServiceStub.getPiiFilterConfig(request))
        .getPiiFilterConfig();
  }

  private PiiElement getPiiElement(
      String regex, String category, RedactionStrategy redactionStrategy, boolean isFqn) {
    return PiiElement.newBuilder()
        .setRegex(regex)
        .setCategory(category)
        .setRedactionStrategy(redactionStrategy)
        .setFqn(isFqn)
        .build();
  }

  private PiiFilterConfig getExpectedPiiFilterConfig(
      List<PiiElement> piiElementsAddedByUser, boolean automaticSecretRedactionEnabled)
      throws InvalidProtocolBufferException {
    ConfigClient configClient =
        IntegrationTestConfigClientFactory.getConfigClientForService(SERVICE_NAME);
    Config piiFilterConfig = configClient.getConfig().getConfig(DEFAULT_PII_FILTER_CONFIG);
    String jsonString = piiFilterConfig.root().render(ConfigRenderOptions.concise());
    PiiFilterConfig.Builder builder = PiiFilterConfig.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    PiiFilterConfig defaultPiiFilterConfig = builder.build();
    PiiFilterConfig.Builder expectedPiiFilterConfigBuilder =
        PiiFilterConfig.newBuilder()
            .addAllPrefixes(defaultPiiFilterConfig.getPrefixesList())
            .setRedactionStrategy(defaultPiiFilterConfig.getRedactionStrategy());
    expectedPiiFilterConfigBuilder.addAllKeyRegexs(piiElementsAddedByUser);
    if (automaticSecretRedactionEnabled) {
      expectedPiiFilterConfigBuilder.addAllKeyRegexs(defaultPiiFilterConfig.getKeyRegexsList());
      expectedPiiFilterConfigBuilder.addAllValueRegexs(defaultPiiFilterConfig.getValueRegexsList());
      expectedPiiFilterConfigBuilder.addAllComplexData(defaultPiiFilterConfig.getComplexDataList());
    }
    return expectedPiiFilterConfigBuilder.build();
  }

  @Test
  void testRateLimitingConfig() {
    // Create rule1
    CreateRateLimitingRuleConfig createRateLimitingRuleConfigOne =
        CreateRateLimitingRuleConfig.newBuilder()
            .setRuleName("rule1")
            .setDescription("this is rule 1")
            .setMaxCallCountAllowed(10L)
            .setMaxCallCountDurationMillis(10000L)
            .setRuleViolationAction(RuleViolationAction.RULE_VIOLATION_ACTION_SUSPEND)
            .setSuspendDurationMillis(1000L)
            .build();
    RateLimitingRuleConfig createdRateLimitRuleConfigOne =
        createRateLimitingRuleConfig(createRateLimitingRuleConfigOne);
    String ruleId1 = createdRateLimitRuleConfigOne.getRuleId();

    // Create rule2
    CreateRateLimitingRuleConfig createRateLimitingRuleConfigTwo =
        CreateRateLimitingRuleConfig.newBuilder()
            .setRuleName("rule2")
            .setDescription("this is rule 1")
            .setMaxCallCountAllowed(10L)
            .setMaxCallCountDurationMillis(100000L)
            .setRuleViolationAction(RuleViolationAction.RULE_VIOLATION_ACTION_SUSPEND)
            .setSuspendDurationMillis(1000L)
            .build();
    String ruleId2 = createRateLimitingRuleConfig(createRateLimitingRuleConfigTwo).getRuleId();

    List<RateLimitingRuleWithRateLimitedEntities> rulesWithEntities = getAllRateLimitingRule();

    // Assert two rules created
    assertEquals(2, rulesWithEntities.size());

    // Associate entity1 with rule1.
    RateLimitedEntity entityOne =
        RateLimitedEntity.newBuilder()
            .setEntityId("entity1")
            .setEntityType(RateLimitedEntityType.RATE_LIMITED_ENTITY_TYPE_API)
            .build();

    CreateRuleRateLimitedEntityAssociationRequest createAssociationRequestOne =
        CreateRuleRateLimitedEntityAssociationRequest.newBuilder()
            .setRuleId(ruleId1)
            .setEntity(entityOne)
            .build();

    GrpcClientRequestContextUtil.executeInTenantContext(
        "tenant1",
        () ->
            rateLimitingConfigServiceStub.createRuleRateLimitedEntityAssociation(
                createAssociationRequestOne));

    // Associate entity2 with rule1.
    RateLimitedEntity entityTwo =
        RateLimitedEntity.newBuilder()
            .setEntityId("entity2")
            .setEntityType(RateLimitedEntityType.RATE_LIMITED_ENTITY_TYPE_API)
            .build();

    CreateRuleRateLimitedEntityAssociationRequest createAssociationRequestTwo =
        CreateRuleRateLimitedEntityAssociationRequest.newBuilder()
            .setRuleId(ruleId1)
            .setEntity(entityTwo)
            .build();

    GrpcClientRequestContextUtil.executeInTenantContext(
        "tenant1",
        () ->
            rateLimitingConfigServiceStub.createRuleRateLimitedEntityAssociation(
                createAssociationRequestTwo));

    // Associate entity3 with rule2.
    RateLimitedEntity entityThree =
        RateLimitedEntity.newBuilder()
            .setEntityId("entity3")
            .setEntityType(RateLimitedEntityType.RATE_LIMITED_ENTITY_TYPE_API)
            .build();

    CreateRuleRateLimitedEntityAssociationRequest createAssociationRequestThree =
        CreateRuleRateLimitedEntityAssociationRequest.newBuilder()
            .setRuleId(ruleId2)
            .setEntity(entityThree)
            .build();

    GrpcClientRequestContextUtil.executeInTenantContext(
        "tenant1",
        () ->
            rateLimitingConfigServiceStub.createRuleRateLimitedEntityAssociation(
                createAssociationRequestThree));

    // After creating all the associations
    // rule1 -> entity1, entity2
    // rule2 -> entity3
    rulesWithEntities = getAllRateLimitingRule();
    assertRuleAssociations(rulesWithEntities.get(0), ruleId1, 2, 1);
    assertRuleAssociations(rulesWithEntities.get(1), ruleId1, 2, 1);

    // Verify rate limit config returned for specific entity
    Assertions.assertEquals(
        createdRateLimitRuleConfigOne, getRateLimitRuleConfigsForEntity(entityOne).get(0));
    // Verify no rate limit config for entity that isn't associated to any config
    RateLimitedEntity randomEntity =
        RateLimitedEntity.newBuilder()
            .setEntityId("random_entity")
            .setEntityType(RateLimitedEntityType.RATE_LIMITED_ENTITY_TYPE_API)
            .build();
    Assertions.assertTrue(getRateLimitRuleConfigsForEntity(randomEntity).isEmpty());

    // Delete association between entity3 and rule2
    DeleteRuleRateLimitedEntityAssociationRequest deleteRuleRateLimitedEntityAssociationRequest1 =
        DeleteRuleRateLimitedEntityAssociationRequest.newBuilder()
            .setEntity(entityThree)
            .setRuleId(ruleId2)
            .build();
    GrpcClientRequestContextUtil.executeInTenantContext(
        "tenant1",
        () ->
            rateLimitingConfigServiceStub.deleteRuleRateLimitedEntityAssociation(
                deleteRuleRateLimitedEntityAssociationRequest1));

    rulesWithEntities = getAllRateLimitingRule();
    // Assert association got deleted
    assertEquals(2, rulesWithEntities.size());
    assertRuleAssociations(rulesWithEntities.get(0), ruleId1, 2, 0);
    assertRuleAssociations(rulesWithEntities.get(1), ruleId1, 2, 0);

    // Update rule1 -> updatedRule1 and disable it too.
    RateLimitingRuleConfig updatedRuleConfig =
        RateLimitingRuleConfig.newBuilder()
            .setRuleId(ruleId1)
            .setRuleName("updatedRule1")
            .setDescription("this is rule 1")
            .setMaxCallCountAllowed(10L)
            .setMaxCallCountDurationMillis(100000L)
            .setRuleViolationAction(RuleViolationAction.RULE_VIOLATION_ACTION_SUSPEND)
            .setSuspendDurationMillis(1000L)
            .setDisabled(true)
            .build();

    UpdateRuleConfigRequest updateRuleConfigRequest =
        UpdateRuleConfigRequest.newBuilder().setRule(updatedRuleConfig).build();
    GrpcClientRequestContextUtil.executeInTenantContext(
        "tenant1", () -> rateLimitingConfigServiceStub.updateRuleConfig(updateRuleConfigRequest));

    // Assert rule update changed the fields
    rulesWithEntities = getAllRateLimitingRule();
    rulesWithEntities.stream()
        .filter(r -> r.getRule().getRuleId().equals(ruleId1))
        .forEach(
            r -> {
              assertEquals("updatedRule1", r.getRule().getRuleName());
              assertTrue(r.getRule().getDisabled());
            });

    // Delete rule1
    DeleteRuleConfigRequest deleteRuleConfigRequest =
        DeleteRuleConfigRequest.newBuilder().setRuleId(ruleId1).build();
    GrpcClientRequestContextUtil.executeInTenantContext(
        "tenant1", () -> rateLimitingConfigServiceStub.deleteRuleConfig(deleteRuleConfigRequest));
    // Assert rule1 got deleted
    rulesWithEntities = getAllRateLimitingRule();
    assertEquals(1, rulesWithEntities.size());
    assertEquals(ruleId2, rulesWithEntities.get(0).getRule().getRuleId());
  }

  private void assertRuleAssociations(
      RateLimitingRuleWithRateLimitedEntities ruleWithEntity,
      String ruleId1,
      long ruleId1ExpectedAssociationCount,
      long ruleId2ExpectedAssociationCount) {
    if (ruleWithEntity.getRule().getRuleId().equals(ruleId1)) {
      assertEquals(
          ruleId1ExpectedAssociationCount, ruleWithEntity.getEntitiesAssociatedList().size());
    } else {
      assertEquals(
          ruleId2ExpectedAssociationCount, ruleWithEntity.getEntitiesAssociatedList().size());
    }
  }

  private RateLimitingRuleConfig createRateLimitingRuleConfig(CreateRateLimitingRuleConfig config) {
    CreateRuleConfigRequest request = CreateRuleConfigRequest.newBuilder().setRule(config).build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
            "tenant1", () -> rateLimitingConfigServiceStub.createRuleConfig(request))
        .getRule();
  }

  private List<RateLimitingRuleWithRateLimitedEntities> getAllRateLimitingRule() {
    GetAllRateLimitingRulesRequest request = GetAllRateLimitingRulesRequest.newBuilder().build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
            "tenant1", () -> rateLimitingConfigServiceStub.getAllRateLimitingRules(request))
        .getRulesList();
  }

  private List<RateLimitingRuleConfig> getRateLimitRuleConfigsForEntity(
      RateLimitedEntity rateLimitedEntity) {
    GetRateLimitingConfigsForEntityRequest request =
        GetRateLimitingConfigsForEntityRequest.newBuilder().setEntity(rateLimitedEntity).build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
        "tenant1",
        () -> rateLimitingConfigServiceStub.getRateLimitingConfigsForEntity(request).getRuleList());
  }
}
