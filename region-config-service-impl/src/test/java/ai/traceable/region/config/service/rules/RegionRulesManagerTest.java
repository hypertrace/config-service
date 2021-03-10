package ai.traceable.region.config.service.rules;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.region.config.service.utils.UuidGenerator;
import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.RegionRule;
import com.google.common.collect.ImmutableSortedMap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RegionRulesManagerTest {
  String REGION_RULE_CONFIG_NAMESPACE = "regionRule";
  String REGION_RULE_CONFIG_RESOURCE_NAME = "regionRuleConfig";

  private MockGenericConfigService mockConfigService;
  private ConfigServiceBlockingStub configServiceBlockingStub;
  private RegionRuleConverter regionRuleConverter;
  private UuidGenerator uuidGenerator;

  private RegionRulesManager rulesManager;

  @BeforeEach
  void setup() {
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    configServiceBlockingStub = ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    regionRuleConverter = mock(RegionRuleConverter.class);
    uuidGenerator = mock(UuidGenerator.class);
    this.rulesManager =
        new RegionRulesManager(configServiceBlockingStub, regionRuleConverter, uuidGenerator);
  }

  @AfterEach
  void teardown() {
    mockConfigService.shutdown();
  }

  @Nested
  class GetAllRegionRules {
    @Test
    void shouldGetAllRegionRules() throws InvalidProtocolBufferException {
      Value mockRegionRuleConfig1 = mockRuleConfig("id-1", "name-1");
      Value mockRegionRuleConfig2 = mockRuleConfig("id-2", "name-2");

      addRegionRules(
          ImmutableSortedMap.of("id-1", mockRegionRuleConfig1, "id-2", mockRegionRuleConfig2));

      RegionRule regionRule1 = RegionRule.newBuilder().setId("id-1").setName("name-1").build();
      RegionRule regionRule2 = RegionRule.newBuilder().setId("id-2").setName("name-2").build();
      when(regionRuleConverter.convert(mockRegionRuleConfig1)).thenReturn(regionRule1);
      when(regionRuleConverter.convert(mockRegionRuleConfig2)).thenReturn(regionRule2);
      List<RegionRule> regionRules = rulesManager.getRegionRules();
      assertEquals(List.of(regionRule2, regionRule1), regionRules);
    }

    @Test
    void should_ignoreRegionRule_invalidProtocolBufferException()
        throws InvalidProtocolBufferException {
      Value mockRegionRuleConfig1 = mockRuleConfig("id-1", "name-1");
      Value mockRegionRuleConfig2 = mockRuleConfig("id-2", "name-2");

      addRegionRules(
          ImmutableSortedMap.of("id-1", mockRegionRuleConfig1, "id-2", mockRegionRuleConfig2));

      RegionRule regionRule1 = RegionRule.newBuilder().setId("id-1").setName("name-1").build();
      when(regionRuleConverter.convert(mockRegionRuleConfig1)).thenReturn(regionRule1);
      when(regionRuleConverter.convert(mockRegionRuleConfig2))
          .thenThrow(InvalidProtocolBufferException.class);
      List<RegionRule> regionRules = rulesManager.getRegionRules();
      assertEquals(List.of(regionRule1), regionRules);
    }
  }

  @Nested
  class CreateRegionRule {

    @Test
    void shouldCreateRegionRule() throws InvalidProtocolBufferException {
      when(uuidGenerator.generateId()).thenReturn("id-1");
      RegionRule regionRule = RegionRule.newBuilder().setId("id-1").setName("name-1").build();
      Value ruleConfig = mockRuleConfig("id-1", "name-1");
      when(regionRuleConverter.convert(regionRule)).thenReturn(ruleConfig);
      when(regionRuleConverter.convert(ruleConfig)).thenReturn(regionRule);

      Optional<RegionRule> maybeCreatedRegionRule =
          rulesManager.createRegionRule(
              CreateRegionRuleRequest.newBuilder().setName("name-1").build());
      assertTrue(maybeCreatedRegionRule.isPresent());
      assertEquals(regionRule, maybeCreatedRegionRule.get());
    }

    @Test
    void should_notCreateRegionRule_invalidRegionRuleConversion()
        throws InvalidProtocolBufferException {
      when(uuidGenerator.generateId()).thenReturn("id-1");
      RegionRule regionRule = RegionRule.newBuilder().setId("id-1").build();
      when(regionRuleConverter.convert(regionRule)).thenThrow(InvalidProtocolBufferException.class);

      Optional<RegionRule> maybeCreatedRegionRule =
          rulesManager.createRegionRule(CreateRegionRuleRequest.getDefaultInstance());
      assertTrue(maybeCreatedRegionRule.isEmpty());
    }

    @Test
    void should_notCreateRegionRule_invalidRegionRuleConfigConversion()
        throws InvalidProtocolBufferException {
      when(uuidGenerator.generateId()).thenReturn("id-1");
      RegionRule regionRule = RegionRule.newBuilder().setId("id-1").setName("name-1").build();
      Value ruleConfig = mockRuleConfig("id-1", "name-1");
      when(regionRuleConverter.convert(regionRule)).thenReturn(ruleConfig);
      when(regionRuleConverter.convert(ruleConfig)).thenThrow(InvalidProtocolBufferException.class);

      Optional<RegionRule> maybeCreatedRegionRule =
          rulesManager.createRegionRule(
              CreateRegionRuleRequest.newBuilder().setName("name-1").build());
      assertTrue(maybeCreatedRegionRule.isEmpty());
    }
  }

  private void addRegionRules(Map<String, Value> regionRuleConfigs) {
    regionRuleConfigs.forEach(
        (id, regionRuleConfig) ->
            configServiceBlockingStub.upsertConfig(
                UpsertConfigRequest.newBuilder()
                    .setResourceNamespace(REGION_RULE_CONFIG_NAMESPACE)
                    .setResourceName(REGION_RULE_CONFIG_RESOURCE_NAME)
                    .setConfig(regionRuleConfig)
                    .setContext(id)
                    .build()));
  }

  private Value mockRuleConfig(String id, String name) {
    Struct ruleConfigStruct =
        Struct.newBuilder()
            .putFields("id", Value.newBuilder().setStringValue(id).build())
            .putFields("name", Value.newBuilder().setStringValue(name).build())
            .build();
    return Value.newBuilder().setStructValue(ruleConfigStruct).build();
  }
}
