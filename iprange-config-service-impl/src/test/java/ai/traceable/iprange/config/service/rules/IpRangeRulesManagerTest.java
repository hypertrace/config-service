package ai.traceable.iprange.config.service.rules;

import static ai.traceable.iprange.config.service.constants.IpRangeConfigConstants.IPRANGE_RULE_CONFIG_NAMESPACE;
import static ai.traceable.iprange.config.service.constants.IpRangeConfigConstants.IPRANGE_RULE_CONFIG_RESOURCE_NAME;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.iprange.config.service.utils.IpValidationUtils;
import ai.traceable.iprange.config.service.utils.UuidGenerator;
import ai.traceable.iprange.config.service.v1.*;
import com.google.common.collect.ImmutableSortedMap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.*;

class IpRangeRulesManagerTest {

  private MockGenericConfigService mockConfigService;
  private ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;
  private IpRangeRuleConverter ipRangeRuleConverter;
  private UuidGenerator uuidGenerator;
  private IpRangeRulesManager rulesManager;
  private RequestContext requestContext;
  private Clock mockClock;

  @BeforeEach
  void setUp() {
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    configServiceBlockingStub = ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    ipRangeRuleConverter = mock(IpRangeRuleConverter.class);
    uuidGenerator = mock(UuidGenerator.class);
    IpValidationUtils ipValidationUtils = new IpValidationUtils();
    mockClock = mock(Clock.class);
    this.rulesManager =
        new IpRangeRulesManager(
            configServiceBlockingStub,
            ipRangeRuleConverter,
            uuidGenerator,
            ipValidationUtils,
            mockClock);
    requestContext = RequestContext.forTenantId("default tenant");
  }

  @AfterEach
  void teardown() {
    mockConfigService.shutdown();
  }

  @Nested
  class getIpRangeRules {
    @Test
    @DisplayName("should fetch all ip range rules for a valid query")
    void shouldGetAllIpRangeRules() throws InvalidProtocolBufferException {
      Value mockIpRangeRuleConfig1 = mockRuleConfig("First test", "Tester-1");
      Value mockIpRangeRuleConfig2 = mockRuleConfig("Second test", "Tester-2");

      addIpRangeRule(
          ImmutableSortedMap.of(
              "First-test", mockIpRangeRuleConfig1, "Second-Test", mockIpRangeRuleConfig2));

      IpRangeRuleDetails ipRangeRuleDetails1 =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester-1")
              .setDescription("Range rule test 1")
              .addAllRawInputIpData(Arrays.asList("4.3.2.1", "16.16.16.16/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT2H2M34S").build())
              .build();

      IpRangeRule ipRangeRule1 =
          IpRangeRule.newBuilder()
              .setId("First-test")
              .setRuleDetails(ipRangeRuleDetails1)
              .setDisabled(true)
              .setInternal(false)
              .addAllIpRanges(Arrays.asList("16.16.16.16/16"))
              .addAllIpAddresses(Arrays.asList("4.3.2.1"))
              .build();

      IpRangeRuleDetails ipRangeRuleDetails2 =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester-2")
              .setDescription("Range rule test 2")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationTimestampMillis(1000).build())
              .build();

      IpRangeRule ipRangeRule2 =
          IpRangeRule.newBuilder()
              .setId("Second-test")
              .setRuleDetails(ipRangeRuleDetails2)
              .setDisabled(false)
              .setInternal(true)
              .addAllIpRanges(Arrays.asList("1.1.1.1/16"))
              .addAllIpAddresses(Arrays.asList("1.2.3.4"))
              .build();

      when(ipRangeRuleConverter.convert(mockIpRangeRuleConfig1)).thenReturn(ipRangeRule1);
      when(ipRangeRuleConverter.convert(mockIpRangeRuleConfig2)).thenReturn(ipRangeRule2);

      // No filter used so should return all rules
      List<IpRangeRule> ipRangeRules =
          rulesManager.getIpRangeRules(requestContext, GetRulesFilter.newBuilder().build());
      assertEquals(List.of(ipRangeRule2, ipRangeRule1), ipRangeRules);

      // Filter by id when id is not present
      assertTrue(
          rulesManager
              .getIpRangeRules(
                  requestContext, GetRulesFilter.newBuilder().addRuleIds("An absent id").build())
              .isEmpty());

      // Filter by id and id is present
      List<IpRangeRule> resultIpRules1 =
          rulesManager.getIpRangeRules(
              requestContext, GetRulesFilter.newBuilder().addRuleIds("First-test").build());
      assertEquals(List.of(ipRangeRule1), resultIpRules1);

      // Filter by interal and name is present
      List<IpRangeRule> resultIpRules2 =
          rulesManager.getIpRangeRules(
              requestContext, GetRulesFilter.newBuilder().setInternal(true).build());
      assertEquals(List.of(ipRangeRule2), resultIpRules2);
    }

    @Test
    @DisplayName(
        "should throw a InvalidProtocolBufferException error if ip range rule giving error")
    void should_handleIpRangeRule_invalidProtocolBufferException()
        throws InvalidProtocolBufferException {
      Value mockIpRangeRuleConfig1 = mockRuleConfig("First test", "Tester-1");
      Value mockIpRangeRuleConfig2 = mockRuleConfig("Second test", "Tester-2");

      addIpRangeRule(
          ImmutableSortedMap.of(
              "First-test", mockIpRangeRuleConfig1, "Second-Test", mockIpRangeRuleConfig2));

      IpRangeRuleDetails ipRangeRuleDetails1 =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester-1")
              .setDescription("Range rule test 1")
              .addAllRawInputIpData(Arrays.asList("4.3.2.1", "16.16.16.16/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT2H2M34S").build())
              .build();

      IpRangeRule ipRangeRule1 =
          IpRangeRule.newBuilder()
              .setId("First-test")
              .setRuleDetails(ipRangeRuleDetails1)
              .setDisabled(true)
              .setInternal(false)
              .build();

      when(ipRangeRuleConverter.convert(mockIpRangeRuleConfig1)).thenReturn(ipRangeRule1);
      when(ipRangeRuleConverter.convert(mockIpRangeRuleConfig2))
          .thenThrow(InvalidProtocolBufferException.class);

      assertThrows(
          RuntimeException.class,
          () -> rulesManager.getIpRangeRules(requestContext, GetRulesFilter.newBuilder().build()));
    }
  }

  @Nested
  class createIpRangeRule {
    @Test
    @DisplayName("should be able to create an Ip Range Rule if given valid arguments")
    void shouldCreateIpRangeRule() throws InvalidProtocolBufferException {
      IpRangeRuleDetails ipRangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester-1")
              .setDescription("Range rule test 1")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
              .build();

      long now = Clock.systemUTC().millis();

      IpRangeRule ipRangeRule =
          IpRangeRule.newBuilder()
              .setId("First-test")
              .setRuleDetails(
                  IpRangeRuleDetails.newBuilder(ipRangeRuleDetails)
                      .setExpirationDetails(
                          ExpirationDetails.newBuilder()
                              .setExpirationDuration(
                                  ipRangeRuleDetails.getExpirationDetails().getExpirationDuration())
                              .setExpirationTimestampMillis(
                                  Duration.parse(
                                              ipRangeRuleDetails
                                                  .getExpirationDetails()
                                                  .getExpirationDuration())
                                          .toMillis()
                                      + now)
                              .build())
                      .build())
              .addAllIpAddresses(Arrays.asList("1.2.3.4"))
              .addAllIpRanges(Arrays.asList("1.1.1.1/16"))
              .build();

      when(uuidGenerator.generateId()).thenReturn("First-test");

      Value ruleConfig = mockRuleConfig("First-test", "Tester-1");

      when(ipRangeRuleConverter.convert(ipRangeRule)).thenReturn(ruleConfig);
      when(ipRangeRuleConverter.convert(ruleConfig)).thenReturn(ipRangeRule);
      when(mockClock.millis()).thenReturn(now);

      IpRangeRule maybeCreatedIpRangeRule =
          rulesManager.createIpRangeRule(
              requestContext,
              CreateIpRangeRuleRequest.newBuilder().setRuleDetails(ipRangeRuleDetails).build());

      assertNotNull(maybeCreatedIpRangeRule);
      assertEquals(ipRangeRule, maybeCreatedIpRangeRule);
    }

    @Test
    @DisplayName(
        "should throw invalidProtocolBufferException if is it thrown while parsing ipRangeRule")
    void should_notCreateIpRangeRule_invalidIpRangeRuleConversion()
        throws InvalidProtocolBufferException {
      IpRangeRuleDetails ipRangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester-1")
              .setDescription("Range rule test 1")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
              .build();

      IpRangeRule ipRangeRule =
          IpRangeRule.newBuilder()
              .setId("First-test")
              .setRuleDetails(
                  IpRangeRuleDetails.newBuilder(ipRangeRuleDetails)
                      .setExpirationDetails(
                          ExpirationDetails.newBuilder()
                              .setExpirationDuration(
                                  ipRangeRuleDetails.getExpirationDetails().getExpirationDuration())
                              .setExpirationTimestampMillis(
                                  Duration.parse(
                                          ipRangeRuleDetails
                                              .getExpirationDetails()
                                              .getExpirationDuration())
                                      .toMillis())
                              .build())
                      .build())
              .addAllIpAddresses(Arrays.asList("1.2.3.4"))
              .addAllIpRanges(Arrays.asList("1.1.1.1/16"))
              .build();

      when(uuidGenerator.generateId()).thenReturn("First-test");
      when(ipRangeRuleConverter.convert(ipRangeRule))
          .thenThrow(InvalidProtocolBufferException.class);

      assertThrows(
          RuntimeException.class,
          () ->
              rulesManager.createIpRangeRule(
                  requestContext,
                  CreateIpRangeRuleRequest.newBuilder()
                      .setRuleDetails(ipRangeRuleDetails)
                      .build()));
    }

    @Test
    void should_notCreateIpRangeRule_invalidIpRangeRuleConfigConversion()
        throws InvalidProtocolBufferException {
      IpRangeRuleDetails ipRangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester-1")
              .setDescription("Range rule test 1")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
              .build();

      IpRangeRule ipRangeRule =
          IpRangeRule.newBuilder()
              .setId("First-test")
              .setRuleDetails(ipRangeRuleDetails)
              .addAllIpAddresses(Arrays.asList("1.2.3.4"))
              .addAllIpRanges(Arrays.asList("1.1.1.1/16"))
              .build();

      Value ruleConfig = mockRuleConfig("First-test", "Tester-1");

      when(uuidGenerator.generateId()).thenReturn("First-test");
      when(ipRangeRuleConverter.convert(ipRangeRule)).thenReturn(ruleConfig);
      when(ipRangeRuleConverter.convert(ruleConfig))
          .thenThrow(InvalidProtocolBufferException.class);

      assertThrows(
          RuntimeException.class,
          () ->
              rulesManager.createIpRangeRule(
                  requestContext,
                  CreateIpRangeRuleRequest.newBuilder()
                      .setRuleDetails(ipRangeRuleDetails)
                      .build()));
    }

    @Test
    @DisplayName(
        "If there is an Ip Range String that is not in valid CIDR format it should throw IllegalArgumentException")
    void should_notCreateIpRangeRule_invalidIpRangeRuleFormat() {
      IpRangeRuleDetails ipRangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester-1")
              .setDescription("Range rule test 1")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16", "apple"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
              .build();

      assertThrows(
          IllegalArgumentException.class,
          () ->
              rulesManager.createIpRangeRule(
                  requestContext,
                  CreateIpRangeRuleRequest.newBuilder()
                      .setRuleDetails(ipRangeRuleDetails)
                      .build()));
    }
  }

  @Nested
  class updateIpRangeRule {
    @Test
    @DisplayName("Should fail when Ip Range Rule to be updated is not present")
    void should_notUpdateIpRangeRule_ifNotPresent() {
      IpRangeRuleDetails updatedRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Updated-Tester-1")
              .setDescription("Updated-Range rule test 1")
              .addAllRawInputIpData(Arrays.asList("11.12.13.14", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
              .build();

      assertThrows(
          NoSuchElementException.class,
          () ->
              rulesManager.updateIpRangeRule(
                  requestContext,
                  UpdateIpRangeRuleRequest.newBuilder()
                      .setId("First-test")
                      .setRuleDetails(updatedRuleDetails)
                      .setDisabled(true)
                      .build()));
    }

    @Test
    @DisplayName("should be able to update an Ip Range Rule if given valid arguments")
    void shouldUpdateIpRangeRule() throws InvalidProtocolBufferException {
      IpRangeRuleDetails updatedRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Updated-Tester-1")
              .setDescription("Updated-Range rule test 1")
              .addAllRawInputIpData(Arrays.asList("11.12.13.14", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
              .build();
      long now = Clock.systemUTC().millis();

      IpRangeRule updatedIpRangeRule =
          IpRangeRule.newBuilder()
              .setId("First-test")
              .setRuleDetails(
                  IpRangeRuleDetails.newBuilder(updatedRuleDetails)
                      .setExpirationDetails(
                          ExpirationDetails.newBuilder()
                              .setExpirationDuration(
                                  updatedRuleDetails.getExpirationDetails().getExpirationDuration())
                              .setExpirationTimestampMillis(
                                  Duration.parse(
                                              updatedRuleDetails
                                                  .getExpirationDetails()
                                                  .getExpirationDuration())
                                          .toMillis()
                                      + now)
                              .build())
                      .build())
              .setDisabled(true)
              .addAllIpAddresses(Arrays.asList("11.12.13.14"))
              .addAllIpRanges(Arrays.asList("1.1.1.1/16"))
              .build();

      Value mockIpRangeRuleConfig = mockRuleConfig("First-test", "Tester-1");
      addIpRangeRule(ImmutableSortedMap.of("First-test", mockIpRangeRuleConfig));

      Value ruleConfig = mockRuleConfig("First-test", "Tester-1");
      when(ipRangeRuleConverter.convert(updatedIpRangeRule)).thenReturn(ruleConfig);
      when(ipRangeRuleConverter.convert(ruleConfig)).thenReturn(updatedIpRangeRule);
      when(mockClock.millis()).thenReturn(now);

      IpRangeRule maybeUpdatedIpRangeRule =
          rulesManager.updateIpRangeRule(
              requestContext,
              UpdateIpRangeRuleRequest.newBuilder()
                  .setId("First-test")
                  .setRuleDetails(updatedRuleDetails)
                  .setDisabled(true)
                  .build());

      assertNotNull(maybeUpdatedIpRangeRule);
      assertEquals(updatedIpRangeRule, maybeUpdatedIpRangeRule);
    }

    @Test
    void should_notUpdateIpRangeRule_invalidIpRangeRuleConfigConversion()
        throws InvalidProtocolBufferException {
      IpRangeRuleDetails updatedRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Updated-Tester-1")
              .setDescription("Updated-Range rule test 1")
              .addAllRawInputIpData(Arrays.asList("11.12.13.14", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
              .build();

      IpRangeRule updatedIpRangeRule =
          IpRangeRule.newBuilder()
              .setId("First-test")
              .setRuleDetails(
                  IpRangeRuleDetails.newBuilder(updatedRuleDetails)
                      .setExpirationDetails(
                          ExpirationDetails.newBuilder()
                              .setExpirationDuration(
                                  updatedRuleDetails.getExpirationDetails().getExpirationDuration())
                              .setExpirationTimestampMillis(
                                  Duration.parse(
                                          updatedRuleDetails
                                              .getExpirationDetails()
                                              .getExpirationDuration())
                                      .toMillis())
                              .build())
                      .build())
              .setDisabled(true)
              .addAllIpAddresses(Arrays.asList("11.12.13.14"))
              .addAllIpRanges(Arrays.asList("1.1.1.1/16"))
              .build();

      Value mockIpRangeRuleConfig = mockRuleConfig("First-test", "Tester-1");
      addIpRangeRule(ImmutableSortedMap.of("First-test", mockIpRangeRuleConfig));

      Value ruleConfig = mockRuleConfig("First-test", "Tester-1");
      when(ipRangeRuleConverter.convert(updatedIpRangeRule)).thenReturn(ruleConfig);
      when(ipRangeRuleConverter.convert(ruleConfig))
          .thenThrow(InvalidProtocolBufferException.class);

      assertThrows(
          RuntimeException.class,
          () ->
              rulesManager.updateIpRangeRule(
                  requestContext,
                  UpdateIpRangeRuleRequest.newBuilder()
                      .setId("First-test")
                      .setRuleDetails(updatedRuleDetails)
                      .setDisabled(true)
                      .build()));
    }

    @Test
    void should_notUpdateIpRangeRule_invalidIpRangeRuleConversion()
        throws InvalidProtocolBufferException {
      IpRangeRuleDetails updatedRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Updated-Tester-1")
              .setDescription("Updated-Range rule test 1")
              .addAllRawInputIpData(Arrays.asList("11.12.13.14", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
              .build();

      IpRangeRule updatedIpRangeRule =
          IpRangeRule.newBuilder()
              .setId("First-test")
              .setRuleDetails(
                  IpRangeRuleDetails.newBuilder(updatedRuleDetails)
                      .setExpirationDetails(
                          ExpirationDetails.newBuilder()
                              .setExpirationDuration(
                                  updatedRuleDetails.getExpirationDetails().getExpirationDuration())
                              .setExpirationTimestampMillis(
                                  Duration.parse(
                                          updatedRuleDetails
                                              .getExpirationDetails()
                                              .getExpirationDuration())
                                      .toMillis())
                              .build())
                      .build())
              .setDisabled(true)
              .addAllIpAddresses(Arrays.asList("11.12.13.14"))
              .addAllIpRanges(Arrays.asList("1.1.1.1/16"))
              .build();

      Value mockIpRangeRuleConfig = mockRuleConfig("First-test", "Tester-1");
      addIpRangeRule(ImmutableSortedMap.of("First-test", mockIpRangeRuleConfig));

      when(ipRangeRuleConverter.convert(updatedIpRangeRule))
          .thenThrow(InvalidProtocolBufferException.class);

      assertThrows(
          RuntimeException.class,
          () ->
              rulesManager.updateIpRangeRule(
                  requestContext,
                  UpdateIpRangeRuleRequest.newBuilder()
                      .setId("First-test")
                      .setRuleDetails(updatedRuleDetails)
                      .setDisabled(true)
                      .build()));
    }

    @Test
    @DisplayName(
        "If there is an Ip Range String that is not in valid CIDR format it should throw exception")
    void should_notUpdateIpRangeRule_invalidIpRangeRuleFormat() {
      IpRangeRuleDetails ipRangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester-1")
              .setDescription("Range rule test 1")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16", "apple"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
              .build();

      assertThrows(
          IllegalArgumentException.class,
          () ->
              rulesManager.createIpRangeRule(
                  requestContext,
                  CreateIpRangeRuleRequest.newBuilder()
                      .setRuleDetails(ipRangeRuleDetails)
                      .build()));
    }
  }

  @Nested
  class deleteIpRangeRule {
    @Test
    @DisplayName("should be able to delete an Ip Range Rule")
    void shouldDeleteIpRangeRule() {
      Value mockIpRangeRuleConfig = mockRuleConfig("id-1", "name-1");
      addIpRangeRule(ImmutableSortedMap.of("id-1", mockIpRangeRuleConfig));

      // Deleting an entity which exists
      assertDoesNotThrow(() -> rulesManager.deleteIpRangeRule(requestContext, "id-1"));
    }
  }

  private void addIpRangeRule(Map<String, Value> ipRangeRuleConfigs) {
    ipRangeRuleConfigs.forEach(
        (id, ipRangeRuleConfig) ->
            configServiceBlockingStub.upsertConfig(
                UpsertConfigRequest.newBuilder()
                    .setResourceNamespace(IPRANGE_RULE_CONFIG_NAMESPACE)
                    .setResourceName(IPRANGE_RULE_CONFIG_RESOURCE_NAME)
                    .setConfig(ipRangeRuleConfig)
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
