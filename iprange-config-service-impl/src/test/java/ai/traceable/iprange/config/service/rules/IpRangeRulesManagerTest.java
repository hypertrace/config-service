package ai.traceable.iprange.config.service.rules;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.iprange.config.service.utils.IpValidationUtils;
import ai.traceable.iprange.config.service.utils.UuidGenerator;
import ai.traceable.iprange.config.service.v1.*;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.*;

class IpRangeRulesManagerTest {

  private MockGenericConfigService mockConfigService;
  private UuidGenerator uuidGenerator;
  private IpRangeRulesManager rulesManager;
  private RequestContext requestContext;
  private Clock mockClock;
  private IpRangeRulesStore ipRangeRulesStore;

  @BeforeEach
  void setUp() {
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    uuidGenerator = mock(UuidGenerator.class);
    IpValidationUtils ipValidationUtils = new IpValidationUtils();
    mockClock = mock(Clock.class);
    ipRangeRulesStore = new IpRangeRulesStore(configServiceBlockingStub);
    this.rulesManager =
        new IpRangeRulesManager(ipRangeRulesStore, uuidGenerator, ipValidationUtils, mockClock);
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
    void shouldGetAllIpRangeRules() {
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

      addIpRangeRule(ipRangeRule1);
      addIpRangeRule(ipRangeRule2);
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
  }

  @Nested
  class createIpRangeRule {
    @Test
    @DisplayName("should be able to create an Ip Range Rule if given valid arguments")
    void shouldCreateIpRangeRule() {
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

      when(mockClock.millis()).thenReturn(now);

      IpRangeRule maybeCreatedIpRangeRule =
          rulesManager.createIpRangeRule(
              requestContext,
              CreateIpRangeRuleRequest.newBuilder().setRuleDetails(ipRangeRuleDetails).build());

      assertNotNull(maybeCreatedIpRangeRule);
      assertEquals(ipRangeRule, maybeCreatedIpRangeRule);
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
    void shouldUpdateIpRangeRule() {
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

      addIpRangeRule(IpRangeRule.getDefaultInstance());

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
  }

  @Nested
  class deleteIpRangeRule {
    @Test
    @DisplayName("should be able to delete an Ip Range Rule")
    void shouldDeleteIpRangeRule() {
      addIpRangeRule(IpRangeRule.newBuilder().setId("id-1").build());
      // Deleting an entity which exists
      assertDoesNotThrow(() -> rulesManager.deleteIpRangeRule(requestContext, "id-1"));
    }
  }

  private void addIpRangeRule(IpRangeRule ipRangeRule) {
    this.ipRangeRulesStore.upsertObject(requestContext, ipRangeRule);
  }
}
