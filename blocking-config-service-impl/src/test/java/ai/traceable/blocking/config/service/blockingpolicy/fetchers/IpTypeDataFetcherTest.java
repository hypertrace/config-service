package ai.traceable.blocking.config.service.blockingpolicy.fetchers;

import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_DENIED;
import static ai.traceable.malicioussources.config.service.v1.EventSeverity.EVENT_SEVERITY_CRITICAL;
import static ai.traceable.malicioussources.config.service.v1.IpLocationType.IP_LOCATION_TYPE_HOSTING_PROVIDER;
import static ai.traceable.malicioussources.config.service.v1.IpLocationType.IP_LOCATION_TYPE_PUBLIC_PROXY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.iptype.BlockingIpTypesClient;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.IpType;
import ai.traceable.malicioussources.config.service.v1.ExpirationDetails;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.malicioussources.config.service.v1.IpLocationTypeCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleAction;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleInfo;
import ai.traceable.malicioussources.config.service.v1.RuleActionType;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IpTypeDataFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final String ENVIRONMENT_ID = "environment-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private BlockingIpTypesClient blockingIpTypesClient;
  private IpTypeDataFetcher ipTypeDataFetcher;

  private static final long inactiveTimestamp = System.currentTimeMillis() - 10000L;
  private static final long activeTimestamp = System.currentTimeMillis() + 10000L;

  @BeforeEach
  void setUp() {
    blockingIpTypesClient = mock(BlockingIpTypesClient.class);

    BlockingRulesUtils blockingRulesUtils = mock(BlockingRulesUtils.class);
    doReturn(true).when(blockingRulesUtils).isRuleActive(activeTimestamp);
    doReturn(true).when(blockingRulesUtils).isRuleActive(0);
    doReturn(false).when(blockingRulesUtils).isRuleActive(inactiveTimestamp);

    ipTypeDataFetcher = new IpTypeDataFetcher(blockingIpTypesClient, blockingRulesUtils);
  }

  @Test
  void getIpTypeRulesTest() {
    doReturn(
            List.of(
                generateIpTypeRule(
                    "1",
                    List.of(IP_LOCATION_TYPE_HOSTING_PROVIDER),
                    Timestamp.getDefaultInstance()),
                generateIpTypeRule(
                    "2",
                    List.of(IP_LOCATION_TYPE_HOSTING_PROVIDER, IP_LOCATION_TYPE_PUBLIC_PROXY),
                    Timestamp.newBuilder()
                        .setSeconds(activeTimestamp / 1000)
                        .setNanos(((int) (activeTimestamp % 1000)) * 1000000)
                        .build()),
                generateIpTypeRule(
                    "3",
                    List.of(IpLocationType.IP_LOCATION_TYPE_BOT, IP_LOCATION_TYPE_PUBLIC_PROXY),
                    Timestamp.newBuilder()
                        .setSeconds(inactiveTimestamp / 1000)
                        .setNanos(((int) (inactiveTimestamp % 1000)) * 1000000)
                        .build())))
        .when(blockingIpTypesClient)
        .fetchMaliciousSourceRules(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    List<BlockingDetails> violations =
        ipTypeDataFetcher.getIpTypeViolations(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    assertEquals(2, violations.size());
    assertEquals(
        List.of(IpType.IP_TYPE_HOSTING_PROVIDER),
        violations.get(0).getIpTypeDetails().getIpTypesList());
    assertEquals(BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE, violations.get(0).getCategory());
    assertEquals(BLOCKING_RULE_TYPE_BLOCK, violations.get(0).getBlockingRuleType());
    assertEquals(BLOCKING_STATUS_DENIED, violations.get(0).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesRuleViolationInfo(
            "1", "rule-1", EVENT_SEVERITY_CRITICAL.name()),
        violations.get(0).getInfo());

    assertEquals(
        List.of(IpType.IP_TYPE_HOSTING_PROVIDER, IpType.IP_TYPE_PROXY),
        violations.get(1).getIpTypeDetails().getIpTypesList());
    assertEquals(BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE, violations.get(1).getCategory());
    assertEquals(BLOCKING_RULE_TYPE_BLOCK, violations.get(1).getBlockingRuleType());
    assertEquals(BLOCKING_STATUS_DENIED, violations.get(1).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesRuleViolationInfo(
            "2", "rule-2", EVENT_SEVERITY_CRITICAL.name()),
        violations.get(1).getInfo());
  }

  private static MaliciousSourcesRule generateIpTypeRule(
      String id, List<IpLocationType> ipLocationTypeList, Timestamp timestamp) {
    MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
        MaliciousSourcesRuleInfo.newBuilder()
            .setName("rule-" + id)
            .setDescription("test rule-" + id)
            .addConditions(
                MaliciousSourcesRuleCondition.newBuilder()
                    .setIpLocationTypeCondition(
                        IpLocationTypeCondition.newBuilder()
                            .addAllIpLocationTypes(ipLocationTypeList)
                            .build()))
            .setRuleAction(
                MaliciousSourcesRuleAction.newBuilder()
                    .setActionType(RuleActionType.RULE_ACTION_TYPE_BLOCK)
                    .setEventSeverity(EVENT_SEVERITY_CRITICAL)
                    .setExpirationDetails(
                        ExpirationDetails.newBuilder().setExpirationTimestamp(timestamp))
                    .build())
            .build();

    return MaliciousSourcesRule.newBuilder()
        .setId(id)
        .setRuleInfo(maliciousSourcesRuleInfo)
        .build();
  }
}
