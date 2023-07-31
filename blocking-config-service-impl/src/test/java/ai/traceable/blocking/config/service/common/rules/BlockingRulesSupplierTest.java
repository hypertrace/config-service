package ai.traceable.blocking.config.service.common.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.common.rules.fetchers.CustomSignatureRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.MaliciousSourcesRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RegionRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RulesFetcher;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BlockingRulesSupplierTest {

  private static final RequestContext REQUEST_CONTEXT = mock(RequestContext.class);
  private static final Optional<String> ENVIRONMENT_ID = Optional.of("env");

  RegionRulesFetcher regionRulesFetcher;
  CustomSignatureRulesFetcher customSignatureRulesFetcher;
  MaliciousSourcesRulesFetcher maliciousSourcesRulesFetcher;

  Map<RulesFetcher.RulesFetcherType, RulesFetcher> rulesFetchers;

  BlockingRulesSupplier blockingRulesSupplier;

  @BeforeEach
  public void setup() {
    regionRulesFetcher = mock(RegionRulesFetcher.class);
    customSignatureRulesFetcher = mock(CustomSignatureRulesFetcher.class);
    maliciousSourcesRulesFetcher = mock(MaliciousSourcesRulesFetcher.class);

    rulesFetchers =
        Map.of(
            RulesFetcher.RulesFetcherType.CUSTOM_SIGNATURE,
            customSignatureRulesFetcher,
            RulesFetcher.RulesFetcherType.REGION,
            regionRulesFetcher,
            RulesFetcher.RulesFetcherType.MALICIOUS_SOURCES,
            maliciousSourcesRulesFetcher);
  }

  @Test
  public void test_getCustomSignatureRulesBlob() {
    CustomModsecRuleVersion version1 = CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3;
    CustomModsecRuleVersion version2 =
        CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS;
    String blob1 = "blob1";
    String blob2 = "blob2";

    when(customSignatureRulesFetcher.fetchModsecRules(REQUEST_CONTEXT, ENVIRONMENT_ID, version1))
        .thenReturn(
            GetCustomSignatureModsecRulesResponse.newBuilder().setModsecRulesBlob(blob1).build());
    when(customSignatureRulesFetcher.fetchModsecRules(REQUEST_CONTEXT, ENVIRONMENT_ID, version2))
        .thenReturn(
            GetCustomSignatureModsecRulesResponse.newBuilder().setModsecRulesBlob(blob2).build());

    blockingRulesSupplier =
        new BlockingRulesSupplier(rulesFetchers, REQUEST_CONTEXT, ENVIRONMENT_ID);
    assertEquals(blob1, blockingRulesSupplier.getCustomSignatureRulesBlob(version1));
    verify(customSignatureRulesFetcher, times(1)).fetchModsecRules(any(), any(), any());
    assertEquals(blob2, blockingRulesSupplier.getCustomSignatureRulesBlob(version2));
    verify(customSignatureRulesFetcher, times(2)).fetchModsecRules(any(), any(), any());
    assertEquals(blob2, blockingRulesSupplier.getCustomSignatureRulesBlob(version2));
    // fetchRules will not be called again..
    verify(customSignatureRulesFetcher, times(2)).fetchModsecRules(any(), any(), any());

    assertEquals(
        "",
        blockingRulesSupplier.getCustomSignatureRulesBlob(
            CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_CORAZA_V3));
  }
}
