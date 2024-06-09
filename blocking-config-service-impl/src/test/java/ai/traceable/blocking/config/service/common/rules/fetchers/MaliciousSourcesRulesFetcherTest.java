package ai.traceable.blocking.config.service.common.rules.fetchers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.malicioussources.config.service.v1.EnvironmentScope;
import ai.traceable.malicioussources.config.service.v1.ExpirationDetails;
import ai.traceable.malicioussources.config.service.v1.GetMaliciousSourcesRulesRequest;
import ai.traceable.malicioussources.config.service.v1.GetMaliciousSourcesRulesResponse;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleAction;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleInfo;
import com.google.protobuf.Timestamp;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

class MaliciousSourcesRulesFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final String environmentId = "env-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private static final long TIMESTAMP_IN_MILLIS = 10000L;

  private final MaliciousSourcesRule ruleForEnv =
      MaliciousSourcesRule.newBuilder().setId("rule1").build();
  private final MaliciousSourcesRule ruleForAllEnv =
      MaliciousSourcesRule.newBuilder().setId("rule2").build();
  private final MaliciousSourcesRule expiredRule =
      MaliciousSourcesRule.newBuilder()
          .setId("rule3")
          .setRuleInfo(
              MaliciousSourcesRuleInfo.newBuilder()
                  .setRuleAction(
                      MaliciousSourcesRuleAction.newBuilder()
                          .setExpirationDetails(
                              ExpirationDetails.newBuilder()
                                  .setExpirationTimestamp(
                                      Timestamp.newBuilder()
                                          .setSeconds(TIMESTAMP_IN_MILLIS / 1000 - 1)
                                          .setNanos(1000)))))
          .build();

  private Clock clock;
  private MaliciousSourcesConfigServiceBlockingStub maliciousSourcesConfigServiceBlockingStub;
  private MaliciousSourcesRulesFetcher maliciousSourcesRulesFetcher;

  @BeforeEach
  void setup() {
    clock = mock(Clock.class);
    maliciousSourcesConfigServiceBlockingStub =
        mock(
            MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub.class,
            Answers.RETURNS_SELF);
    maliciousSourcesRulesFetcher =
        new MaliciousSourcesRulesFetcher(
            Clock.systemUTC(), maliciousSourcesConfigServiceBlockingStub, ClientConfig.DEFAULT);

    when(clock.millis()).thenReturn(TIMESTAMP_IN_MILLIS);

    // Mock response for rules fetch
    doAnswer(
            invocation -> {
              EnvironmentScope envScope =
                  invocation
                      .getArgument(0, GetMaliciousSourcesRulesRequest.class)
                      .getFilter()
                      .getRuleScope()
                      .getEnvironmentScope();
              if (envScope.getEnvironmentIdsCount() == 0) {
                return GetMaliciousSourcesRulesResponse.newBuilder()
                    .addRules(ruleForAllEnv)
                    .addRules(expiredRule)
                    .build();
              } else {
                return GetMaliciousSourcesRulesResponse.newBuilder()
                    .addRules(ruleForEnv)
                    .addRules(expiredRule)
                    .build();
              }
            })
        .when(maliciousSourcesConfigServiceBlockingStub)
        .getMaliciousSourcesRules(any());
  }

  @Test
  void testFetchMaliciousSourceRules() {
    assertEquals(
        List.of(ruleForEnv),
        maliciousSourcesRulesFetcher.fetchRules(REQUEST_CONTEXT, Optional.of(environmentId)));
    assertEquals(
        List.of(ruleForAllEnv),
        maliciousSourcesRulesFetcher.fetchRules(REQUEST_CONTEXT, Optional.empty()));
  }
}
