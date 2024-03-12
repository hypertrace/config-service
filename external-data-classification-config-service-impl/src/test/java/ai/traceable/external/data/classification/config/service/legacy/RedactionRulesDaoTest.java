package ai.traceable.external.data.classification.config.service.legacy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.external.data.classification.config.service.legacy.RedactionRulesDao.RedactionRuleFilter;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesResponse;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub;
import java.util.List;
import java.util.Set;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RedactionRulesDaoTest {

  private static final RedactionRule DISABLED_SESSION_ID_RULE =
      RedactionRule.newBuilder()
          .setDisabled(true)
          .setSessionIdentifier(true)
          .setId("disabled-session-id")
          .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_HASH)
          .build();
  private static final RedactionRule ENABLED_SESSION_ID_RULE =
      RedactionRule.newBuilder()
          .setDisabled(false)
          .setSessionIdentifier(true)
          .setId("enabled-session-id")
          .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_RAW)
          .build();
  private static final RedactionRule ENABLED_OBFUSCATING_REDACTION_RULE =
      RedactionRule.newBuilder()
          .setDisabled(false)
          .setSessionIdentifier(false)
          .setId("enabled-obfuscating-id")
          .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_HASH)
          .build();
  private static final RedactionRule ENABLED_RAW_REDACTION_RULE =
      RedactionRule.newBuilder()
          .setDisabled(false)
          .setSessionIdentifier(false)
          .setId("enabled-raw-id")
          .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_RAW)
          .build();

  private static final RequestContext TEST_CONTEXT =
      RequestContext.forTenantId("RedactionRulesDaoTest");
  @Mock SensitiveDataConfigServiceBlockingStub mockSensitiveDataStub;

  @InjectMocks RedactionRulesDao redactionRulesDao;

  @Test
  void filtersReturnedRedactionRules() {
    when(mockSensitiveDataStub.getAllRedactionRules(
            GetAllRedactionRulesRequest.getDefaultInstance()))
        .thenReturn(
            GetAllRedactionRulesResponse.newBuilder()
                .addRedactionRules(DISABLED_SESSION_ID_RULE)
                .addRedactionRules(ENABLED_SESSION_ID_RULE)
                .addRedactionRules(ENABLED_OBFUSCATING_REDACTION_RULE)
                .addRedactionRules(ENABLED_RAW_REDACTION_RULE)
                .build());

    // If no data types enabled, only expect the enabled session id rule
    assertEquals(
        List.of(ENABLED_SESSION_ID_RULE),
        this.redactionRulesDao.getRulesMatchFilter(
            TEST_CONTEXT, new RedactionRuleFilter(Set.of(), true)));

    // If no data types enabled, only expect the enabled session id rule
    assertEquals(
        List.of(),
        this.redactionRulesDao.getRulesMatchFilter(
            TEST_CONTEXT, new RedactionRuleFilter(Set.of(), false)));

    // If all are, we should return them - raw included (these are filtered later)
    assertEquals(
        List.of(
            ENABLED_SESSION_ID_RULE,
            ENABLED_OBFUSCATING_REDACTION_RULE,
            ENABLED_RAW_REDACTION_RULE),
        this.redactionRulesDao.getRulesMatchFilter(
            TEST_CONTEXT,
            new RedactionRuleFilter(
                Set.of(
                    ENABLED_SESSION_ID_RULE.getId(),
                    ENABLED_OBFUSCATING_REDACTION_RULE.getId(),
                    ENABLED_RAW_REDACTION_RULE.getId()),
                true)));
  }
}
