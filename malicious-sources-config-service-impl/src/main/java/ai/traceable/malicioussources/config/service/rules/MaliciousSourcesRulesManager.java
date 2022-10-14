package ai.traceable.malicioussources.config.service.rules;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.malicioussources.config.service.v1.CreateMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import java.util.UUID;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class MaliciousSourcesRulesManager implements RulesManager {
  private final MaliciousSourcesRulesStore maliciousSourcesRulesStore;
  private final UuidGenerator uuidGenerator;

  @Inject
  public MaliciousSourcesRulesManager(
      MaliciousSourcesRulesStore maliciousSourcesRulesStore, UuidGenerator uuidGenerator) {
    this.maliciousSourcesRulesStore = maliciousSourcesRulesStore;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public MaliciousSourcesRule createMaliciousSourcesRule(
      RequestContext requestContext, CreateMaliciousSourcesRuleRequest createRuleRequest) {
    String ruleId = this.uuidGenerator.generateId(UUID.randomUUID().toString());

    MaliciousSourcesRule maliciousSourcesRule =
        MaliciousSourcesRule.newBuilder()
            .setId(ruleId)
            .setRuleScope(createRuleRequest.getRuleScope())
            .setRuleInfo(createRuleRequest.getRuleInfo())
            .build();

    return maliciousSourcesRulesStore.upsertObject(requestContext, maliciousSourcesRule).getData();
  }
}
