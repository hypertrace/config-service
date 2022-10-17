package ai.traceable.malicioussources.config.service.rules;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.malicioussources.config.service.v1.CreateMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.GetRulesFilter;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.UpdateMaliciousSourcesRuleRequest;
import io.grpc.Status;
import io.grpc.StatusException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ConfigObject;
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
  public List<MaliciousSourcesRule> getMaliciousSourcesRules(
      RequestContext requestContext, GetRulesFilter filter) {

    if (filter.equals(GetRulesFilter.getDefaultInstance())) {
      return maliciousSourcesRulesStore.getAllConfigData(requestContext);
    }
    return maliciousSourcesRulesStore.getAllConfigData(requestContext, filter);
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

  @Override
  public MaliciousSourcesRule updateMaliciousSourcesRule(
      RequestContext requestContext, UpdateMaliciousSourcesRuleRequest updateRuleRequest)
      throws StatusException {
    String ruleId = updateRuleRequest.getRule().getId();
    if (!doesMaliciousSourcesRuleExist(requestContext, ruleId)) {
      throw new StatusException(Status.NOT_FOUND);
    }

    return maliciousSourcesRulesStore
        .upsertObject(requestContext, updateRuleRequest.getRule())
        .getData();
  }

  @Override
  public MaliciousSourcesRule deleteMaliciousSourcesRule(RequestContext requestContext, String id) {
    return maliciousSourcesRulesStore
        .deleteObject(requestContext, id)
        .map(ConfigObject::getData)
        .orElseThrow(Status.NOT_FOUND::asRuntimeException);
  }

  private boolean doesMaliciousSourcesRuleExist(RequestContext requestContext, String ruleId) {
    Optional<MaliciousSourcesRule> optionalRule =
        maliciousSourcesRulesStore.getData(requestContext, ruleId);
    return optionalRule.isPresent();
  }
}
