package ai.traceable.malicioussources.config.service.rules;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.malicioussources.config.service.v1.CreateMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.ExpirationDetails;
import ai.traceable.malicioussources.config.service.v1.GetRulesFilter;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleInfo;
import ai.traceable.malicioussources.config.service.v1.UpdateMaliciousSourcesRuleRequest;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.DeletedConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class MaliciousSourcesRulesManager implements RulesManager {
  private final MaliciousSourcesRulesStore maliciousSourcesRulesStore;
  private final UuidGenerator uuidGenerator;
  private Clock clock;

  @Inject
  public MaliciousSourcesRulesManager(
      MaliciousSourcesRulesStore maliciousSourcesRulesStore,
      UuidGenerator uuidGenerator,
      Clock clock) {
    this.maliciousSourcesRulesStore = maliciousSourcesRulesStore;
    this.uuidGenerator = uuidGenerator;
    this.clock = clock;
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
            .setRuleInfo(parseRuleInfo(createRuleRequest.getRuleInfo()))
            .build();

    return upsertObject(requestContext, maliciousSourcesRule);
  }

  @Override
  public MaliciousSourcesRule updateMaliciousSourcesRule(
      RequestContext requestContext, UpdateMaliciousSourcesRuleRequest updateRuleRequest) {
    String ruleId = updateRuleRequest.getRule().getId();
    if (!doesMaliciousSourcesRuleExist(requestContext, ruleId)) {
      throw Status.NOT_FOUND
          .withDescription(
              String.format(
                  "Unable to update as Malicious Sources rule with id = %s does not exist", ruleId))
          .asRuntimeException();
    }
    MaliciousSourcesRule.Builder builder = updateRuleRequest.getRule().toBuilder();
    builder.setRuleInfo(parseRuleInfo(updateRuleRequest.getRule().getRuleInfo()));

    return upsertObject(requestContext, builder.build());
  }

  @Override
  public Optional<MaliciousSourcesRule> deleteMaliciousSourcesRule(
      RequestContext requestContext, String id) {
    return maliciousSourcesRulesStore
        .deleteObject(requestContext, id)
        .map(DeletedConfigObject::getDeletedData)
        .orElseThrow(() -> Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers()));
  }

  private boolean doesMaliciousSourcesRuleExist(RequestContext requestContext, String ruleId) {
    Optional<MaliciousSourcesRule> optionalRule =
        maliciousSourcesRulesStore.getData(requestContext, ruleId);
    return optionalRule.isPresent();
  }

  private MaliciousSourcesRule upsertObject(
      RequestContext requestContext, MaliciousSourcesRule maliciousSourcesRule) {
    return maliciousSourcesRulesStore.upsertObject(requestContext, maliciousSourcesRule).getData();
  }

  private MaliciousSourcesRuleInfo parseRuleInfo(
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo) {
    ExpirationDetails expirationDetails =
        maliciousSourcesRuleInfo.getRuleAction().getExpirationDetails();
    if (expirationDetails.hasExpirationDuration() && !expirationDetails.hasExpirationTimestamp()) {

      MaliciousSourcesRuleInfo.Builder builder = maliciousSourcesRuleInfo.toBuilder();
      builder
          .getRuleActionBuilder()
          .getExpirationDetailsBuilder()
          .setExpirationTimestamp(
              Timestamps.add(
                  Timestamps.fromMillis(clock.millis()),
                  expirationDetails.getExpirationDuration()));
      return builder.build();
    }
    return maliciousSourcesRuleInfo;
  }
}
