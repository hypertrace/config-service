package ai.traceable.genai.system.discovery.config.service.v1.manager;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.genai.system.discovery.config.service.v1.CreateGenAiSystemDiscoveryRuleRequest;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiSystemDiscoveryRule;
import ai.traceable.genai.system.discovery.config.service.v1.GetGenAiSystemDiscoveryRulesFilter;
import ai.traceable.genai.system.discovery.config.service.v1.UpdateGenAiSystemDiscoveryRuleRequest;
import ai.traceable.genai.system.discovery.config.service.v1.store.GenAiSystemDiscoveryRuleStore;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.DeletedConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class GenAiSystemDiscoveryRuleManagerImpl implements GenAiSystemDiscoveryRuleManager {
  private final UuidGenerator uuidGenerator;
  private final GenAiSystemDiscoveryRuleStore genAiSystemDiscoveryRuleStore;

  @Inject
  public GenAiSystemDiscoveryRuleManagerImpl(
      UuidGenerator uuidGenerator, GenAiSystemDiscoveryRuleStore genAiSystemDiscoveryRuleStore) {

    this.genAiSystemDiscoveryRuleStore = genAiSystemDiscoveryRuleStore;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public List<GenAiSystemDiscoveryRule> getGenAiSystemDiscoveryRules(
      RequestContext requestContext, GetGenAiSystemDiscoveryRulesFilter filter) {

    if (GetGenAiSystemDiscoveryRulesFilter.getDefaultInstance().equals(filter)) {
      return genAiSystemDiscoveryRuleStore.getAllConfigData(requestContext);
    }
    return genAiSystemDiscoveryRuleStore.getAllConfigData(requestContext, filter);
  }

  @Override
  public GenAiSystemDiscoveryRule createGenAiSystemDiscoveryRule(
      RequestContext requestContext, CreateGenAiSystemDiscoveryRuleRequest createRuleRequest) {
    String ruleId = this.uuidGenerator.generateId(UUID.randomUUID().toString());
    GenAiSystemDiscoveryRule genAiSystemDiscoveryRule =
        GenAiSystemDiscoveryRule.newBuilder()
            .setRuleId(ruleId)
            .setGenAiSystemDiscoveryRuleData(createRuleRequest.getGenAiSystemDiscoveryRuleData())
            .build();
    return upsertObject(requestContext, genAiSystemDiscoveryRule);
  }

  @Override
  public GenAiSystemDiscoveryRule updateGenAiSystemDiscoveryRule(
      RequestContext requestContext, UpdateGenAiSystemDiscoveryRuleRequest updateRuleRequest) {
    String ruleId = updateRuleRequest.getGenAiSystemDiscoveryRule().getRuleId();
    if (!doesGenAiSystemDiscoveryRuleExist(requestContext, ruleId)) {
      throw Status.NOT_FOUND
          .withDescription(
              String.format(
                  "Unable to update as GenAiSystemDiscoveryRule with id = %s does not exist",
                  ruleId))
          .asRuntimeException();
    }
    return upsertObject(requestContext, updateRuleRequest.getGenAiSystemDiscoveryRule());
  }

  @Override
  public void deleteGenAiSystemDiscoveryRule(RequestContext requestContext, String id) {
    genAiSystemDiscoveryRuleStore
        .deleteObject(requestContext, id)
        .map(DeletedConfigObject::getDeletedData)
        .orElseThrow(() -> Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers()));
  }

  private boolean doesGenAiSystemDiscoveryRuleExist(RequestContext requestContext, String ruleId) {
    Optional<GenAiSystemDiscoveryRule> optionalRule =
        genAiSystemDiscoveryRuleStore.getData(requestContext, ruleId);
    return optionalRule.isPresent();
  }

  private GenAiSystemDiscoveryRule upsertObject(
      RequestContext requestContext, GenAiSystemDiscoveryRule genAiSystemDiscoveryRule) {
    return genAiSystemDiscoveryRuleStore
        .upsertObject(requestContext, genAiSystemDiscoveryRule)
        .getData();
  }
}
