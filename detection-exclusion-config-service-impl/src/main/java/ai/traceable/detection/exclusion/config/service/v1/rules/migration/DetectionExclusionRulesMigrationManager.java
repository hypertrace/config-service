package ai.traceable.detection.exclusion.config.service.v1.rules.migration;

import ai.traceable.anomaly.config.service.exclusion.handlers.AnomalyExclusionRuleConfigStore;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleConfig;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import ai.traceable.detection.exclusion.config.service.v1.RuleSource;
import ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionRulesStore;
import ai.traceable.platform.actor.v1.Actor;
import ai.traceable.platform.config.provider.common.clients.ActorServiceClient;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DetectionExclusionRulesMigrationManager implements RulesMigrationManager {

  private static final GetRulesFilter OLD_RULES_FILTER =
      GetRulesFilter.newBuilder().addRuleCreationSources(RuleSource.RULE_SOURCE_OLD_API).build();

  private final FeatureCachingClient featureCachingClient;
  private final DetectionExclusionRulesStore newRulesStore;
  private final AnomalyExclusionRuleConfigStore oldRulesStore;
  private final DetectionExclusionRuleConverter ruleConverter;
  private final ActorServiceClient actorServiceClient;

  private final Map<ContextualKey<Void>, Boolean> tenantMigrationCompletedMap = new HashMap<>();

  @Inject
  public DetectionExclusionRulesMigrationManager(
      FeatureCachingClient featureCachingClient,
      DetectionExclusionRulesStore newRulesStore,
      AnomalyExclusionRuleConfigStore oldRulesStore,
      DetectionExclusionRuleConverter ruleConverter,
      ActorServiceClient actorServiceClient) {
    this.featureCachingClient = featureCachingClient;
    this.newRulesStore = newRulesStore;
    this.oldRulesStore = oldRulesStore;
    this.ruleConverter = ruleConverter;
    this.actorServiceClient = actorServiceClient;
  }

  @Override
  public boolean shouldMigrateFromOldStore(RequestContext requestContext) {
    return !tenantMigrationCompletedMap.getOrDefault(
            requestContext.buildInternalContextualKey(), false)
        && featureCachingClient.isDetectionExclusionV2EnabledForTenant(requestContext);
  }

  @Override
  public void updateDetectionExclusionRulesFromOldStore(RequestContext requestContext) {
    Map<String, ContextualConfigObject<DetectionExclusionRule>> newRuleObjects =
        newRulesStore.getAllObjects(requestContext, OLD_RULES_FILTER).stream()
            .collect(Collectors.toMap(object -> object.getData().getId(), Function.identity()));
    Map<String, ContextualConfigObject<AnomalyExclusionRuleConfig>> oldRuleObjects =
        oldRulesStore.getAllObjects(requestContext).stream()
            .collect(Collectors.toMap(object -> object.getData().getId(), Function.identity()));

    List<String> oldRulesActorEntityIds =
        oldRuleObjects.values().stream()
            .map(ConfigObject::getData)
            .filter(rule -> rule.getRuleData().getAnomalyActorExclusionInfo().hasAnomalyActor())
            .map(
                rule -> rule.getRuleData().getAnomalyActorExclusionInfo().getAnomalyActor().getId())
            .collect(Collectors.toUnmodifiableList());

    Map<String, String> oldRulesActorEntityIdToIdMap =
        actorServiceClient
            .getActorsByEntityIds(
                requestContext.getTenantId().orElseThrow(), oldRulesActorEntityIds)
            .stream()
            .collect(Collectors.toUnmodifiableMap(Actor::getEntityId, Actor::getActorId));

    // rules created by old api, not present in new api
    List<DetectionExclusionRule> oldRulesToCreate =
        Sets.difference(oldRuleObjects.keySet(), newRuleObjects.keySet()).stream()
            .map(
                key ->
                    ruleConverter.convertRule(
                        oldRuleObjects.get(key).getData(), oldRulesActorEntityIdToIdMap))
            .filter(Objects::nonNull)
            .collect(Collectors.toUnmodifiableList());

    // rules present in both stores
    // need to be conditionally overridden if latest update is by old api
    List<DetectionExclusionRule> oldRulesToUpdate =
        Sets.intersection(oldRuleObjects.keySet(), newRuleObjects.keySet()).stream()
            .filter(
                key ->
                    oldRuleObjects.get(key).getLastUpdatedTimestamp().toEpochMilli()
                        > newRuleObjects.get(key).getLastUpdatedTimestamp().toEpochMilli())
            .map(
                key ->
                    ruleConverter.convertRule(
                        oldRuleObjects.get(key).getData(), oldRulesActorEntityIdToIdMap))
            .filter(Objects::nonNull)
            .collect(Collectors.toUnmodifiableList());

    if (!oldRulesToCreate.isEmpty()) {
      newRulesStore.upsertObjects(requestContext, oldRulesToCreate);
    }
    if (!oldRulesToUpdate.isEmpty()) {
      newRulesStore.upsertObjects(requestContext, oldRulesToUpdate);
    }

    tenantMigrationCompletedMap.put(requestContext.buildInternalContextualKey(), true);
  }
}
