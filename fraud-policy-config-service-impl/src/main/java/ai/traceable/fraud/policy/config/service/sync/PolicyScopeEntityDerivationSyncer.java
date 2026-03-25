package ai.traceable.fraud.policy.config.service.sync;

import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.DeleteEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityCategory;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigData;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigFilter;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigServiceGrpc;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityScope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityType;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EnvironmentScope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EventDerivationConfigDetails;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.Scope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanProjection;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.UpdateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicy;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyScope;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class PolicyScopeEntityDerivationSyncer {

  static final String DUMMY_ENTITY_ID = "__policy_scope_entity";
  static final String DUMMY_ENTITY_DISPLAY_NAME = "__policy_scope_marker_system_entity";
  static final String JEXL_CONSTANT = "'__policy_scope_dummy'";
  static final String EVENT_KIND_STRING = "system_event_kind_string";

  private final EntityDerivationConfigServiceGrpc.EntityDerivationConfigServiceBlockingStub
      entityDerivationConfigServiceStub;

  @Inject
  public PolicyScopeEntityDerivationSyncer(
      EntityDerivationConfigServiceGrpc.EntityDerivationConfigServiceBlockingStub
          entityDerivationConfigServiceStub) {
    this.entityDerivationConfigServiceStub = entityDerivationConfigServiceStub;
  }

  public void onPolicyCreated(RequestContext requestContext, AbusePolicy createdPolicy) {
    try {
      EventDerivationConfigDetails entry =
          buildEventDerivationEntry(createdPolicy.getId(), createdPolicy.getData().getScope());

      Optional<EntityDerivationConfig> existing = findDummyEntity(requestContext);
      if (existing.isPresent()) {
        List<EventDerivationConfigDetails> entries =
            new ArrayList<>(
                existing.get().getData().getSpanProjection().getEventDerivationConfigsList());
        entries.add(entry);
        updateDummyEntity(requestContext, entries);
      } else {
        createDummyEntity(requestContext, List.of(entry));
      }
    } catch (Exception e) {
      log.warn(
          "Failed to sync entity derivation for created policy {}. "
              + "Policy was created successfully but derivation scope may be missing.",
          createdPolicy.getId(),
          e);
    }
  }

  public void onPolicyUpdated(
      RequestContext requestContext, AbusePolicy oldPolicy, AbusePolicy newPolicy) {
    try {
      if (oldPolicy.getData().getScope().equals(newPolicy.getData().getScope())) {
        return;
      }

      Optional<EntityDerivationConfig> existing = findDummyEntity(requestContext);
      if (existing.isEmpty()) {
        log.warn(
            "Dummy entity not found during policy update for policy {}. Creating new one.",
            newPolicy.getId());
        EventDerivationConfigDetails entry =
            buildEventDerivationEntry(newPolicy.getId(), newPolicy.getData().getScope());
        createDummyEntity(requestContext, List.of(entry));
        return;
      }

      List<EventDerivationConfigDetails> entries =
          existing.get().getData().getSpanProjection().getEventDerivationConfigsList().stream()
              .filter(detail -> !detail.getName().equals(newPolicy.getId()))
              .collect(Collectors.toCollection(ArrayList::new));

      entries.add(buildEventDerivationEntry(newPolicy.getId(), newPolicy.getData().getScope()));
      updateDummyEntity(requestContext, entries);
    } catch (Exception e) {
      log.warn(
          "Failed to sync entity derivation for updated policy {}. "
              + "Policy was updated successfully but derivation scope may be stale.",
          newPolicy.getId(),
          e);
    }
  }

  public void onPoliciesDeleted(RequestContext requestContext, List<AbusePolicy> deletedPolicies) {
    try {
      Optional<EntityDerivationConfig> existing = findDummyEntity(requestContext);
      if (existing.isEmpty()) {
        return;
      }

      List<String> deletedIds =
          deletedPolicies.stream().map(AbusePolicy::getId).collect(Collectors.toList());

      List<EventDerivationConfigDetails> remaining =
          existing.get().getData().getSpanProjection().getEventDerivationConfigsList().stream()
              .filter(detail -> !deletedIds.contains(detail.getName()))
              .collect(Collectors.toList());

      if (remaining.isEmpty()) {
        deleteDummyEntity(requestContext);
      } else {
        updateDummyEntity(requestContext, remaining);
      }
    } catch (Exception e) {
      log.warn(
          "Failed to sync entity derivation after policy deletion. "
              + "Policies were deleted successfully but dummy derivation may have stale entries.",
          e);
    }
  }

  Optional<EntityDerivationConfig> findDummyEntity(RequestContext requestContext) {
    GetEntityDerivationConfigsRequest request =
        GetEntityDerivationConfigsRequest.newBuilder()
            .setFilter(
                EntityDerivationConfigFilter.newBuilder()
                    .setIncludeInternal(true)
                    .setIncludeDisabled(true))
            .addIds(DUMMY_ENTITY_ID)
            .build();

    GetEntityDerivationConfigsResponse response =
        requestContext.call(
            () -> entityDerivationConfigServiceStub.getEntityDerivationConfigs(request));

    return response.getEntityDerivationConfigsList().stream().findFirst();
  }

  private void createDummyEntity(
      RequestContext requestContext, List<EventDerivationConfigDetails> entries) {
    EntityDerivationConfigData data = buildDummyEntityData(entries);
    CreateEntityDerivationConfigRequest request =
        CreateEntityDerivationConfigRequest.newBuilder()
            .setId(DUMMY_ENTITY_ID)
            .setData(data)
            .build();

    CreateEntityDerivationConfigResponse response =
        requestContext.call(
            () -> entityDerivationConfigServiceStub.createEntityDerivationConfig(request));

    log.info(
        "Created dummy policy scope entity derivation with id={}",
        response.getEntityDerivationConfig().getId());
  }

  private void updateDummyEntity(
      RequestContext requestContext, List<EventDerivationConfigDetails> entries) {
    EntityDerivationConfigData data = buildDummyEntityData(entries);
    UpdateEntityDerivationConfigRequest request =
        UpdateEntityDerivationConfigRequest.newBuilder()
            .setId(DUMMY_ENTITY_ID)
            .setData(data)
            .build();

    requestContext.call(
        () -> entityDerivationConfigServiceStub.updateEntityDerivationConfig(request));
  }

  private void deleteDummyEntity(RequestContext requestContext) {
    DeleteEntityDerivationConfigRequest request =
        DeleteEntityDerivationConfigRequest.newBuilder()
            .setEntityDerivationConfigId(DUMMY_ENTITY_ID)
            .build();

    requestContext.call(
        () -> entityDerivationConfigServiceStub.deleteEntityDerivationConfig(request));

    log.info("Deleted dummy policy scope entity derivation with id={}", DUMMY_ENTITY_ID);
  }

  private EntityDerivationConfigData buildDummyEntityData(
      List<EventDerivationConfigDetails> entries) {
    return EntityDerivationConfigData.newBuilder()
        .setDisplayName(DUMMY_ENTITY_DISPLAY_NAME)
        .setDescription("Internal entity to ensure derivation processor runs for policy scopes")
        .setDisabled(false)
        .setInternal(true)
        .setCategory(EntityCategory.ENTITY_CATEGORY_SYSTEM)
        .setEventKind(ComplexDataModelEventKind.newBuilder().setKindId(EVENT_KIND_STRING))
        .setSpanProjection(SpanProjection.newBuilder().addAllEventDerivationConfigs(entries))
        .build();
  }

  static EventDerivationConfigDetails buildEventDerivationEntry(
      String policyId, AbusePolicyScope policyScope) {
    return EventDerivationConfigDetails.newBuilder()
        .setName(policyId)
        .setDisabled(false)
        .setScope(mapPolicyScopeToDerivationScope(policyScope))
        .setJexlExpression(JEXL_CONSTANT)
        .build();
  }

  static Scope mapPolicyScopeToDerivationScope(AbusePolicyScope policyScope) {
    Scope.Builder scopeBuilder = Scope.newBuilder();

    if (policyScope.hasEnvironmentScope()) {
      scopeBuilder.setEnvironmentScope(
          EnvironmentScope.newBuilder()
              .addAllEnvironments(policyScope.getEnvironmentScope().getEnvironmentIdsList()));
    } else {
      scopeBuilder.setEnvironmentScope(EnvironmentScope.getDefaultInstance());
    }

    if (policyScope.hasApiScope() && policyScope.getApiScope().hasApiIds()) {
      scopeBuilder.setEntityScope(
          EntityScope.newBuilder()
              .setEntityType(EntityType.ENTITY_TYPE_API)
              .addAllEntityIds(policyScope.getApiScope().getApiIds().getIdsList()));
    }
    // TODO: api_labels support - for now, omit entity_scope so derivation applies to all APIs

    return scopeBuilder.build();
  }
}
