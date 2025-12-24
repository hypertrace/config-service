package ai.traceable.entity.fetcher.cache;

import ai.traceable.entity.fetcher.cache.config.CachedEntityFetcherConfig;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.entity.change.event.v1.EntityChangeEventKey;
import org.hypertrace.entity.change.event.v1.EntityChangeEventValue;
import org.hypertrace.entity.data.service.v1.AttributeFilter;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.AttributeValueList;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc;
import org.hypertrace.entity.data.service.v1.Operator;
import org.hypertrace.entity.data.service.v1.Query;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.data.service.v2.EntityRelationshipServiceGrpc;
import org.hypertrace.entity.data.service.v2.EntityRelationshipV2;
import org.hypertrace.entity.data.service.v2.Filter;
import org.hypertrace.entity.data.service.v2.GetEntityRelationshipsRequest;
import org.hypertrace.entity.data.service.v2.GetEntityRelationshipsResponse;
import org.hypertrace.entity.data.service.v2.RelationalFilter;
import org.hypertrace.entity.data.service.v2.RelationalOperator;

@Singleton
@Slf4j
public class SecuritySchemeProviderImpl implements SecuritySchemeProvider {

  private static final String USER_ROLE = "USER_ROLE";
  private static final String USER_SCOPE = "USER_SCOPE";
  private static final String USER_ROLE_API = "USER_ROLE_API";
  private static final String USER_SCOPE_API = "USER_SCOPE_API";
  private static final String PARENT_ROLE_IDS = "parentRoleIds";
  private static final String PARENT_SCOPE_IDS = "parentScopeIds";
  private static final String USER_DEFINED = "userDefined";
  private static final String TO_API_ID = "toApiId";
  public static final String ATTRIBUTES_ID = "attributes.id";
  private static final String API_ROLE_MAPPING_CACHE_NAME = "apiRoleMappingCache";
  private static final String API_SCOPE_MAPPING_CACHE_NAME = "apiScopeMappingCache";

  private final LoadingCache<ContextualKey<String>, Set<UserRoleSecurityScheme>>
      apiRoleMappingCache;
  private final LoadingCache<ContextualKey<String>, Set<UserScopeSecurityScheme>>
      apiScopeMappingCache;
  private final EntityRelationshipServiceGrpc.EntityRelationshipServiceBlockingStub
      entityRelationshipService;
  private final EntityDataServiceGrpc.EntityDataServiceBlockingStub entityDataService;
  private final long timeoutMillis;

  @Inject
  SecuritySchemeProviderImpl(
      CachedEntityFetcherConfig config,
      EntityRelationshipServiceGrpc.EntityRelationshipServiceBlockingStub entityRelationshipService,
      EntityDataServiceGrpc.EntityDataServiceBlockingStub entityDataService,
      KafkaLiveEventListener<EntityChangeEventKey, EntityChangeEventValue> kafkaLiveEventListener) {
    this.entityRelationshipService = entityRelationshipService;
    this.entityDataService = entityDataService;
    this.timeoutMillis = config.getSecuritySchemeCacheExpireAfterAccessDuration().toMillis();
    this.apiRoleMappingCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(config.getSecuritySchemeCacheExpireAfterAccessDuration())
            .refreshAfterWrite(config.getSecuritySchemeCacheRefreshAfterWriteDuration())
            .maximumSize(config.getSecuritySchemeCacheMaxSize())
            .recordStats()
            .build(CacheLoader.from(this::loadApiRoleMapping));
    this.apiScopeMappingCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(config.getSecuritySchemeCacheExpireAfterAccessDuration())
            .refreshAfterWrite(config.getSecuritySchemeCacheRefreshAfterWriteDuration())
            .maximumSize(config.getSecuritySchemeCacheMaxSize())
            .recordStats()
            .build(CacheLoader.from(this::loadApiScopeMapping));
    PlatformMetricsRegistry.registerCacheTrackingOccupancy(
        API_ROLE_MAPPING_CACHE_NAME,
        this.apiRoleMappingCache,
        Collections.emptyMap(),
        config.getSecuritySchemeCacheMaxSize());

    PlatformMetricsRegistry.registerCacheTrackingOccupancy(
        API_SCOPE_MAPPING_CACHE_NAME,
        this.apiScopeMappingCache,
        Collections.emptyMap(),
        config.getSecuritySchemeCacheMaxSize());
    try {
      kafkaLiveEventListener.registerCallback(this::handleEntityChangeEvent);
    } catch (Exception e) {
      log.error("Error registering entity change event listener", e);
    }
  }

  private Set<UserRoleSecurityScheme> loadApiRoleMapping(ContextualKey<String> cacheKey) {
    try {
      String tenantId = cacheKey.getContext().getTenantId().orElseThrow();
      String apiId = cacheKey.getData();
      List<EntityRelationshipV2> relationships =
          fetchEntityRelationships(apiId, tenantId, USER_ROLE_API);

      if (relationships.isEmpty()) {
        return Collections.emptySet();
      }
      Map<String, Boolean> roleRelationshipUserDefined = new HashMap<>();
      Set<String> directRoleIds = new HashSet<>();
      for (EntityRelationshipV2 relationship : relationships) {
        String roleId = relationship.getFromEntityId();
        boolean isUserDefined =
            relationship.getAttributesMap().containsKey(USER_DEFINED)
                && relationship.getAttributesMap().get(USER_DEFINED).getBoolValue();
        roleRelationshipUserDefined.put(roleId, isUserDefined);
        directRoleIds.add(roleId);
      }

      Map<String, RoleEntity> roleMap =
          fetchRolesWithHierarchy(tenantId, directRoleIds, new HashSet<>());

      if (roleMap.isEmpty()) {
        return Collections.emptySet();
      }

      Set<UserRoleSecurityScheme> resolvedRoles = new LinkedHashSet<>();
      for (String roleId : roleRelationshipUserDefined.keySet()) {
        Set<UserRoleSecurityScheme> hierarchyRoles =
            resolveRoleHierarchy(roleId, roleMap, roleRelationshipUserDefined);
        resolvedRoles.addAll(hierarchyRoles);
      }
      return resolvedRoles;
    } catch (Exception e) {
      log.error(
          "Error loading API role mapping for apiId:{} and tenantId:{}",
          cacheKey.getData(),
          cacheKey.getContext(),
          e);
      return Collections.emptySet();
    }
  }

  private Set<UserScopeSecurityScheme> loadApiScopeMapping(ContextualKey<String> cacheKey) {
    try {
      String tenantId = cacheKey.getContext().getTenantId().orElseThrow();
      String apiId = cacheKey.getData();
      List<EntityRelationshipV2> relationships =
          fetchEntityRelationships(apiId, tenantId, USER_SCOPE_API);

      if (relationships.isEmpty()) {
        return Collections.emptySet();
      }
      Map<String, Boolean> scopeRelationshipUserDefined = new HashMap<>();
      Set<String> directScopeIds = new HashSet<>();
      for (EntityRelationshipV2 relationship : relationships) {
        String scopeId = relationship.getFromEntityId();
        boolean isUserDefined =
            relationship.getAttributesMap().containsKey(USER_DEFINED)
                && relationship.getAttributesMap().get(USER_DEFINED).getBoolValue();
        scopeRelationshipUserDefined.put(scopeId, isUserDefined);
        directScopeIds.add(scopeId);
      }

      Map<String, ScopeEntity> scopeMap =
          fetchScopesWithHierarchy(tenantId, directScopeIds, new HashSet<>());

      if (scopeMap.isEmpty()) {
        return Collections.emptySet();
      }
      Set<UserScopeSecurityScheme> resolvedScopes = new LinkedHashSet<>();
      for (String scopeId : scopeRelationshipUserDefined.keySet()) {
        Set<UserScopeSecurityScheme> hierarchyScopes =
            resolveScopeHierarchy(scopeId, scopeMap, scopeRelationshipUserDefined);
        resolvedScopes.addAll(hierarchyScopes);
      }

      return resolvedScopes;
    } catch (Exception e) {
      log.error(
          "Error loading API scope mapping for apiId:{} and tenantId:{}",
          cacheKey.getData(),
          cacheKey.getContext(),
          e);
      return Collections.emptySet();
    }
  }

  private Map<String, RoleEntity> fetchRolesWithHierarchy(
      String tenantId, Set<String> roleIds, Set<String> alreadyFetched) {
    return fetchEntitiesWithHierarchy(
        tenantId,
        roleIds,
        alreadyFetched,
        USER_ROLE,
        PARENT_ROLE_IDS,
        RoleEntity::new,
        (parentIds) -> fetchRolesWithHierarchy(tenantId, parentIds, alreadyFetched));
  }

  private Set<UserRoleSecurityScheme> resolveRoleHierarchy(
      String roleId,
      Map<String, RoleEntity> roleMap,
      Map<String, Boolean> roleRelationshipUserDefined) {
    Set<UserRoleSecurityScheme> resolvedRoles = new LinkedHashSet<>();
    Set<String> visited = new HashSet<>();

    resolveRoleHierarchyRecursive(
        roleId, roleMap, roleRelationshipUserDefined, resolvedRoles, visited, false);
    return resolvedRoles;
  }

  private void resolveRoleHierarchyRecursive(
      String roleId,
      Map<String, RoleEntity> roleMap,
      Map<String, Boolean> roleRelationshipUserDefined,
      Set<UserRoleSecurityScheme> resolvedRoles,
      Set<String> visited,
      boolean isUserDefined) {
    resolveHierarchyRecursive(
        roleId,
        roleMap,
        roleRelationshipUserDefined,
        visited,
        isUserDefined,
        (role, userDef) ->
            resolvedRoles.add(
                new UserRoleSecurityScheme(role.getRoleId(), role.getRoleName(), userDef)),
        RoleEntity::getParentRoleIds);
  }

  private Map<String, ScopeEntity> fetchScopesWithHierarchy(
      String tenantId, Set<String> scopeIds, Set<String> alreadyFetched) {
    return fetchEntitiesWithHierarchy(
        tenantId,
        scopeIds,
        alreadyFetched,
        USER_SCOPE,
        PARENT_SCOPE_IDS,
        ScopeEntity::new,
        (parentIds) -> fetchScopesWithHierarchy(tenantId, parentIds, alreadyFetched));
  }

  @FunctionalInterface
  private interface EntityCreator<T> {
    T create(String id, String name, List<String> parentIds);
  }

  private <T> Map<String, T> fetchEntitiesWithHierarchy(
      String tenantId,
      Set<String> entityIds,
      Set<String> alreadyFetched,
      String entityType,
      String parentIdsAttribute,
      EntityCreator<T> entityCreator,
      Function<Set<String>, Map<String, T>> recursiveFetcher) {
    Map<String, T> entityMap = new HashMap<>();
    Set<String> toFetch = new HashSet<>(entityIds);
    toFetch.removeAll(alreadyFetched);

    if (toFetch.isEmpty()) {
      return entityMap;
    }

    List<Entity> entities = queryEntitiesById(tenantId, entityType, toFetch);
    Set<String> parentIdsToFetch = new HashSet<>();

    for (Entity entity : entities) {
      String entityId = entity.getEntityId();
      String entityName = entity.getEntityName();
      List<String> parentIds =
          Optional.ofNullable(entity.getAttributesMap().get(parentIdsAttribute))
              .map(AttributeValue::getValueList)
              .map(org.hypertrace.entity.data.service.v1.AttributeValueList::getValuesList)
              .orElse(Collections.emptyList())
              .stream()
              .map(AttributeValue::getValue)
              .map(org.hypertrace.entity.data.service.v1.Value::getString)
              .collect(Collectors.toList());

      entityMap.put(entityId, entityCreator.create(entityId, entityName, parentIds));
      alreadyFetched.add(entityId);
      for (String parentId : parentIds) {
        if (!alreadyFetched.contains(parentId)) {
          parentIdsToFetch.add(parentId);
        }
      }
    }
    if (!parentIdsToFetch.isEmpty()) {
      Map<String, T> parentEntities = recursiveFetcher.apply(parentIdsToFetch);
      entityMap.putAll(parentEntities);
    }

    return entityMap;
  }

  private Set<UserScopeSecurityScheme> resolveScopeHierarchy(
      String scopeId,
      Map<String, ScopeEntity> scopeMap,
      Map<String, Boolean> scopeRelationshipUserDefined) {
    Set<UserScopeSecurityScheme> resolvedScopes = new LinkedHashSet<>();
    Set<String> visited = new HashSet<>();

    resolveScopeHierarchyRecursive(
        scopeId, scopeMap, scopeRelationshipUserDefined, resolvedScopes, visited, false);
    return resolvedScopes;
  }

  private void resolveScopeHierarchyRecursive(
      String scopeId,
      Map<String, ScopeEntity> scopeMap,
      Map<String, Boolean> scopeRelationshipUserDefined,
      Set<UserScopeSecurityScheme> resolvedScopes,
      Set<String> visited,
      boolean isUserDefined) {
    resolveHierarchyRecursive(
        scopeId,
        scopeMap,
        scopeRelationshipUserDefined,
        visited,
        isUserDefined,
        (scope, userDef) ->
            resolvedScopes.add(
                new UserScopeSecurityScheme(scope.getScopeId(), scope.getScopeName(), userDef)),
        ScopeEntity::getParentScopeIds);
  }

  private <T> void resolveHierarchyRecursive(
      String entityId,
      Map<String, T> entityMap,
      Map<String, Boolean> relationshipUserDefined,
      Set<String> visited,
      boolean isUserDefined,
      BiConsumer<T, Boolean> processEntity,
      Function<T, List<String>> getParentIds) {
    if (visited.contains(entityId)) {
      return;
    }
    visited.add(entityId);

    T entity = entityMap.get(entityId);
    if (entity != null) {
      boolean currentIsUserDefined =
          isUserDefined || relationshipUserDefined.getOrDefault(entityId, false);
      processEntity.accept(entity, currentIsUserDefined);

      for (String parentId : getParentIds.apply(entity)) {
        resolveHierarchyRecursive(
            parentId,
            entityMap,
            relationshipUserDefined,
            visited,
            currentIsUserDefined,
            processEntity,
            getParentIds);
      }
    }
  }

  private List<Entity> queryEntitiesById(
      String tenantId, String entityType, Set<String> entityIds) {
    if (entityIds.isEmpty()) {
      return Collections.emptyList();
    }
    try {
      RequestContext context = RequestContext.forTenantId(tenantId);
      Query query = buildEntityQueryWithIds(entityType, entityIds);

      Iterator<Entity> entityIterator =
          context.call(
              () ->
                  entityDataService
                      .withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                      .query(query));

      List<Entity> entities = new java.util.ArrayList<>();
      while (entityIterator.hasNext()) {
        entities.add(entityIterator.next());
      }
      return entities;
    } catch (Exception e) {
      if (log.isDebugEnabled()) {
        log.debug(
            "Failed to query {} entities with IDs {}: {}",
            entityType,
            entityIds,
            e.getMessage(),
            e);
      }
      return Collections.emptyList();
    }
  }

  private Query buildEntityQueryWithIds(String entityType, Set<String> entityIds) {
    AttributeValueList valueList = createValueListFromIds(entityIds);

    AttributeFilter filter =
        AttributeFilter.newBuilder()
            .setName(ATTRIBUTES_ID)
            .setOperator(Operator.IN)
            .setAttributeValue(AttributeValue.newBuilder().setValueList(valueList).build())
            .build();

    return Query.newBuilder().setEntityType(entityType).setFilter(filter).build();
  }

  private AttributeValueList createValueListFromIds(Set<String> entityIds) {
    AttributeValueList.Builder valueList = AttributeValueList.newBuilder();
    for (String id : entityIds) {
      valueList.addValues(
          AttributeValue.newBuilder().setValue(Value.newBuilder().setString(id).build()).build());
    }
    return valueList.build();
  }

  private List<EntityRelationshipV2> fetchEntityRelationships(
      String toApiId, String tenantId, String relationshipScope) {
    RequestContext context = RequestContext.forTenantId(tenantId);
    GetEntityRelationshipsRequest request =
        GetEntityRelationshipsRequest.newBuilder()
            .setRelationshipScope(relationshipScope)
            .setFilter(
                Filter.newBuilder()
                    .setRelationalFilter(
                        RelationalFilter.newBuilder()
                            .setValue(
                                com.google.protobuf.Value.newBuilder()
                                    .setStringValue(toApiId)
                                    .build())
                            .setOperator(RelationalOperator.RELATIONAL_OPERATOR_EQUALS)
                            .setField(TO_API_ID)
                            .build())
                    .build())
            .build();
    GetEntityRelationshipsResponse entityRelationships =
        context.call(
            () ->
                entityRelationshipService
                    .withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                    .getEntityRelationships(request));
    return entityRelationships.getEntityRelationshipsList();
  }

  @Override
  public Set<UserRoleSecurityScheme> getUserRoleSecuritySchemes(
      RequestContext requestContext, String apiId) {
    try {
      ContextualKey<String> cacheKey = requestContext.buildInternalContextualKey(apiId);
      return apiRoleMappingCache.getUnchecked(cacheKey);
    } catch (Exception e) {
      log.error(
          "Error fetching user role security schemes for apiId: {} in context: {}",
          apiId,
          requestContext,
          e);
      return Collections.emptySet();
    }
  }

  @Override
  public Set<UserScopeSecurityScheme> getUserScopeSecuritySchemes(
      RequestContext requestContext, String apiId) {
    try {
      ContextualKey<String> cacheKey = requestContext.buildInternalContextualKey(apiId);
      return apiScopeMappingCache.getUnchecked(cacheKey);
    } catch (Exception e) {
      log.error(
          "Error fetching user scope security schemes for apiId: {} in context: {}",
          apiId,
          requestContext,
          e);
      return Collections.emptySet();
    }
  }

  private void handleEntityChangeEvent(
      EntityChangeEventKey eventKey, EntityChangeEventValue eventValue) {
    if (!eventKey.getEntityType().equals(USER_ROLE)
        && !eventKey.getEntityType().equals(USER_SCOPE)) {
      return;
    }
    switch (eventValue.getEventCase()) {
      case CREATE_EVENT:
      case UPDATE_EVENT:
      case DELETE_EVENT:
        invalidateCacheForTenant(eventKey.getTenantId());
        break;
      default:
        log.warn(
            "Entity change event value has invalid event type -> {}", eventValue.getEventCase());
    }
  }

  private void invalidateCacheForTenant(String tenantId) {
    if (log.isDebugEnabled()) {
      log.debug("Invalidated security scheme cache entries for tenant: {}", tenantId);
    }
    apiRoleMappingCache.asMap().keySet().stream()
        .filter(key -> key.getContext().getTenantId().filter(tenantId::equals).isPresent())
        .forEach(apiRoleMappingCache::invalidate);

    apiScopeMappingCache.asMap().keySet().stream()
        .filter(key -> key.getContext().getTenantId().filter(tenantId::equals).isPresent())
        .forEach(apiScopeMappingCache::invalidate);
  }

  @Getter
  private static class RoleEntity {
    private final String roleId;
    private final String roleName;
    private final List<String> parentRoleIds;

    RoleEntity(String roleId, String roleName, List<String> parentRoleIds) {
      this.roleId = roleId;
      this.roleName = roleName;
      this.parentRoleIds = parentRoleIds != null ? parentRoleIds : Collections.emptyList();
    }
  }

  @Getter
  private static class ScopeEntity {
    private final String scopeId;
    private final String scopeName;
    private final List<String> parentScopeIds;

    ScopeEntity(String scopeId, String scopeName, List<String> parentScopeIds) {
      this.scopeId = scopeId;
      this.scopeName = scopeName;
      this.parentScopeIds = parentScopeIds != null ? parentScopeIds : Collections.emptyList();
    }
  }
}
