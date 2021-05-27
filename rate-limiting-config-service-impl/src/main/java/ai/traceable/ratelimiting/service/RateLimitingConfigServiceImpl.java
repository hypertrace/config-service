package ai.traceable.ratelimiting.service;

import static ai.traceable.ratelimiting.service.RateLimitingConfigConstants.RATE_LIMITING_NAMESPACE;
import static ai.traceable.ratelimiting.service.RateLimitingConfigConstants.RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME;
import static ai.traceable.ratelimiting.service.RateLimitingConfigConstants.RULE_RATE_LIMITED_ENTITY_ASSOCIATION_RESOURCE_NAME;
import static ai.traceable.ratelimiting.service.RateLimitingConfigServiceUtils.getRuleRateLimitedEntityContext;
import static ai.traceable.ratelimiting.service.RateLimitingConfigServiceUtils.toRateLimitingRuleConfig;
import static ai.traceable.ratelimiting.service.RateLimitingConfigServiceUtils.toValue;

import ai.traceable.activity.event.SecurityConfigurationAction;
import ai.traceable.activity.event.SecurityConfigurationChange;
import ai.traceable.activity.event.SecurityConfigurationType;
import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.ratelimiting.config.service.v1.CreateRateLimitingRuleConfig;
import ai.traceable.ratelimiting.config.service.v1.CreateRuleConfigRequest;
import ai.traceable.ratelimiting.config.service.v1.CreateRuleConfigResponse;
import ai.traceable.ratelimiting.config.service.v1.CreateRuleRateLimitedEntityAssociationRequest;
import ai.traceable.ratelimiting.config.service.v1.CreateRuleRateLimitedEntityAssociationResponse;
import ai.traceable.ratelimiting.config.service.v1.DeleteRuleConfigRequest;
import ai.traceable.ratelimiting.config.service.v1.DeleteRuleConfigResponse;
import ai.traceable.ratelimiting.config.service.v1.DeleteRuleRateLimitedEntityAssociationRequest;
import ai.traceable.ratelimiting.config.service.v1.DeleteRuleRateLimitedEntityAssociationResponse;
import ai.traceable.ratelimiting.config.service.v1.GetAllRateLimitingRulesRequest;
import ai.traceable.ratelimiting.config.service.v1.GetAllRateLimitingRulesResponse;
import ai.traceable.ratelimiting.config.service.v1.GetRateLimitingConfigsForEntityRequest;
import ai.traceable.ratelimiting.config.service.v1.GetRateLimitingConfigsForEntityResponse;
import ai.traceable.ratelimiting.config.service.v1.RateLimitedEntity;
import ai.traceable.ratelimiting.config.service.v1.RateLimitedEntityType;
import ai.traceable.ratelimiting.config.service.v1.RateLimitingConfigServiceGrpc;
import ai.traceable.ratelimiting.config.service.v1.RateLimitingRuleConfig;
import ai.traceable.ratelimiting.config.service.v1.RateLimitingRuleWithRateLimitedEntities;
import ai.traceable.ratelimiting.config.service.v1.UpdateRuleConfigRequest;
import ai.traceable.ratelimiting.config.service.v1.UpdateRuleConfigResponse;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import com.typesafe.config.Config;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.GetConfigResponse;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class RateLimitingConfigServiceImpl
    extends RateLimitingConfigServiceGrpc.RateLimitingConfigServiceImplBase {

  private static final String RATE_LIMITING_CONFIG_SERVICE_CONFIG = "rate.limiting.config.service";
  private static final String MAX_CALL_COUNT_DURATION_LIMIT_MINUTES =
      "maxCallCountDurationLimitMinutes";
  private static final String PUBLISH_ACTIVITY_EVENTS_CONFIG = "publishActivityEvents";
  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private static long maxCallCountDurationLimit = TimeUnit.MINUTES.toMillis(180);
  private ActivityEventProducer activityEventProducer;
  private final boolean publishActivityEvents;

  public RateLimitingConfigServiceImpl(
      Channel configChannel, Config config, ActivityEventProducer activityEventProducer) {
    this.configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    Config rateLimitingConfigServiceConfig = config.getConfig(RATE_LIMITING_CONFIG_SERVICE_CONFIG);
    if (rateLimitingConfigServiceConfig.hasPath(MAX_CALL_COUNT_DURATION_LIMIT_MINUTES)) {
      maxCallCountDurationLimit =
          rateLimitingConfigServiceConfig.getDuration(
              MAX_CALL_COUNT_DURATION_LIMIT_MINUTES, TimeUnit.MILLISECONDS);
    }
    this.activityEventProducer = activityEventProducer;
    this.publishActivityEvents =
        rateLimitingConfigServiceConfig.getBoolean(PUBLISH_ACTIVITY_EVENTS_CONFIG);
  }

  @Override
  public void getAllRateLimitingRules(
      GetAllRateLimitingRulesRequest request,
      StreamObserver<GetAllRateLimitingRulesResponse> responseObserver) {
    try {
      // Get all rate limiting rules.
      GetAllConfigsRequest getAllRuleConfigsRequest =
          GetAllConfigsRequest.newBuilder()
              .setResourceName(RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME)
              .setResourceNamespace(RATE_LIMITING_NAMESPACE)
              .build();

      List<ContextSpecificConfig> contextSpecificRuleConfigs =
          configServiceBlockingStub
              .getAllConfigs(getAllRuleConfigsRequest)
              .getContextSpecificConfigsList();

      // Get all rule to entity associations
      GetAllConfigsRequest getAllAssociationsRequest =
          GetAllConfigsRequest.newBuilder()
              .setResourceName(RULE_RATE_LIMITED_ENTITY_ASSOCIATION_RESOURCE_NAME)
              .setResourceNamespace(RATE_LIMITING_NAMESPACE)
              .build();

      List<ContextSpecificConfig> contextSpecificAssociations =
          configServiceBlockingStub
              .getAllConfigs(getAllAssociationsRequest)
              .getContextSpecificConfigsList();

      // get list of rules with associated entities.
      List<RateLimitingRuleWithRateLimitedEntities> rulesWithAssociatedEntities =
          getRuleIdToRateLimitedEntityAssociations(
              contextSpecificRuleConfigs, contextSpecificAssociations);

      GetAllRateLimitingRulesResponse.Builder responseBuilder =
          GetAllRateLimitingRulesResponse.newBuilder().addAllRules(rulesWithAssociatedEntities);

      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error getting all rules for request {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getRateLimitingConfigsForEntity(
      GetRateLimitingConfigsForEntityRequest request,
      StreamObserver<GetRateLimitingConfigsForEntityResponse> responseObserver) {
    Optional<String> maybeRuleId = getRuleIdForAssociatedEntity(request.getEntity());
    if (maybeRuleId.isEmpty()) {
      log.debug("No Rate Limiting configured for entity:{}", request.getEntity());
      responseObserver.onNext(GetRateLimitingConfigsForEntityResponse.newBuilder().build());
      responseObserver.onCompleted();
      return;
    }

    // Get Rate Limit Rule config corresponding to the rule id
    String ruleId = maybeRuleId.get();
    try {
      GetConfigResponse rateLimitConfigResponse =
          configServiceBlockingStub.getConfig(
              GetConfigRequest.newBuilder()
                  .setResourceNamespace(RATE_LIMITING_NAMESPACE)
                  .setResourceName(RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME)
                  .addContexts(ruleId)
                  .build());
      RateLimitingRuleConfig ruleConfig =
          toRateLimitingRuleConfig(rateLimitConfigResponse.getConfig());
      responseObserver.onNext(
          GetRateLimitingConfigsForEntityResponse.newBuilder().addRule(ruleConfig).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error getting rule config for ruleId:{}", ruleId, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createRuleConfig(
      CreateRuleConfigRequest request, StreamObserver<CreateRuleConfigResponse> responseObserver) {
    try {
      String ruleId = UUID.randomUUID().toString();
      CreateRateLimitingRuleConfig ruleConfig = request.getRule();

      if (!isMaxCallCountDurationWithinLimit(ruleConfig.getMaxCallCountDurationMillis())) {
        throw new IllegalArgumentException("max calls count duration value not allowed");
      }

      RateLimitingRuleConfig createdRuleConfig =
          RateLimitingRuleConfig.newBuilder()
              .setRuleId(ruleId)
              .setRuleName(ruleConfig.getRuleName())
              .setDescription(ruleConfig.getDescription())
              .setDisabled(false)
              .setMaxCallCountAllowed(ruleConfig.getMaxCallCountAllowed())
              .setMaxCallCountDurationMillis(ruleConfig.getMaxCallCountDurationMillis())
              .setRuleViolationAction(ruleConfig.getRuleViolationAction())
              .setSuspendDurationMillis(ruleConfig.getSuspendDurationMillis())
              .build();

      UpsertConfigRequest upsertConfigRequest =
          UpsertConfigRequest.newBuilder()
              .setResourceName(RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME)
              .setResourceNamespace(RATE_LIMITING_NAMESPACE)
              .setConfig(toValue(createdRuleConfig))
              .setContext(ruleId)
              .build();

      configServiceBlockingStub.upsertConfig(upsertConfigRequest);

      CreateRuleConfigResponse createRuleConfigResponse =
          CreateRuleConfigResponse.newBuilder().setRule(createdRuleConfig).build();
      responseObserver.onNext(createRuleConfigResponse);
      responseObserver.onCompleted();

      if (publishActivityEvents) {
        activityEventProducer.publishSecurityConfigurationChangeEvent(
            RequestContext.CURRENT.get(),
            getSecurityConfigurationChangeEvent(
                createdRuleConfig, SecurityConfigurationAction.ADD));
      }
    } catch (Exception e) {
      log.error("Error occurred while creating the Rate Limiting Rule for request {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteRuleConfig(
      DeleteRuleConfigRequest request, StreamObserver<DeleteRuleConfigResponse> responseObserver) {
    try {
      // RuleId to delete
      String ruleId = request.getRuleId();

      GetConfigResponse rateLimitConfigResponse =
          configServiceBlockingStub.getConfig(
              GetConfigRequest.newBuilder()
                  .setResourceName(RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME)
                  .setResourceNamespace(RATE_LIMITING_NAMESPACE)
                  .addContexts(ruleId)
                  .build());
      RateLimitingRuleConfig ruleConfig =
          toRateLimitingRuleConfig(rateLimitConfigResponse.getConfig());

      // Get all rule to rate limited entities associations
      GetAllConfigsRequest getAllAssociationsRequest =
          GetAllConfigsRequest.newBuilder()
              .setResourceName(RULE_RATE_LIMITED_ENTITY_ASSOCIATION_RESOURCE_NAME)
              .setResourceNamespace(RATE_LIMITING_NAMESPACE)
              .build();

      // get associations with rule to be deleted.
      List<ContextSpecificConfig> contextSpecificAssociationsToDelete =
          configServiceBlockingStub
              .getAllConfigs(getAllAssociationsRequest)
              .getContextSpecificConfigsList()
              .stream()
              .filter(c -> c.getConfig().getStringValue().equals(ruleId))
              .collect(Collectors.toList());

      // Delete associations with rule to be deleted.
      deleteAllAssociationForRule(contextSpecificAssociationsToDelete);

      // Delete rule config
      DeleteConfigRequest deleteConfigRequest =
          DeleteConfigRequest.newBuilder()
              .setResourceName(RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME)
              .setResourceNamespace(RATE_LIMITING_NAMESPACE)
              .setContext(ruleId)
              .build();

      configServiceBlockingStub.deleteConfig(deleteConfigRequest);

      DeleteRuleConfigResponse deleteRuleConfigResponse =
          DeleteRuleConfigResponse.newBuilder().build();
      responseObserver.onNext(deleteRuleConfigResponse);
      responseObserver.onCompleted();

      if (publishActivityEvents) {
        activityEventProducer.publishSecurityConfigurationChangeEvent(
            RequestContext.CURRENT.get(),
            getSecurityConfigurationChangeEvent(ruleConfig, SecurityConfigurationAction.REMOVE));
      }
    } catch (Exception e) {
      log.error("Error while deleting the Rate Limiting Rule for request {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateRuleConfig(
      UpdateRuleConfigRequest request, StreamObserver<UpdateRuleConfigResponse> responseObserver) {
    try {
      RateLimitingRuleConfig updatedRuleConfig = request.getRule();

      // This method throws an exception if rule config being updated doesn't exist.
      validateRuleExistsBeforeUpdate(updatedRuleConfig);

      UpsertConfigRequest upsertConfigRequest =
          UpsertConfigRequest.newBuilder()
              .setContext(updatedRuleConfig.getRuleId())
              .setResourceName(RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME)
              .setResourceNamespace(RATE_LIMITING_NAMESPACE)
              .setConfig(toValue(updatedRuleConfig))
              .build();

      configServiceBlockingStub.upsertConfig(upsertConfigRequest);

      UpdateRuleConfigResponse updateRuleConfigResponse =
          UpdateRuleConfigResponse.newBuilder().setRule(updatedRuleConfig).build();
      responseObserver.onNext(updateRuleConfigResponse);
      responseObserver.onCompleted();

      if (publishActivityEvents) {
        activityEventProducer.publishSecurityConfigurationChangeEvent(
            RequestContext.CURRENT.get(),
            getSecurityConfigurationChangeEvent(
                updatedRuleConfig, SecurityConfigurationAction.UPDATE));
      }
    } catch (Exception e) {
      log.error("Error while updating Rate limiting Rule for request {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createRuleRateLimitedEntityAssociation(
      CreateRuleRateLimitedEntityAssociationRequest request,
      StreamObserver<CreateRuleRateLimitedEntityAssociationResponse> responseObserver) {
    try {
      RateLimitedEntity rateLimitedEntity = request.getEntity();
      String context = getRuleRateLimitedEntityContext(rateLimitedEntity);
      Value associationConfig = Value.newBuilder().setStringValue(request.getRuleId()).build();

      UpsertConfigRequest upsertConfigRequest =
          UpsertConfigRequest.newBuilder()
              .setResourceName(RULE_RATE_LIMITED_ENTITY_ASSOCIATION_RESOURCE_NAME)
              .setResourceNamespace(RATE_LIMITING_NAMESPACE)
              .setContext(context)
              .setConfig(associationConfig)
              .build();

      configServiceBlockingStub.upsertConfig(upsertConfigRequest);

      CreateRuleRateLimitedEntityAssociationResponse
          createRuleRateLimitedEntityAssociationResponse =
              CreateRuleRateLimitedEntityAssociationResponse.newBuilder().build();
      responseObserver.onNext(createRuleRateLimitedEntityAssociationResponse);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error while associating rate limited entity with rule {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteRuleRateLimitedEntityAssociation(
      DeleteRuleRateLimitedEntityAssociationRequest request,
      StreamObserver<DeleteRuleRateLimitedEntityAssociationResponse> responseObserver) {
    try {
      RateLimitedEntity rateLimitedEntity = request.getEntity();
      String context = getRuleRateLimitedEntityContext(rateLimitedEntity);

      DeleteConfigRequest deleteConfigRequest =
          DeleteConfigRequest.newBuilder()
              .setResourceName(RULE_RATE_LIMITED_ENTITY_ASSOCIATION_RESOURCE_NAME)
              .setResourceNamespace(RATE_LIMITING_NAMESPACE)
              .setContext(context)
              .build();

      configServiceBlockingStub.deleteConfig(deleteConfigRequest);

      DeleteRuleRateLimitedEntityAssociationResponse
          deleteRuleRateLimitedEntityAssociationResponse =
              DeleteRuleRateLimitedEntityAssociationResponse.newBuilder().build();
      responseObserver.onNext(deleteRuleRateLimitedEntityAssociationResponse);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error while deleting Rule Rate Limited entity association {}", request, e);
      responseObserver.onError(e);
    }
  }

  private Optional<String> getRuleIdForAssociatedEntity(RateLimitedEntity rateLimitedEntity) {
    try {
      // Get Rate limit rule association configs
      String entityContext = getRuleRateLimitedEntityContext(rateLimitedEntity);
      GetConfigResponse rateLimitEntityAssociationConfigResponse =
          configServiceBlockingStub.getConfig(
              GetConfigRequest.newBuilder()
                  .setResourceNamespace(RATE_LIMITING_NAMESPACE)
                  .setResourceName(RULE_RATE_LIMITED_ENTITY_ASSOCIATION_RESOURCE_NAME)
                  .addContexts(entityContext)
                  .build());
      return Optional.of(rateLimitEntityAssociationConfigResponse.getConfig().getStringValue());
    } catch (Exception ex) {
      if (!Status.fromThrowable(ex).equals(Status.NOT_FOUND)) {
        log.error(
            "Error fetching Rate Limit Association config for entity:{}", rateLimitedEntity, ex);
      }
    }
    return Optional.empty();
  }

  private List<RateLimitingRuleWithRateLimitedEntities> getRuleIdToRateLimitedEntityAssociations(
      List<ContextSpecificConfig> contextSpecificRuleConfigs,
      List<ContextSpecificConfig> contextSpecificAssociations)
      throws InvalidProtocolBufferException {
    // Map of ruleId to entities associated
    Map<String, List<RateLimitedEntity>> rulesWithAssociatedEntities = new HashMap<>();

    // Populate map of ruleId to entities associated.
    for (ContextSpecificConfig contextSpecificAssociation : contextSpecificAssociations) {
      RateLimitedEntity rateLimitedEntity =
          getRateLimitedEntityFromAssociationContext(contextSpecificAssociation.getContext());
      String ruleId = contextSpecificAssociation.getConfig().getStringValue();
      if (!rulesWithAssociatedEntities.containsKey(ruleId)) {
        rulesWithAssociatedEntities.put(ruleId, new ArrayList<>());
      }
      rulesWithAssociatedEntities.get(ruleId).add(rateLimitedEntity);
    }

    List<RateLimitingRuleWithRateLimitedEntities> rateLimitingRuleWithRateLimitedEntities =
        new ArrayList<>();

    // Create list of rules with associated entities.
    for (ContextSpecificConfig contextSpecificRuleConfig : contextSpecificRuleConfigs) {
      String ruleId = contextSpecificRuleConfig.getContext();
      RateLimitingRuleConfig ruleConfig =
          toRateLimitingRuleConfig(contextSpecificRuleConfig.getConfig());
      List<RateLimitedEntity> associatedEntities = new ArrayList<>();
      if (rulesWithAssociatedEntities.containsKey(ruleId)) {
        associatedEntities = rulesWithAssociatedEntities.get(ruleId);
      }
      RateLimitingRuleWithRateLimitedEntities ruleWithAssociation =
          RateLimitingRuleWithRateLimitedEntities.newBuilder()
              .addAllEntitiesAssociated(associatedEntities)
              .setRule(ruleConfig)
              .build();
      rateLimitingRuleWithRateLimitedEntities.add(ruleWithAssociation);
    }
    return rateLimitingRuleWithRateLimitedEntities;
  }

  private boolean isMaxCallCountDurationWithinLimit(long inputDuration) {
    return inputDuration <= maxCallCountDurationLimit;
  }

  private RateLimitedEntity getRateLimitedEntityFromAssociationContext(String associationContext) {
    String[] typeEntityId = associationContext.split(":");
    return RateLimitedEntity.newBuilder()
        .setEntityId(typeEntityId[1])
        .setEntityType(RateLimitedEntityType.valueOf(typeEntityId[0]))
        .build();
  }

  private void validateRuleExistsBeforeUpdate(RateLimitingRuleConfig updatedRuleConfig) {
    try {
      GetConfigRequest getConfigRequest =
          GetConfigRequest.newBuilder()
              .addAllContexts(List.of(updatedRuleConfig.getRuleId()))
              .setResourceName(RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME)
              .setResourceNamespace(RATE_LIMITING_NAMESPACE)
              .build();
      configServiceBlockingStub.getConfig(getConfigRequest).getConfig();
    } catch (Exception e) {
      if (Status.fromThrowable(e).equals(Status.NOT_FOUND)) {
        throw e;
      }
    }
  }

  private void deleteAllAssociationForRule(
      List<ContextSpecificConfig> contextSpecificAssociationsToBeDeleted) {
    for (ContextSpecificConfig contextSpecificAssociation :
        contextSpecificAssociationsToBeDeleted) {
      DeleteConfigRequest deleteConfigRequest =
          DeleteConfigRequest.newBuilder()
              .setResourceName(RULE_RATE_LIMITED_ENTITY_ASSOCIATION_RESOURCE_NAME)
              .setResourceNamespace(RATE_LIMITING_NAMESPACE)
              .setContext(contextSpecificAssociation.getContext())
              .build();
      configServiceBlockingStub.deleteConfig(deleteConfigRequest);
    }
  }

  private SecurityConfigurationChange getSecurityConfigurationChangeEvent(
      RateLimitingRuleConfig rateLimitingRuleConfig,
      SecurityConfigurationAction securityConfigurationAction) {
    return SecurityConfigurationChange.newBuilder()
        .setRuleId(rateLimitingRuleConfig.getRuleId())
        .setRuleName(rateLimitingRuleConfig.getRuleName())
        .setSecurityConfigurationType(SecurityConfigurationType.RATE_LIMITING_RULE)
        .setSecurityConfigurationAction(securityConfigurationAction)
        .build();
  }
}
