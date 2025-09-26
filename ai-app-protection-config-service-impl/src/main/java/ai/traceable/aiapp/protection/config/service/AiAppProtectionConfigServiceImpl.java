package ai.traceable.aiapp.protection.config.service;

import ai.traceable.aiapp.protection.config.service.converter.AnomalyToAiAppRuleConverter;
import ai.traceable.aiapp.protection.config.service.converter.customsignature.AiAppToCustomSignatureConverter;
import ai.traceable.aiapp.protection.config.service.converter.customsignature.CustomSignatureToAiAppConverter;
import ai.traceable.aiapp.protection.config.service.converter.ratelimit.AiAppToRateLimitingConverter;
import ai.traceable.aiapp.protection.config.service.converter.ratelimit.RateLimitingToAiAppConverter;
import ai.traceable.aiapp.protection.config.service.v1.AiAppConfigServiceGrpc;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleData;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleToDelete;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleType;
import ai.traceable.aiapp.protection.config.service.v1.AiAppOotbSubRuleUpdate;
import ai.traceable.aiapp.protection.config.service.v1.AiAppRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppRuleUpdate;
import ai.traceable.aiapp.protection.config.service.v1.CreateAiAppCustomRuleRequest;
import ai.traceable.aiapp.protection.config.service.v1.CreateAiAppCustomRuleResponse;
import ai.traceable.aiapp.protection.config.service.v1.DeleteAiAppRulesRequest;
import ai.traceable.aiapp.protection.config.service.v1.DeleteAiAppRulesResponse;
import ai.traceable.aiapp.protection.config.service.v1.GetAiAppRulesRequest;
import ai.traceable.aiapp.protection.config.service.v1.GetAiAppRulesResponse;
import ai.traceable.aiapp.protection.config.service.v1.ResetToDefault;
import ai.traceable.aiapp.protection.config.service.v1.RuleAction;
import ai.traceable.aiapp.protection.config.service.v1.RuleScope;
import ai.traceable.aiapp.protection.config.service.v1.RuleStatusChange;
import ai.traceable.aiapp.protection.config.service.v1.UpdateAiAppRulesRequest;
import ai.traceable.aiapp.protection.config.service.v1.UpdateAiAppRulesResponse;
import ai.traceable.aiapp.protection.config.service.v1.UpsertAiAppCustomRuleRequest;
import ai.traceable.aiapp.protection.config.service.v1.UpsertAiAppCustomRuleResponse;
import ai.traceable.aiapp.protection.config.service.validator.AiAppProtectionConfigServiceValidator;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfigType;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfigMap;
import ai.traceable.anomaly.config.service.v1.detector.DeleteAnomalyConfigOption;
import ai.traceable.anomaly.config.service.v1.detector.DeleteScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.DetectorConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.detector.GenAiAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.GetAllUnresolvedScopedAnomalyDetectionConfigsRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetAllUnresolvedScopedAnomalyDetectionConfigsResponse;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigResponse;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.UpdateScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyRuleInfosRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyRuleInfosResponse;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleResponse;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.DeleteCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleResponse;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleResponse;
import ai.traceable.ratelimiting.config.service.v2.DeleteRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesResponse;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import ai.traceable.ratelimiting.config.service.v2.UpdateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.UpdateRateLimitingRuleResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AiAppProtectionConfigServiceImpl
    extends AiAppConfigServiceGrpc.AiAppConfigServiceImplBase {

  private static final Logger log = LoggerFactory.getLogger(AiAppProtectionConfigServiceImpl.class);

  // Static enum sets to determine which service to route to
  private static final Set<AiAppCustomRuleType> RATE_LIMITING_RULE_TYPES =
      EnumSet.of(
          AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_PII_DETECTED_IN_PROMPT,
          AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_RATE_LIMITING);

  private static final Set<AiAppCustomRuleType> CUSTOM_SIGNATURE_RULE_TYPES =
      EnumSet.of(
          AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_MODEL_GOVERNANCE,
          AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_INPUT_EXPLOSION);

  private final AiAppProtectionConfigServiceValidator validator;
  private final AnomalyToAiAppRuleConverter anomalyToAiAppRuleConverter;
  private final AiAppToCustomSignatureConverter aiAppToCustomSignatureConverter;
  private final CustomSignatureToAiAppConverter customSignatureToAiAppConverter;
  private final RateLimitingToAiAppConverter rateLimitingToAiAppConverter;
  private final AiAppToRateLimitingConverter aiAppToRateLimitingConverter;
  private final AnomalyGlobalConfigServiceGrpc.AnomalyGlobalConfigServiceBlockingStub
      anomalyGlobalConfigService;
  private final DetectorConfigServiceGrpc.DetectorConfigServiceBlockingStub detectorConfigService;
  private final RateLimitingConfigServiceGrpc.RateLimitingConfigServiceBlockingStub
      rateLimitingConfigService;
  private final CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub
      customSignatureConfigService;

  @Inject
  public AiAppProtectionConfigServiceImpl(
      AiAppProtectionConfigServiceValidator validator,
      AnomalyToAiAppRuleConverter anomalyToAiAppRuleConverter,
      AiAppToCustomSignatureConverter aiAppToCustomSignatureConverter,
      CustomSignatureToAiAppConverter customSignatureToAiAppConverter,
      RateLimitingToAiAppConverter rateLimitingToAiAppConverter,
      AiAppToRateLimitingConverter aiAppToRateLimitingConverter,
      AnomalyGlobalConfigServiceGrpc.AnomalyGlobalConfigServiceBlockingStub
          anomalyGlobalConfigService,
      DetectorConfigServiceGrpc.DetectorConfigServiceBlockingStub detectorConfigService,
      RateLimitingConfigServiceGrpc.RateLimitingConfigServiceBlockingStub rateLimitingConfigService,
      CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub
          customSignatureConfigService) {
    this.validator = validator;
    this.anomalyToAiAppRuleConverter = anomalyToAiAppRuleConverter;
    this.aiAppToCustomSignatureConverter = aiAppToCustomSignatureConverter;
    this.customSignatureToAiAppConverter = customSignatureToAiAppConverter;
    this.rateLimitingToAiAppConverter = rateLimitingToAiAppConverter;
    this.aiAppToRateLimitingConverter = aiAppToRateLimitingConverter;
    this.anomalyGlobalConfigService = anomalyGlobalConfigService;
    this.detectorConfigService = detectorConfigService;
    this.rateLimitingConfigService = rateLimitingConfigService;
    this.customSignatureConfigService = customSignatureConfigService;
  }

  @Override
  public void getAiAppRules(
      GetAiAppRulesRequest request, StreamObserver<GetAiAppRulesResponse> responseObserver) {
    RequestContext context = RequestContext.CURRENT.get();
    try {
      log.debug("Getting AI app rules for request: {}", request);

      // Validate request
      Status validationStatus = validator.validateGetAiAppRulesRequest(request);
      if (!validationStatus.isOk()) {
        responseObserver.onError(validationStatus.asRuntimeException(context.buildTrailers()));
        return;
      }

      GetAiAppRulesResponse response =
          GetAiAppRulesResponse.newBuilder()
              .addAllAiAppRules(getAiAppRules(context, request.getRuleScope()))
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error getting AI app rules", e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Failed to get AI app rules: " + e.getMessage())
              .asRuntimeException(context.buildTrailers()));
    }
  }

  @Override
  public void deleteAiAppRules(
      DeleteAiAppRulesRequest request, StreamObserver<DeleteAiAppRulesResponse> responseObserver) {
    RequestContext context = RequestContext.CURRENT.get();
    try {
      log.debug("Deleting AI app rules for request: {}", request);

      // Validate request
      Status validationStatus = validator.validateDeleteAiAppRulesRequest(request);
      if (!validationStatus.isOk()) {
        responseObserver.onError(validationStatus.asRuntimeException(context.buildTrailers()));
        return;
      }

      // Handle different delete options
      if (request.hasResetToDefault()) {
        ResetToDefault resetToDefault = request.getResetToDefault();

        // Create AnomalyConfigScope from ResetToDefault
        AnomalyConfigScope configScope = createAnomalyConfigScope(resetToDefault.getRuleScope());

        // Reset rules to default by deleting scoped anomaly detection config
        resetRulesToDefault(context, configScope, resetToDefault.getRuleIdsList());

      } else if (request.hasCustomRuleToDelete()) {
        AiAppCustomRuleToDelete customRuleToDelete = request.getCustomRuleToDelete();
        String ruleId = customRuleToDelete.getRuleId();
        AiAppCustomRuleType ruleType = customRuleToDelete.getCustomRuleType();

        // Delete the custom rule from appropriate service based on rule type
        try {
          if (RATE_LIMITING_RULE_TYPES.contains(ruleType)) {
            // Delete from rate limiting service
            DeleteRateLimitingRuleRequest deleteRequest =
                DeleteRateLimitingRuleRequest.newBuilder().setRuleId(ruleId).build();

            context.call(() -> rateLimitingConfigService.deleteRateLimitingRule(deleteRequest));
          } else if (CUSTOM_SIGNATURE_RULE_TYPES.contains(ruleType)) {
            // Delete from custom signature service
            DeleteCustomSignatureRuleRequest deleteRequest =
                DeleteCustomSignatureRuleRequest.newBuilder().setId(ruleId).build();

            context.call(
                () -> customSignatureConfigService.deleteCustomSignatureRule(deleteRequest));
          }
        } catch (Exception e) {
          log.error("Failed to delete custom rule with ID: {}", ruleId, e);
          responseObserver.onError(
              Status.INTERNAL
                  .withDescription("Failed to delete custom rule: " + e.getMessage())
                  .asRuntimeException(context.buildTrailers()));
          return;
        }
      }

      DeleteAiAppRulesResponse response = DeleteAiAppRulesResponse.newBuilder().build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error deleting AI app rules", e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Failed to delete AI app rules: " + e.getMessage())
              .asRuntimeException(context.buildTrailers()));
    }
  }

  @Override
  public void updateAiAppRules(
      UpdateAiAppRulesRequest request, StreamObserver<UpdateAiAppRulesResponse> responseObserver) {
    RequestContext context = RequestContext.CURRENT.get();
    try {
      // Validate request
      Status validationStatus = validator.validateUpdateAiAppRulesRequest(request);
      if (!validationStatus.isOk()) {
        responseObserver.onError(validationStatus.asRuntimeException(context.buildTrailers()));
        return;
      }

      // Create anomaly config scope from AI app rule scope
      AnomalyConfigScope configScope = createAnomalyConfigScope(request.getRuleScope());

      // Build scoped anomaly detection config from rule updates
      ScopedAnomalyDetectionConfig scopedConfig =
          buildScopedAnomalyDetectionConfigFromUpdates(
              configScope, request.getAiAppRuleUpdatesList());

      // Update the scoped anomaly detection config
      UpdateScopedAnomalyDetectionConfigRequest updateRequest =
          UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
              .setScopedAnomalyDetectionConfig(scopedConfig)
              .build();

      context.call(() -> detectorConfigService.updateScopedAnomalyDetectionConfig(updateRequest));

      // Fetch updated rules to return in response
      List<AiAppRule> updatedRules = getAiAppRules(context, request.getRuleScope());

      UpdateAiAppRulesResponse response =
          UpdateAiAppRulesResponse.newBuilder().addAllAiAppRules(updatedRules).build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error updating AI app rules", e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Failed to update AI app rules: " + e.getMessage())
              .asRuntimeException(context.buildTrailers()));
    }
  }

  @Override
  public void createAiAppCustomRule(
      CreateAiAppCustomRuleRequest request,
      StreamObserver<CreateAiAppCustomRuleResponse> responseObserver) {
    RequestContext context = RequestContext.CURRENT.get();
    try {
      // Validate request
      Status validationStatus = validator.validateCreateAiAppCustomRuleRequest(request);
      if (!validationStatus.isOk()) {
        responseObserver.onError(validationStatus.asRuntimeException(context.buildTrailers()));
        return;
      }

      AiAppCustomRuleData ruleData = request.getAiAppCustomRuleData();

      // Determine rule type based on rule data content
      AiAppCustomRuleType ruleType = determineRuleType(ruleData);

      AiAppCustomRule aiAppCustomRule;

      if (RATE_LIMITING_RULE_TYPES.contains(ruleType)) {
        // Route to rate limiting service
        CreateRateLimitingRuleRequest createRequest =
            aiAppToRateLimitingConverter.convertToCreateRateLimitingRuleRequest(ruleData);

        CreateRateLimitingRuleResponse createResponse =
            context.call(() -> rateLimitingConfigService.createRateLimitingRule(createRequest));
        RateLimitingRule createdRule = createResponse.getRule();
        log.debug("Successfully created rate limiting rule with ID: {}", createdRule.getId());

        aiAppCustomRule = rateLimitingToAiAppConverter.convertFromRateLimitingRule(createdRule);
      } else if (CUSTOM_SIGNATURE_RULE_TYPES.contains(ruleType)) {
        // Route to custom signature service
        CreateCustomSignatureRuleRequest createRequest =
            aiAppToCustomSignatureConverter.convertToCreateCustomSignatureRuleRequest(ruleData);

        CreateCustomSignatureRuleResponse createResponse =
            context.call(
                () -> customSignatureConfigService.createCustomSignatureRule(createRequest));
        CustomSignatureRule createdRule = createResponse.getRule();
        log.debug("Successfully created custom signature rule with ID: {}", createdRule.getId());

        aiAppCustomRule =
            customSignatureToAiAppConverter.convertFromCustomSignatureRule(createdRule);
      } else {
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription("Unsupported rule type: " + ruleType)
                .asRuntimeException(context.buildTrailers()));
        return;
      }

      CreateAiAppCustomRuleResponse response =
          CreateAiAppCustomRuleResponse.newBuilder().setAiAppCustomRule(aiAppCustomRule).build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error creating AI app custom rule", e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Failed to create AI app custom rule: " + e.getMessage())
              .asRuntimeException(context.buildTrailers()));
    }
  }

  @Override
  public void upsertAiAppCustomRule(
      UpsertAiAppCustomRuleRequest request,
      StreamObserver<UpsertAiAppCustomRuleResponse> responseObserver) {
    RequestContext context = RequestContext.CURRENT.get();
    try {
      // Validate request
      Status validationStatus = validator.validateUpsertAiAppCustomRuleRequest(request);
      if (!validationStatus.isOk()) {
        responseObserver.onError(validationStatus.asRuntimeException(context.buildTrailers()));
        return;
      }

      AiAppCustomRule inputRule = request.getAiAppCustomRule();
      String ruleId = inputRule.getRuleId();

      // Determine rule type based on rule data content
      AiAppCustomRuleType ruleType = determineRuleType(inputRule.getRuleData());

      AiAppCustomRule resultRule;

      if (RATE_LIMITING_RULE_TYPES.contains(ruleType)) {
        UpdateRateLimitingRuleRequest updateRequest =
            UpdateRateLimitingRuleRequest.newBuilder()
                .setRuleId(ruleId)
                .setData(
                    aiAppToRateLimitingConverter.convertToRateLimitingRule(inputRule.getRuleData()))
                .build();

        UpdateRateLimitingRuleResponse updateResponse =
            context.call(() -> rateLimitingConfigService.updateRateLimitingRule(updateRequest));

        // Convert back to AI app custom rule for response
        resultRule =
            rateLimitingToAiAppConverter.convertFromRateLimitingRule(updateResponse.getRule());
      } else if (CUSTOM_SIGNATURE_RULE_TYPES.contains(ruleType)) {
        CustomSignatureRule customSignatureRule =
            aiAppToCustomSignatureConverter.convertToCustomSignatureRule(inputRule);

        UpdateCustomSignatureRuleRequest updateRequest =
            UpdateCustomSignatureRuleRequest.newBuilder().setRule(customSignatureRule).build();

        UpdateCustomSignatureRuleResponse updateResponse =
            context.call(
                () -> customSignatureConfigService.updateCustomSignatureRule(updateRequest));
        // Convert back to AI app custom rule for response
        resultRule =
            customSignatureToAiAppConverter.convertFromCustomSignatureRule(
                updateResponse.getRule());
      } else {
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription("Unsupported rule type: " + ruleType)
                .asRuntimeException(context.buildTrailers()));
        return;
      }

      UpsertAiAppCustomRuleResponse response =
          UpsertAiAppCustomRuleResponse.newBuilder().setAiAppCustomRule(resultRule).build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error upserting AI app custom rule", e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Failed to upsert AI app custom rule: " + e.getMessage())
              .asRuntimeException(context.buildTrailers()));
    }
  }

  /**
   * Retrieves ALL custom rules (both rate limiting and custom signature) and converts them to
   * AiAppCustomRule format.
   */
  private List<AiAppCustomRule> getAllCustomRulesAsList(
      RequestContext context, RuleScope ruleScope) {
    // Get all custom signature rules
    List<AiAppCustomRule> customSignatureRules = getAllCustomSignatureRules(context, ruleScope);
    List<AiAppCustomRule> allCustomRules = new ArrayList<>(customSignatureRules);
    log.debug("Added {} custom signature rules", customSignatureRules.size());

    // Get all rate limiting rules
    List<AiAppCustomRule> rateLimitingRules = getAllRateLimitingRules(context, ruleScope);
    allCustomRules.addAll(rateLimitingRules);
    log.debug("Added {} rate limiting rules", rateLimitingRules.size());
    return allCustomRules;
  }

  /** Retrieves all custom signature rules and converts them to AiAppCustomRule format. */
  private List<AiAppCustomRule> getAllCustomSignatureRules(
      RequestContext context, RuleScope ruleScope) {
    GetRulesFilter.Builder filterBuilder = GetRulesFilter.newBuilder();
    if (ruleScope.hasEnvironmentScope()) {
      filterBuilder.setRuleScope(
          ai.traceable.customsignature.config.service.v1.RuleScope.newBuilder()
              .setEnvironmentScope(
                  EnvironmentScope.newBuilder()
                      .addAllEnvironmentIds(
                          ruleScope.getEnvironmentScope().getEnvironmentIdsList())));
    }
    GetCustomSignatureRulesRequest request =
        GetCustomSignatureRulesRequest.newBuilder().setFilter(filterBuilder).build();
    GetCustomSignatureRulesResponse response =
        context.call(() -> customSignatureConfigService.getCustomSignatureRules(request));

    return response.getRulesList().stream()
        .filter(
            rule ->
                rule.getCategory()
                    .equals(
                        ai.traceable.customsignature.config.service.v1.Category
                            .CATEGORY_AI_APP_PROTECTION))
        .map(
            rule -> {
              try {
                return customSignatureToAiAppConverter.convertFromCustomSignatureRule(rule);
              } catch (Exception e) {
                log.error("Error converting custom signature rule: {}", rule.getId(), e);
                return null;
              }
            })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  /** Retrieves all rate limiting rules and converts them to AiAppCustomRule format. */
  private List<AiAppCustomRule> getAllRateLimitingRules(
      RequestContext context, RuleScope ruleScope) {
    GetRateLimitingRulesFilter.Builder filterBuilder =
        GetRateLimitingRulesFilter.newBuilder().addCategories(Category.CATEGORY_AI_APP_PROTECTION);

    if (ruleScope.hasEnvironmentScope()) {
      filterBuilder.setScope(
          RuleConfigScope.newBuilder()
              .setEnvironmentScope(
                  ai.traceable.ratelimiting.config.service.v2.EnvironmentScope.newBuilder()
                      .addAllEnvironmentIds(
                          ruleScope.getEnvironmentScope().getEnvironmentIdsList())));
    }

    GetRateLimitingRulesRequest request =
        GetRateLimitingRulesRequest.newBuilder().setRulesFilter(filterBuilder).build();
    GetRateLimitingRulesResponse response =
        context.call(() -> rateLimitingConfigService.getRateLimitingRules(request));

    return response.getRulesList().stream()
        .map(
            rule -> {
              try {
                return rateLimitingToAiAppConverter.convertFromRateLimitingRule(rule);
              } catch (Exception e) {
                log.error("Error converting rate limiting rule: {}", rule.getId(), e);
                return null;
              }
            })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  /** Creates AnomalyConfigScope from AI app RuleScope. */
  private AnomalyConfigScope createAnomalyConfigScope(
      ai.traceable.aiapp.protection.config.service.v1.RuleScope ruleScope) {
    AnomalyConfigScope.Builder builder = AnomalyConfigScope.newBuilder();

    if (ruleScope.hasTenantScope()) {
      // Tenant scope maps to customer scope
      builder.setCustomerScope(AnomalyCustomerScope.newBuilder().build());
    } else if (ruleScope.hasEnvironmentScope()) {
      // Environment scope maps to environment scope
      ai.traceable.aiapp.protection.config.service.v1.EnvironmentScope envScope =
          ruleScope.getEnvironmentScope();
      if (!envScope.getEnvironmentIdsList().isEmpty()) {
        // Use the first environment ID (anomaly service expects single environment)
        String environmentId = envScope.getEnvironmentIdsList().get(0);
        builder.setEnvironmentScope(
            AnomalyEnvironmentScope.newBuilder().setEnvironmentId(environmentId).build());
      }
    }

    return builder.build();
  }

  /** Fetches anomaly rule infos from AnomalyGlobalConfigService. */
  private List<AnomalyRuleInfo> fetchAnomalyRuleInfos(RequestContext context) {
    GetAnomalyRuleInfosRequest request =
        GetAnomalyRuleInfosRequest.newBuilder()
            .addEventFamilies(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_GEN_AI)
            .addEventFamilies(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC)
            .build();

    GetAnomalyRuleInfosResponse response =
        context.call(() -> anomalyGlobalConfigService.getAnomalyRuleInfos(request));

    log.debug("Successfully fetched {} anomaly rule infos", response.getRuleInfosList().size());
    return response.getRuleInfosList();
  }

  /** Fetches scoped anomaly detection config from DetectorConfigService. */
  private ScopedAnomalyDetectionConfig fetchScopedAnomalyDetectionConfig(
      RequestContext context, AnomalyConfigScope configScope) {
    GetScopedAnomalyDetectionConfigRequest request =
        GetScopedAnomalyDetectionConfigRequest.newBuilder()
            .setConfigScope(configScope)
            .setFilter(
                GetAnomalyDetectionConfigsFilter.newBuilder()
                    .addAnomalyDetectionConfigTypes(
                        AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_GEN_AI))
            .build();

    GetScopedAnomalyDetectionConfigResponse response =
        context.call(() -> detectorConfigService.getScopedAnomalyDetectionConfig(request));

    log.debug("Successfully fetched scoped anomaly detection config");
    return response.getScopedAnomalyDetectionConfig();
  }

  /** Fetches all unresolved scoped anomaly detection configs from DetectorConfigService. */
  private List<ScopedAnomalyDetectionConfig> fetchUnresolvedScopedAnomalyDetectionConfigs(
      RequestContext context) {
    GetAllUnresolvedScopedAnomalyDetectionConfigsRequest request =
        GetAllUnresolvedScopedAnomalyDetectionConfigsRequest.newBuilder()
            .setFilter(
                GetAnomalyDetectionConfigsFilter.newBuilder()
                    .addAnomalyDetectionConfigTypes(
                        AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_GEN_AI))
            .build();

    GetAllUnresolvedScopedAnomalyDetectionConfigsResponse response =
        context.call(
            () -> detectorConfigService.getAllUnresolvedScopedAnomalyDetectionConfigs(request));

    log.debug(
        "Successfully fetched {} unresolved scoped configs",
        response.getScopedAnomalyDetectionConfigsList().size());
    return response.getScopedAnomalyDetectionConfigsList();
  }

  /** Builds scoped anomaly detection config from rule updates. */
  private ScopedAnomalyDetectionConfig buildScopedAnomalyDetectionConfigFromUpdates(
      AnomalyConfigScope configScope, List<AiAppRuleUpdate> ruleUpdates) {

    log.debug("Building scoped anomaly detection config with {} rule updates", ruleUpdates.size());

    List<AnomalyDetectionConfig> detectionConfigs = new ArrayList<>();

    // Create GenAI anomaly detection configs from rule updates
    for (AiAppRuleUpdate ruleUpdate : ruleUpdates) {
      GenAiAnomalyDetectionConfig genAiConfig = createGenAiConfigFromRuleUpdate(ruleUpdate);

      AnomalyDetectionConfig.Builder detectionConfigBuilder =
          AnomalyDetectionConfig.newBuilder().setGenAiAnomalyDetectionConfig(genAiConfig);

      // Set AnomalyConfigStatusChange from RuleStatusChange for the main rule
      if (ruleUpdate.hasRuleStatusChange()) {
        AnomalyConfigStatusChange configStatusChange =
            convertRuleStatusChangeToAnomalyConfigStatusChange(ruleUpdate.getRuleStatusChange());
        detectionConfigBuilder.setConfigStatus(configStatusChange);
      }

      detectionConfigs.add(detectionConfigBuilder.build());
    }

    return ScopedAnomalyDetectionConfig.newBuilder()
        .setConfigScope(configScope)
        .addAllAnomalyDetectionConfigs(detectionConfigs)
        .build();
  }

  /** Creates GenAI config from rule update. */
  private GenAiAnomalyDetectionConfig createGenAiConfigFromRuleUpdate(AiAppRuleUpdate ruleUpdate) {
    GenAiAnomalyDetectionConfig.Builder configBuilder =
        GenAiAnomalyDetectionConfig.newBuilder().setAnomalyRuleId(ruleUpdate.getRuleId());

    // Create sub rule configs from ootb sub rule updates
    Map<String, AnomalySubRuleConfig> subRuleConfigMap = new HashMap<>();

    for (AiAppOotbSubRuleUpdate subRuleUpdate : ruleUpdate.getOotbSubRuleUpdatesList()) {
      String subRuleId = subRuleUpdate.getRuleId();

      AnomalySubRuleConfig.Builder subRuleConfigBuilder =
          AnomalySubRuleConfig.newBuilder().setSubRuleId(subRuleId);

      // Apply sub rule updates - only set rule action/internal for sub-rules
      if (subRuleUpdate.getRuleAction() != RuleAction.RULE_ACTION_UNSPECIFIED) {
        AnomalyRuleAction anomalyRuleAction =
            convertRuleActionToAnomalyRuleAction(subRuleUpdate.getRuleAction());
        subRuleConfigBuilder.setAnomalyRuleAction(anomalyRuleAction);
      }

      if (subRuleUpdate.hasInternal()) {
        // Set internal flag directly on sub rule config
        subRuleConfigBuilder.setInternal(subRuleUpdate.getInternal());
      }

      subRuleConfigMap.put(subRuleId, subRuleConfigBuilder.build());
    }

    // Set sub rule configs map
    if (!subRuleConfigMap.isEmpty()) {
      AnomalySubRuleConfigMap subRuleConfigMapProto =
          AnomalySubRuleConfigMap.newBuilder().putAllSubRuleConfigs(subRuleConfigMap).build();
      configBuilder.setSubRuleConfigs(subRuleConfigMapProto);
    }

    return configBuilder.build();
  }

  /** Converts AI app rule action to anomaly rule action. */
  private AnomalyRuleAction convertRuleActionToAnomalyRuleAction(RuleAction ruleAction) {
    switch (ruleAction) {
      case RULE_ACTION_DISABLE:
        return AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE;
      case RULE_ACTION_MONITOR:
        return AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR;
      case RULE_ACTION_BLOCK:
        return AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK;
      case RULE_ACTION_TESTING:
        return AnomalyRuleAction.ANOMALY_RULE_ACTION_TESTING;
      default:
        log.warn("Unknown rule action: {}, defaulting to monitor", ruleAction);
        return AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR;
    }
  }

  /** Converts AI app rule status change to anomaly config status change. */
  private AnomalyConfigStatusChange convertRuleStatusChangeToAnomalyConfigStatusChange(
      RuleStatusChange ruleStatusChange) {
    AnomalyConfigStatusChange.Builder statusBuilder = AnomalyConfigStatusChange.newBuilder();

    if (ruleStatusChange.hasDisabled()) {
      statusBuilder.setDisabled(ruleStatusChange.getDisabled());
    }

    if (ruleStatusChange.hasInternal()) {
      statusBuilder.setInternal(ruleStatusChange.getInternal());
    }

    return statusBuilder.build();
  }

  /** Fetches updated AI app rules after update operation. */
  private List<AiAppRule> getAiAppRules(RequestContext context, RuleScope ruleScope) {
    AnomalyConfigScope configScope = createAnomalyConfigScope(ruleScope);
    List<AnomalyRuleInfo> anomalyRuleInfos = fetchAnomalyRuleInfos(context);
    ScopedAnomalyDetectionConfig scopedConfig =
        fetchScopedAnomalyDetectionConfig(context, configScope);
    List<ScopedAnomalyDetectionConfig> unresolvedConfigs =
        fetchUnresolvedScopedAnomalyDetectionConfigs(context);
    List<AiAppCustomRule> customRules = getAllCustomRulesAsList(context, ruleScope);

    return anomalyToAiAppRuleConverter.convertToAiAppRules(
        context, configScope, scopedConfig, anomalyRuleInfos, customRules, unresolvedConfigs);
  }

  /** Resets rules to default by deleting scoped anomaly detection config. */
  private void resetRulesToDefault(
      RequestContext context, AnomalyConfigScope configScope, List<String> ruleIds) {
    // Create GenAiAnomalyDetectionConfig list based on rule IDs provided
    List<GenAiAnomalyDetectionConfig> genAiConfigs = new ArrayList<>();

    if (ruleIds.isEmpty()) {
      // If no rule IDs provided, create default instance to reset all GenAI rules
      genAiConfigs.add(GenAiAnomalyDetectionConfig.getDefaultInstance());
    } else {
      // Create GenAiAnomalyDetectionConfig for each rule ID
      for (String ruleId : ruleIds) {
        GenAiAnomalyDetectionConfig genAiConfig =
            GenAiAnomalyDetectionConfig.newBuilder().setAnomalyRuleId(ruleId).build();
        genAiConfigs.add(genAiConfig);
      }
    }

    // Create AnomalyDetectionConfig list with GenAI configs
    List<AnomalyDetectionConfig> anomalyDetectionConfigs = new ArrayList<>();
    for (GenAiAnomalyDetectionConfig genAiConfig : genAiConfigs) {
      AnomalyDetectionConfig detectionConfig =
          AnomalyDetectionConfig.newBuilder().setGenAiAnomalyDetectionConfig(genAiConfig).build();
      anomalyDetectionConfigs.add(detectionConfig);
    }

    // Create ScopedAnomalyDetectionConfig
    ScopedAnomalyDetectionConfig scopedConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(configScope)
            .addAllAnomalyDetectionConfigs(anomalyDetectionConfigs)
            .build();

    // Create DeleteScopedAnomalyDetectionConfigRequest
    DeleteScopedAnomalyDetectionConfigRequest deleteRequest =
        DeleteScopedAnomalyDetectionConfigRequest.newBuilder()
            .setDeleteAnomalyConfigOption(
                DeleteAnomalyConfigOption.DELETE_ANOMALY_CONFIG_OPTION_WHOLE_DETECTION_CONFIG)
            .setScopedAnomalyDetectionConfig(scopedConfig)
            .build();

    context.call(() -> detectorConfigService.deleteScopedAnomalyDetectionConfig(deleteRequest));
  }

  /** Helper method to determine the rule type based on the rule data content */
  private AiAppCustomRuleType determineRuleType(AiAppCustomRuleData ruleData) {
    if (ruleData.hasPiiDetectedInPromptRuleData()) {
      return AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_PII_DETECTED_IN_PROMPT;
    } else if (ruleData.hasAiRateLimitingRuleData()) {
      return AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_RATE_LIMITING;
    } else if (ruleData.hasModelGovernanceRuleData()) {
      return AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_MODEL_GOVERNANCE;
    } else if (ruleData.hasAiInputExplosionRuleData()) {
      return AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_INPUT_EXPLOSION;
    } else {
      return AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_UNSPECIFIED;
    }
  }
}
