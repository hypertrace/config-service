package ai.traceable.customsignature.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.customsignature.config.service.migration.CustomSignatureRuleMigrationManager;
import ai.traceable.customsignature.config.service.modsec.ModsecRulesManager;
import ai.traceable.customsignature.config.service.rules.CustomSignatureRulesEdgeDecisionFilter;
import ai.traceable.customsignature.config.service.rules.RulesManager;
import ai.traceable.customsignature.config.service.rules.RulesValidator;
import ai.traceable.customsignature.config.service.rules.converter.CustomSignatureEdgeDecisionConverter;
import ai.traceable.customsignature.config.service.v1.BulkDeleteCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.BulkDeleteCustomSignatureRulesResponse;
import ai.traceable.customsignature.config.service.v1.BulkUpdateCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.BulkUpdateCustomSignatureRulesResponse;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleResponse;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRuleRecord;
import ai.traceable.customsignature.config.service.v1.DeleteCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.DeleteCustomSignatureRuleResponse;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEdgeDecisionRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEdgeDecisionRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEvaluationConfigContextRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEvaluationConfigContextResponse;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesResponse;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleResponse;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureConfigContext;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class CustomSignatureConfigServiceImpl
    extends CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceImplBase {

  private final RulesValidator rulesValidator;
  private final RulesManager rulesManager;
  private final ModsecRulesManager modsecRulesManager;
  private final CustomSignatureEdgeDecisionConverter edgeDecisionConverter;
  private final CustomSignatureRuleMigrationManager ruleMigrationManager;
  private final FeatureCachingClient featureCachingClient;

  @Inject
  public CustomSignatureConfigServiceImpl(
      RulesValidator rulesValidator,
      RulesManager rulesManager,
      ModsecRulesManager modsecRulesManager,
      CustomSignatureEdgeDecisionConverter edgeDecisionConverter,
      CustomSignatureRuleMigrationManager ruleMigrationManager,
      FeatureCachingClient featureCachingClient) {
    this.rulesValidator = rulesValidator;
    this.rulesManager = rulesManager;
    this.modsecRulesManager = modsecRulesManager;
    this.edgeDecisionConverter = edgeDecisionConverter;
    this.ruleMigrationManager = ruleMigrationManager;
    this.featureCachingClient = featureCachingClient;
  }

  private <T> boolean isInvalidRequest(
      T request,
      StreamObserver<?> responseObserver,
      String requestType,
      ValidationFunction<T> validationFunction) {
    try {
      validationFunction.validate(request);
      return false;
    } catch (StatusRuntimeException e) {
      log.error(
          "{} Request is not valid: {}", requestType, Status.fromThrowable(e).getDescription());
      responseObserver.onError(e);
      return true;
    }
  }

  @FunctionalInterface
  private interface ValidationFunction<T> {
    void validate(T request);
  }

  @Override
  public void getCustomSignatureEvaluationConfigContext(
      GetCustomSignatureEvaluationConfigContextRequest request,
      StreamObserver<GetCustomSignatureEvaluationConfigContextResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      rulesValidator.validate(request);

      if (!featureCachingClient.isProtectionEngineCustomSignatureEnabledForTenant(requestContext)) {
        responseObserver.onNext(
            GetCustomSignatureEvaluationConfigContextResponse.getDefaultInstance());
        responseObserver.onCompleted();
        return;
      }

      CustomSignatureConfigContext configContext =
          rulesManager.getCustomSignatureEvaluationConfigContext(requestContext, request);
      GetCustomSignatureEvaluationConfigContextResponse response =
          GetCustomSignatureEvaluationConfigContextResponse.newBuilder()
              .setCustomSignatureEvaluationConfigContext(configContext.toByteString())
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed during fetching Custom Signature Evaluation Config Context for request: {}",
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void bulkDeleteCustomSignatureRules(
      BulkDeleteCustomSignatureRulesRequest request,
      StreamObserver<BulkDeleteCustomSignatureRulesResponse> responseObserver) {
    try {
      if (isInvalidRequest(
          request,
          responseObserver,
          "Bulk Delete Custom Signature Rules",
          rulesValidator::validate)) {
        return;
      }
      rulesManager.bulkDeleteCustomSignatureRules(
          RequestContext.CURRENT.get(), request.getIdsList());
      responseObserver.onNext(BulkDeleteCustomSignatureRulesResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to bulk delete custom signature rules {}", request.getIdsList(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getCustomSignatureRules(
      GetCustomSignatureRulesRequest request,
      StreamObserver<GetCustomSignatureRulesResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      if (isInvalidRequest(
          request, responseObserver, "Get Custom Signature Rules", rulesValidator::validate)) {
        return;
      }

      ruleMigrationManager.migrateCustomSignatureRules(context);

      List<CustomSignatureRuleRecord> ruleRecords =
          rulesManager.getCustomSignatureRuleRecords(context, request.getFilter());

      responseObserver.onNext(
          GetCustomSignatureRulesResponse.newBuilder()
              .addAllRules(
                  ruleRecords.stream()
                      .map(CustomSignatureRuleRecord::getRule)
                      .collect(Collectors.toList()))
              .addAllRuleRecords(ruleRecords)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to fetch custom signature rules", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createCustomSignatureRule(
      CreateCustomSignatureRuleRequest request,
      StreamObserver<CreateCustomSignatureRuleResponse> responseObserver) {
    try {
      CreateCustomSignatureRuleRequest migratedCreateRuleRequest =
          ruleMigrationManager.migrateCreateCustomSignatureRuleRequest(request);

      if (isInvalidRequest(
          migratedCreateRuleRequest,
          responseObserver,
          "Create Custom Signature Rule",
          rulesValidator::validate)) {
        return;
      }

      Optional<CustomSignatureRule> customSignatureRuleOptional =
          rulesManager.createCustomSignatureRule(
              RequestContext.CURRENT.get(), migratedCreateRuleRequest);
      if (customSignatureRuleOptional.isEmpty()) {
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(
                    String.format(
                        "Unable to create custom signature rule %s",
                        migratedCreateRuleRequest.getName()))
                .asException());
        return;
      }
      responseObserver.onNext(
          CreateCustomSignatureRuleResponse.newBuilder()
              .setRule(customSignatureRuleOptional.get())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to create custom signature rule", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateCustomSignatureRule(
      UpdateCustomSignatureRuleRequest request,
      StreamObserver<UpdateCustomSignatureRuleResponse> responseObserver) {
    try {
      UpdateCustomSignatureRuleRequest migratedUpdateRuleRequest =
          ruleMigrationManager.migrateUpdateCustomSignatureRuleRequest(request);

      if (isInvalidRequest(
          migratedUpdateRuleRequest,
          responseObserver,
          "Update Custom Signature Rule",
          rulesValidator::validate)) {
        return;
      }

      Optional<CustomSignatureRule> customSignatureRuleOptional =
          rulesManager.updateCustomSignatureRule(
              RequestContext.CURRENT.get(), migratedUpdateRuleRequest.getRule());
      if (customSignatureRuleOptional.isEmpty()) {
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(
                    String.format(
                        "Unable to update custom signature rule %s",
                        migratedUpdateRuleRequest.getRule().getId()))
                .asException());
        return;
      }
      responseObserver.onNext(
          UpdateCustomSignatureRuleResponse.newBuilder()
              .setRule(customSignatureRuleOptional.get())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Unable to update custom signature rule with id {} :", request.getRule().getId(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteCustomSignatureRule(
      DeleteCustomSignatureRuleRequest request,
      StreamObserver<DeleteCustomSignatureRuleResponse> responseObserver) {
    try {
      if (isInvalidRequest(
          request, responseObserver, "Delete Custom Signature Rule", rulesValidator::validate)) {
        return;
      }
      String ruleId = request.getId();
      rulesManager.deleteCustomSignatureRule(RequestContext.CURRENT.get(), ruleId);
      responseObserver.onNext(DeleteCustomSignatureRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to delete custom signature rule with id {} :", request.getId(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getCustomSignatureModsecRules(
      GetCustomSignatureModsecRulesRequest request,
      StreamObserver<GetCustomSignatureModsecRulesResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      if (isInvalidRequest(
          request,
          responseObserver,
          "Get Custom Signature Modsec Rules",
          rulesValidator::validate)) {
        return;
      }
      List<CustomSignatureRule> rules =
          rulesManager.getCustomSignatureRules(context, request.getFilter());
      GetCustomSignatureModsecRulesResponse response =
          modsecRulesManager.getModsecRules(
              context,
              rules,
              request.getRuleVersion(),
              request.getIncludeAllPartialModsecRules(),
              request.getModsecCrsRulesTarget(),
              request.getServiceNamesList());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to fetch modsec custom signature rules", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getCustomSignatureEdgeDecisionRules(
      GetCustomSignatureEdgeDecisionRulesRequest request,
      StreamObserver<GetCustomSignatureEdgeDecisionRulesResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      if (isInvalidRequest(
          request,
          responseObserver,
          "Get Custom Signature Edge Decision Rules",
          rulesValidator::validate)) {
        return;
      }

      if (featureCachingClient.isProtectionEngineCustomSignatureEnabledForTenant(context)) {
        responseObserver.onNext(GetCustomSignatureEdgeDecisionRulesResponse.getDefaultInstance());
        responseObserver.onCompleted();
        return;
      }

      EdgeDecisionEngineConfig edgeDecisionEngineConfig =
          edgeDecisionConverter.convert(
              CustomSignatureRulesEdgeDecisionFilter.getConvertibleRules(
                  rulesManager.getCustomSignatureRules(context, request.getRulesFilter())));

      GetCustomSignatureEdgeDecisionRulesResponse response =
          GetCustomSignatureEdgeDecisionRulesResponse.newBuilder()
              .setEdgeDecisionEngineConfig(edgeDecisionEngineConfig)
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to fetch custom signature edge decision rules", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void bulkUpdateCustomSignatureRules(
      BulkUpdateCustomSignatureRulesRequest request,
      StreamObserver<BulkUpdateCustomSignatureRulesResponse> responseObserver) {
    try {
      if (isInvalidRequest(
          request,
          responseObserver,
          "Bulk Update Custom Signature Rules",
          rulesValidator::validate)) {
        return;
      }
      rulesManager.bulkUpdateCustomSignatureRules(RequestContext.CURRENT.get(), request);
      responseObserver.onNext(BulkUpdateCustomSignatureRulesResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Unable to bulk update custom signature rules with ids {} :", request.getIdsList(), e);
      responseObserver.onError(e);
    }
  }
}
