package ai.traceable.customsignature.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.customsignature.config.service.migration.CustomSignatureRuleMigrationManager;
import ai.traceable.customsignature.config.service.modsec.ModsecRulesManager;
import ai.traceable.customsignature.config.service.rules.CustomSignatureRulesEdgeDecisionFilter;
import ai.traceable.customsignature.config.service.rules.RulesManager;
import ai.traceable.customsignature.config.service.rules.RulesValidator;
import ai.traceable.customsignature.config.service.rules.converter.CustomSignatureEdgeDecisionConverter;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleResponse;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.DeleteCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.DeleteCustomSignatureRuleResponse;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEdgeDecisionRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEdgeDecisionRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesResponse;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleResponse;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class CustomSignatureConfigServiceImpl
    extends CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceImplBase {

  private final RulesValidator rulesValidator;
  private final RulesManager rulesManager;
  private final ModsecRulesManager modsecRulesManager;
  private final CustomSignatureEdgeDecisionConverter edgeDecisionConverter;
  private final FeatureCachingClient featureCachingClient;
  private final CustomSignatureConfigServiceConfig customSignatureConfigServiceConfig;
  private final CustomSignatureRuleMigrationManager ruleMigrationManager;

  @Inject
  public CustomSignatureConfigServiceImpl(
      RulesValidator rulesValidator,
      RulesManager rulesManager,
      ModsecRulesManager modsecRulesManager,
      CustomSignatureEdgeDecisionConverter edgeDecisionConverter,
      FeatureCachingClient featureCachingClient,
      CustomSignatureConfigServiceConfig customSignatureConfigServiceConfig,
      CustomSignatureRuleMigrationManager ruleMigrationManager) {
    this.rulesValidator = rulesValidator;
    this.rulesManager = rulesManager;
    this.modsecRulesManager = modsecRulesManager;
    this.edgeDecisionConverter = edgeDecisionConverter;
    this.featureCachingClient = featureCachingClient;
    this.customSignatureConfigServiceConfig = customSignatureConfigServiceConfig;
    this.ruleMigrationManager = ruleMigrationManager;
  }

  @Override
  public void getCustomSignatureRules(
      GetCustomSignatureRulesRequest request,
      StreamObserver<GetCustomSignatureRulesResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      rulesValidator.validate(request);
      List<CustomSignatureRule> rules =
          rulesManager.getCustomSignatureRules(context, request.getFilter());
      List<CustomSignatureRule> rulesWithRuleEvaluationPointsSet =
          ruleMigrationManager.migrateRules(rules);
      responseObserver.onNext(
          GetCustomSignatureRulesResponse.newBuilder()
              .addAllRules(rulesWithRuleEvaluationPointsSet)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(
          Status.INTERNAL.withDescription("Unable to fetch custom signature rules").asException());
    }
  }

  @Override
  public void createCustomSignatureRule(
      CreateCustomSignatureRuleRequest request,
      StreamObserver<CreateCustomSignatureRuleResponse> responseObserver) {
    try {
      CreateCustomSignatureRuleRequest migratedCreateRuleRequest =
          ruleMigrationManager.migrateCreateCustomSignatureRuleRequest(request);

      Status status = rulesValidator.validate(migratedCreateRuleRequest);
      if (!status.isOk()) {
        log.error("Create Custom Signature Rule Request is not valid {}", status.getDescription());
        responseObserver.onError(status.asException());
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

      Status status = rulesValidator.validate(migratedUpdateRuleRequest);
      if (!status.isOk()) {
        log.error("Update Custom Signature Rule Request is not valid {}", status.getDescription());
        responseObserver.onError(status.asException());
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
      Status status = rulesValidator.validate(request);
      if (!status.isOk()) {
        log.error("Delete Custom Signature Rule Request is not valid {}", status.getDescription());
        responseObserver.onError(status.asException());
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
      rulesValidator.validate(request);
      List<CustomSignatureRule> rules =
          rulesManager.getCustomSignatureRules(context, request.getFilter());
      GetCustomSignatureModsecRulesResponse response =
          modsecRulesManager.getModsecRules(
              context,
              rules,
              request.getRuleVersion(),
              request.getIncludeAllPartialModsecRules(),
              request.getServiceNamesList(),
              request.getModsecCrsRulesTarget());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Unable to fetch modsec custom signature rules")
              .asException());
    }
  }

  @Override
  public void getCustomSignatureEdgeDecisionRules(
      GetCustomSignatureEdgeDecisionRulesRequest request,
      StreamObserver<GetCustomSignatureEdgeDecisionRulesResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      rulesValidator.validate(request);

      EdgeDecisionEngineConfig edgeDecisionEngineConfig =
          featureCachingClient.isEdgeDecisionEnabledForTenant(context)
                  && customSignatureConfigServiceConfig.isEdsConversionEnabled()
              ? edgeDecisionConverter.convert(
                  CustomSignatureRulesEdgeDecisionFilter.getConvertibleRules(
                      rulesManager.getCustomSignatureRules(context, request.getRulesFilter())))
              : EdgeDecisionEngineConfig.getDefaultInstance();
      GetCustomSignatureEdgeDecisionRulesResponse response =
          GetCustomSignatureEdgeDecisionRulesResponse.newBuilder()
              .setEdgeDecisionEngineConfig(edgeDecisionEngineConfig)
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Unable to fetch custom signature edge decision rules")
              .asException());
    }
  }
}
