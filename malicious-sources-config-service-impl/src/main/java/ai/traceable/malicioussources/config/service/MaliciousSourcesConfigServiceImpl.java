package ai.traceable.malicioussources.config.service;

import ai.traceable.malicioussources.config.service.rules.RulesManager;
import ai.traceable.malicioussources.config.service.rules.RulesValidator;
import ai.traceable.malicioussources.config.service.rules.migration.MaliciousSourcesMigrationManager;
import ai.traceable.malicioussources.config.service.v1.BulkDeleteMaliciousSourcesRulesRequest;
import ai.traceable.malicioussources.config.service.v1.BulkDeleteMaliciousSourcesRulesResponse;
import ai.traceable.malicioussources.config.service.v1.CreateMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.CreateMaliciousSourcesRuleResponse;
import ai.traceable.malicioussources.config.service.v1.DeleteMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.DeleteMaliciousSourcesRuleResponse;
import ai.traceable.malicioussources.config.service.v1.GetMaliciousSourcesRulesRequest;
import ai.traceable.malicioussources.config.service.v1.GetMaliciousSourcesRulesResponse;
import ai.traceable.malicioussources.config.service.v1.GetRulesFilter;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceImplBase;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleRecord;
import ai.traceable.malicioussources.config.service.v1.UpdateMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.UpdateMaliciousSourcesRuleResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class MaliciousSourcesConfigServiceImpl extends MaliciousSourcesConfigServiceImplBase {
  private final RulesValidator rulesValidator;
  private final RulesManager rulesManager;
  private final MaliciousSourcesMigrationManager migrationManager;

  @Inject
  MaliciousSourcesConfigServiceImpl(
      RulesValidator rulesValidator,
      RulesManager rulesManager,
      MaliciousSourcesMigrationManager migrationManager) {
    this.rulesValidator = rulesValidator;
    this.rulesManager = rulesManager;
    this.migrationManager = migrationManager;
  }

  @Override
  public void getMaliciousSourcesRules(
      GetMaliciousSourcesRulesRequest request,
      StreamObserver<GetMaliciousSourcesRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      migrationManager.migrateFromChangeLog1IfApplicable(requestContext);
      List<MaliciousSourcesRuleRecord> ruleRecords =
          rulesManager.getMaliciousSourcesRuleRecords(requestContext, request.getFilter());
      List<MaliciousSourcesRule> rules =
          ruleRecords.stream()
              .map(MaliciousSourcesRuleRecord::getRule)
              .collect(Collectors.toList());

      responseObserver.onNext(
          GetMaliciousSourcesRulesResponse.newBuilder()
              .addAllRules(rules)
              .addAllRuleRecords(ruleRecords)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to fetch malicious sources rules", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createMaliciousSourcesRule(
      CreateMaliciousSourcesRuleRequest request,
      StreamObserver<CreateMaliciousSourcesRuleResponse> responseObserver) {
    try {
      List<MaliciousSourcesRule> existingRules =
          rulesManager.getMaliciousSourcesRules(
              RequestContext.CURRENT.get(), GetRulesFilter.getDefaultInstance());
      Status status = rulesValidator.validate(request, existingRules);
      if (!status.isOk()) {
        log.error("Create Malicious Sources Rule Request is not valid {}", status.getDescription());
        responseObserver.onError(status.asException());
        return;
      }
      MaliciousSourcesRule maliciousSourcesRule =
          rulesManager.createMaliciousSourcesRule(RequestContext.CURRENT.get(), request);

      responseObserver.onNext(
          CreateMaliciousSourcesRuleResponse.newBuilder().setRule(maliciousSourcesRule).build());
      responseObserver.onCompleted();

    } catch (Exception e) {
      log.error("Unable to create Malicious Sources Rule {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateMaliciousSourcesRule(
      UpdateMaliciousSourcesRuleRequest request,
      StreamObserver<UpdateMaliciousSourcesRuleResponse> responseObserver) {
    try {
      List<MaliciousSourcesRule> existingRules =
          rulesManager.getMaliciousSourcesRules(
              RequestContext.CURRENT.get(), GetRulesFilter.getDefaultInstance());
      Status status = rulesValidator.validate(request, existingRules);
      if (!status.isOk()) {
        log.error("Update Malicious Sources Rule Request is not valid {}", status.getDescription());
        responseObserver.onError(status.asException());
        return;
      }
      MaliciousSourcesRule updatedMaliciousSourcesRule =
          rulesManager.updateMaliciousSourcesRule(RequestContext.CURRENT.get(), request);
      responseObserver.onNext(
          UpdateMaliciousSourcesRuleResponse.newBuilder()
              .setRule(updatedMaliciousSourcesRule)
              .build());
      responseObserver.onCompleted();

    } catch (Exception e) {
      log.error("Unable to update Malicious Sources Rule {} :", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteMaliciousSourcesRule(
      DeleteMaliciousSourcesRuleRequest request,
      StreamObserver<DeleteMaliciousSourcesRuleResponse> responseObserver) {
    try {
      Status status = rulesValidator.validate(request);
      if (!status.isOk()) {
        log.error("Delete Malicious Source Rule Request is not valid {}", status.getDescription());
        responseObserver.onError(status.asException());
        return;
      }
      rulesManager.deleteMaliciousSourcesRule(RequestContext.CURRENT.get(), request.getId());
      responseObserver.onNext(DeleteMaliciousSourcesRuleResponse.newBuilder().build());
      responseObserver.onCompleted();

    } catch (Exception e) {
      log.error("Unable to delete malicious source rule with id {} :", request.getId(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void bulkDeleteMaliciousSourcesRules(
      BulkDeleteMaliciousSourcesRulesRequest request,
      StreamObserver<BulkDeleteMaliciousSourcesRulesResponse> responseObserver) {
    try {
      Status status = rulesValidator.validate(request);
      if (!status.isOk()) {
        log.error(
            "Bulk Delete Malicious Sources Rules Request is not valid {}", status.getDescription());
        responseObserver.onError(status.asException());
        return;
      }
      rulesManager.bulkDeleteMaliciousSourcesRules(
          RequestContext.CURRENT.get(), request.getIdsList());
      responseObserver.onNext(BulkDeleteMaliciousSourcesRulesResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Unable to bulk delete malicious sources rules with ids {} :", request.getIdsList(), e);
      responseObserver.onError(e);
    }
  }
}
