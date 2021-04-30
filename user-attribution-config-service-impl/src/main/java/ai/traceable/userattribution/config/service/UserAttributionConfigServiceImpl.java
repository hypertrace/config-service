package ai.traceable.userattribution.config.service;

import ai.traceable.userattribution.config.service.store.UserAttributionRuleGenerator;
import ai.traceable.userattribution.config.service.store.UserAttributionRuleStore;
import ai.traceable.userattribution.config.service.v1.CreateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.CreateUserAttributionRuleResponse;
import ai.traceable.userattribution.config.service.v1.DeleteUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.DeleteUserAttributionRuleResponse;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesResponse;
import ai.traceable.userattribution.config.service.v1.UpdateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.UpdateUserAttributionRuleResponse;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc.UserAttributionConfigServiceImplBase;
import ai.traceable.userattribution.config.service.validation.UserAttributionConfigRequestValidator;
import io.grpc.stub.StreamObserver;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class UserAttributionConfigServiceImpl extends UserAttributionConfigServiceImplBase {

  private final UserAttributionConfigRequestValidator validator;
  private final UserAttributionRuleStore ruleStore;
  private final UserAttributionRuleGenerator ruleGenerator;

  @Inject
  UserAttributionConfigServiceImpl(
      UserAttributionConfigRequestValidator validator,
      UserAttributionRuleStore ruleStore,
      UserAttributionRuleGenerator ruleGenerator) {
    this.validator = validator;
    this.ruleStore = ruleStore;
    this.ruleGenerator = ruleGenerator;
  }

  @Override
  public void getUserAttributionRules(
      GetUserAttributionRulesRequest request,
      StreamObserver<GetUserAttributionRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          GetUserAttributionRulesResponse.newBuilder()
              .addAllRules(this.ruleStore.getRules(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error retrieving user attribution rules", exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void createUserAttributionRule(
      CreateUserAttributionRuleRequest request,
      StreamObserver<CreateUserAttributionRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          CreateUserAttributionRuleResponse.newBuilder()
              .setRule(
                  this.ruleStore.upsertRule(
                      requestContext, this.ruleGenerator.generateNewRule(request)))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error creating user attribution rule {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateUserAttributionRule(
      UpdateUserAttributionRuleRequest request,
      StreamObserver<UpdateUserAttributionRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          UpdateUserAttributionRuleResponse.newBuilder()
              .setRule(this.ruleStore.upsertRule(requestContext, request.getRule()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating user attribution rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteUserAttributionRule(
      DeleteUserAttributionRuleRequest request,
      StreamObserver<DeleteUserAttributionRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);
      this.ruleStore.deleteRule(requestContext, request.getRuleId());
      responseObserver.onNext(DeleteUserAttributionRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting user attribution rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }
}
