package ai.traceable.genai.system.discovery.config.service.v1;

import ai.traceable.genai.system.discovery.config.service.v1.GenAiSystemDiscoveryConfigServiceGrpc.GenAiSystemDiscoveryConfigServiceImplBase;
import ai.traceable.genai.system.discovery.config.service.v1.manager.GenAiSystemDiscoveryRuleManager;
import ai.traceable.genai.system.discovery.config.service.v1.validation.GenAiSystemDiscoveryRulesValidator;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class GenAiSystemDiscoveryConfigServiceImpl
    extends GenAiSystemDiscoveryConfigServiceImplBase {
  private final GenAiSystemDiscoveryRulesValidator rulesValidator;
  private final GenAiSystemDiscoveryRuleManager ruleManager;

  @Override
  public void getGenAiSystemDiscoveryRules(
      GetGenAiSystemDiscoveryRulesRequest request,
      StreamObserver<GetGenAiSystemDiscoveryRulesResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      rulesValidator.validateOrThrow(context, request);
      List<GenAiSystemDiscoveryRule> genAiSystemDiscoveryRules =
          ruleManager.getGenAiSystemDiscoveryRules(context, request.getFilter());
      GetGenAiSystemDiscoveryRulesResponse response =
          GetGenAiSystemDiscoveryRulesResponse.newBuilder()
              .addAllGenAiSystemDiscoveryRules(genAiSystemDiscoveryRules)
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void createGenAiSystemDiscoveryRule(
      CreateGenAiSystemDiscoveryRuleRequest request,
      StreamObserver<CreateGenAiSystemDiscoveryRuleResponse> responseObserver) {

    try {
      RequestContext context = RequestContext.CURRENT.get();
      rulesValidator.validateOrThrow(context, request);
      GenAiSystemDiscoveryRule genAiSystemDiscoveryRule =
          ruleManager.createGenAiSystemDiscoveryRule(context, request);
      CreateGenAiSystemDiscoveryRuleResponse response =
          CreateGenAiSystemDiscoveryRuleResponse.newBuilder()
              .setGenAiSystemDiscoveryRule(genAiSystemDiscoveryRule)
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateGenAiSystemDiscoveryRule(
      UpdateGenAiSystemDiscoveryRuleRequest request,
      StreamObserver<UpdateGenAiSystemDiscoveryRuleResponse> responseObserver) {

    try {
      RequestContext context = RequestContext.CURRENT.get();
      rulesValidator.validateOrThrow(context, request);
      GenAiSystemDiscoveryRule genAiSystemDiscoveryRule =
          ruleManager.updateGenAiSystemDiscoveryRule(context, request);
      UpdateGenAiSystemDiscoveryRuleResponse response =
          UpdateGenAiSystemDiscoveryRuleResponse.newBuilder()
              .setGenAiSystemDiscoveryRule(genAiSystemDiscoveryRule)
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteGenAiSystemDiscoveryRule(
      DeleteGenAiSystemDiscoveryRuleRequest request,
      StreamObserver<DeleteGenAiSystemDiscoveryRuleResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      rulesValidator.validateOrThrow(context, request);
      ruleManager.deleteGenAiSystemDiscoveryRule(context, request.getRuleId());
      responseObserver.onNext(DeleteGenAiSystemDiscoveryRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }
}
