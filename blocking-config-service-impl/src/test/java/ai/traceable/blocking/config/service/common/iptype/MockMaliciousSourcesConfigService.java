package ai.traceable.blocking.config.service.common.iptype;

import ai.traceable.malicioussources.config.service.v1.EventSeverity;
import ai.traceable.malicioussources.config.service.v1.GetMaliciousSourcesRulesRequest;
import ai.traceable.malicioussources.config.service.v1.GetMaliciousSourcesRulesResponse;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.malicioussources.config.service.v1.IpLocationTypeCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceImplBase;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleAction;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleInfo;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleScope;
import ai.traceable.malicioussources.config.service.v1.RuleActionType;
import com.google.common.collect.Lists;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class MockMaliciousSourcesConfigService extends MaliciousSourcesConfigServiceImplBase {
  private Server grpcServer;
  private final InProcessServerBuilder serverBuilder;
  private final ManagedChannel configChannel;
  private List<MaliciousSourcesRule> rules;

  MockMaliciousSourcesConfigService() {
    String uniqueName = InProcessServerBuilder.generateName();
    this.configChannel = InProcessChannelBuilder.forName(uniqueName).directExecutor().build();
    this.serverBuilder =
        InProcessServerBuilder.forName(uniqueName).directExecutor().addService(this);
  }

  void start() {
    rules = new ArrayList<>();
    try {
      this.grpcServer = serverBuilder.build().start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  Channel channel() {
    return this.configChannel;
  }

  void shutdown() {
    this.rules.clear();
    this.grpcServer.shutdownNow();
    this.configChannel.shutdownNow();
  }

  @Override
  public void getMaliciousSourcesRules(
      GetMaliciousSourcesRulesRequest request,
      StreamObserver<GetMaliciousSourcesRulesResponse> responseObserver) {
    responseObserver.onNext(
        GetMaliciousSourcesRulesResponse.newBuilder()
            .addAllRules(filteredRules(rules, request))
            .build());
    responseObserver.onCompleted();
  }

  void addRule(
      String id, List<IpLocationType> ipLocationTypeList, MaliciousSourcesRuleScope ruleScope) {
    MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
        MaliciousSourcesRuleInfo.newBuilder()
            .setName("rule-" + id)
            .setDescription("test rule-" + id)
            .addConditions(
                MaliciousSourcesRuleCondition.newBuilder()
                    .setIpLocationTypeCondition(
                        IpLocationTypeCondition.newBuilder()
                            .addAllIpLocationTypes(ipLocationTypeList)
                            .build()))
            .setRuleAction(
                MaliciousSourcesRuleAction.newBuilder()
                    .setActionType(RuleActionType.RULE_ACTION_TYPE_BLOCK)
                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_CRITICAL)
                    .build())
            .build();
    rules.add(
        MaliciousSourcesRule.newBuilder()
            .setId(id)
            .setRuleScope(ruleScope)
            .setRuleInfo(maliciousSourcesRuleInfo)
            .build());
  }

  private List<MaliciousSourcesRule> filteredRules(
      List<MaliciousSourcesRule> rules, GetMaliciousSourcesRulesRequest request) {
    if (request.hasFilter()
        && request.getFilter().hasRuleScope()
        && request.getFilter().getRuleScope().hasEnvironmentScope()) {
      List<String> environmentIds =
          request.getFilter().getRuleScope().getEnvironmentScope().getEnvironmentIdsList();
      return Lists.reverse(rules).stream()
          .filter(
              maliciousSourcesRule -> {
                List<String> ruleEnvironmentIds =
                    maliciousSourcesRule
                        .getRuleScope()
                        .getEnvironmentScope()
                        .getEnvironmentIdsList();
                return ruleEnvironmentIds.isEmpty()
                    || ruleEnvironmentIds.stream().anyMatch(environmentIds::contains);
              })
          .collect(Collectors.toUnmodifiableList());
    }
    return Lists.reverse(rules);
  }
}
