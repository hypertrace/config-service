package ai.traceable.edge.config.service;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.supplier.CaptchaSiteKeyConfigSupplier;
import ai.traceable.edge.config.service.supplier.ClientBotFingerprintPolicySupplier;
import ai.traceable.edge.config.service.supplier.EdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.config.service.supplier.FlowConfigSupplier;
import ai.traceable.edge.config.service.supplier.InvisibleCaptchaPolicySupplier;
import ai.traceable.edge.config.service.supplier.WebAppEvaluationConfigContextSupplier;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.edge.config.service.v1.GetConfigsRequest;
import ai.traceable.edge.config.service.v1.GetConfigsResponse;
import ai.traceable.edge.config.service.v1.TraceableEdgeConfigServiceGrpc;
import ai.traceable.edge.config.service.validation.RequestValidator;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class TraceableEdgeConfigService
    extends TraceableEdgeConfigServiceGrpc.TraceableEdgeConfigServiceImplBase {
  private static final String TENANT_ID_KEY = "tenant-id";
  private final UuidGenerator uuidGenerator;
  private final Map<String, TraceableEdgeConfigSupplier> configSuppliersByType;

  @Inject
  public TraceableEdgeConfigService(
      UuidGenerator uuidGenerator,
      EdgeDecisionEngineConfigSupplier edgeDecisionEngineConfigSupplier,
      CaptchaSiteKeyConfigSupplier captchaSiteKeyConfigSupplier,
      ClientBotFingerprintPolicySupplier clientBotFingerprintPolicySupplier,
      InvisibleCaptchaPolicySupplier invisibleCaptchaPolicySupplier,
      FlowConfigSupplier flowConfigSupplier,
      WebAppEvaluationConfigContextSupplier webAppEvaluationConfigContextSupplier) {
    this.uuidGenerator = uuidGenerator;
    this.configSuppliersByType = new HashMap<>();
    this.configSuppliersByType.put(
        edgeDecisionEngineConfigSupplier.getConfigType(), edgeDecisionEngineConfigSupplier);
    this.configSuppliersByType.put(
        captchaSiteKeyConfigSupplier.getConfigType(), captchaSiteKeyConfigSupplier);
    this.configSuppliersByType.put(
        clientBotFingerprintPolicySupplier.getConfigType(), clientBotFingerprintPolicySupplier);
    this.configSuppliersByType.put(flowConfigSupplier.getConfigType(), flowConfigSupplier);
    this.configSuppliersByType.put(
        invisibleCaptchaPolicySupplier.getConfigType(), invisibleCaptchaPolicySupplier);
    this.configSuppliersByType.put(
        webAppEvaluationConfigContextSupplier.getConfigType(),
        webAppEvaluationConfigContextSupplier);
    // todo: use configSupplier to automatically instantiate the appropriate class.
    //    var configTypeSupplierConfigs = config.getConfigList(CONFIG_TYPES_CONFIG_NAME);
    //    for (var configTypeSupplierConfig : configTypeSupplierConfigs) {
    //      var configType = configTypeSupplierConfig.getString("configType");
    //      var configSupplier = configTypeSupplierConfig.getString("configSupplier");
    //      var configPollingFrequency = defaultPollingFrequency;
    //      if (configTypeSupplierConfig.hasPath(AGENT_POLLING_FREQUENCY_CONFIG_NAME)) {
    //        configPollingFrequency =
    // configTypeSupplierConfig.getDuration(AGENT_POLLING_FREQUENCY_CONFIG_NAME);
    //      }
    //      var configPollingDuration = Duration.newBuilder()
    //              .setSeconds(configPollingFrequency.getSeconds())
    //              .setNanos(configPollingFrequency.getNano())
    //              .build();
    //    }
    //    this.configSuppliersByType.put("EdgeDecisionEngineConfig", new
    // EdgeDecisionEngineConfigSupplier());
    //    this.configSuppliersByType.put("CaptchaSiteKeyConfig", new
    // CaptchaSiteKeyConfigSupplier());
    //    this.configSuppliersByType.put("ClientBotFingerprintPolicy", new
    // ClientBotFingerprintPolicySupplier());
  }

  // generic fetcher for config types
  @Override
  public void getConfigs(
      GetConfigsRequest request, StreamObserver<GetConfigsResponse> responseObserver) {
    var requestElements = request.getConfigRequestsList();
    GetConfigsResponse.Builder responseBuilder = GetConfigsResponse.newBuilder();
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      RequestValidator.validateRequestContext(requestContext);
      responseBuilder.putAllConfigMetadata(
          Map.of(TENANT_ID_KEY, requestContext.getTenantId().get()));
      List<ConfigResponseElement> responseElements = new ArrayList<>();
      for (var requestElement : requestElements) {
        var configType = requestElement.getConfigType();
        var configSupplier = this.configSuppliersByType.get(configType);
        if (configSupplier == null) {
          log.warn("No supplier for configType={}", configType);
          continue;
        }
        var agentCapabilities = requestElement.getAgentCapabilities();
        var responseElement =
            configSupplier.getConfigs(requestContext, requestElement, agentCapabilities);
        responseElements.add(responseElement);
      }
      String hash =
          uuidGenerator.generateId(
              responseElements.stream()
                  .map(ConfigResponseElement::getHash)
                  .collect(Collectors.toList()));
      responseBuilder.setHash(hash);
      if (!request.getPreviousHash().equals(hash)) {
        responseBuilder.addAllConfigResponses(responseElements);
      }
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (RuntimeException e) {
      log.error("Get Configs RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
