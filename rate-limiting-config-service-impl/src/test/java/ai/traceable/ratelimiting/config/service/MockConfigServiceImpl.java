package ai.traceable.ratelimiting.config.service;

import static ai.traceable.ratelimiting.service.RateLimitingConfigConstants.RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME;
import static ai.traceable.ratelimiting.service.RateLimitingConfigConstants.RULE_RATE_LIMITED_ENTITY_ASSOCIATION_RESOURCE_NAME;

import com.google.protobuf.Value;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Triple;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.DeleteConfigResponse;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetAllConfigsResponse;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.GetConfigResponse;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;

public class MockConfigServiceImpl extends ConfigServiceGrpc.ConfigServiceImplBase {
  private Map<Triple<String, String, String>, Value> rateLimitingRuleConfigs = new HashMap<>();
  private Map<Triple<String, String, String>, Value> ruleEntityAssociations = new HashMap<>();

  @Override
  public void getAllConfigs(
      GetAllConfigsRequest request, StreamObserver<GetAllConfigsResponse> responseObserver) {
    try {
      GetAllConfigsResponse getAllConfigsResponse = null;
      if (request.getResourceName().equals(RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME)) {
        getAllConfigsResponse =
            GetAllConfigsResponse.newBuilder()
                .addAllContextSpecificConfigs(getContextSpecificRuleConfigs())
                .build();
      } else if (request
          .getResourceName()
          .equals(RULE_RATE_LIMITED_ENTITY_ASSOCIATION_RESOURCE_NAME)) {
        getAllConfigsResponse =
            GetAllConfigsResponse.newBuilder()
                .addAllContextSpecificConfigs(getContextSpecificAssociationConfigs())
                .build();
      }
      responseObserver.onNext(getAllConfigsResponse);
      responseObserver.onCompleted();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void upsertConfig(
      UpsertConfigRequest request, StreamObserver<UpsertConfigResponse> responseObserver) {
    try {
      Triple<String, String, String> resourceInfo =
          Triple.of(
              request.getResourceName(), request.getResourceNamespace(), request.getContext());

      if (request.getResourceName().equals(RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME)) {
        rateLimitingRuleConfigs.put(resourceInfo, request.getConfig());
      } else if (request
          .getResourceName()
          .equals(RULE_RATE_LIMITED_ENTITY_ASSOCIATION_RESOURCE_NAME)) {
        ruleEntityAssociations.put(resourceInfo, request.getConfig());
      }
      responseObserver.onNext(
          UpsertConfigResponse.newBuilder().setConfig(request.getConfig()).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void getConfig(
      GetConfigRequest request, StreamObserver<GetConfigResponse> responseObserver) {
    Triple<String, String, String> resourceInfo =
        Triple.of(
            request.getResourceName(), request.getResourceNamespace(), request.getContexts(0));
    Value configValue = null;

    if (request.getResourceName().equals(RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME)) {
      configValue = rateLimitingRuleConfigs.get(resourceInfo);
    } else if (request
        .getResourceName()
        .equals(RULE_RATE_LIMITED_ENTITY_ASSOCIATION_RESOURCE_NAME)) {
      configValue = ruleEntityAssociations.get(resourceInfo);
    }
    responseObserver.onNext(GetConfigResponse.newBuilder().setConfig(configValue).build());
    responseObserver.onCompleted();
  }

  @Override
  public void deleteConfig(
      DeleteConfigRequest request, StreamObserver<DeleteConfigResponse> responseObserver) {
    Triple<String, String, String> resourceInfo =
        Triple.of(request.getResourceName(), request.getResourceNamespace(), request.getContext());

    ContextSpecificConfig.Builder deletedConfigBuilder =
        ContextSpecificConfig.newBuilder().setContext(request.getContext());
    if (request.getResourceName().equals(RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME)) {
      deletedConfigBuilder.setConfig(rateLimitingRuleConfigs.remove(resourceInfo));
    } else if (request
        .getResourceName()
        .equals(RULE_RATE_LIMITED_ENTITY_ASSOCIATION_RESOURCE_NAME)) {
      deletedConfigBuilder.setConfig(ruleEntityAssociations.remove(resourceInfo));
    }
    responseObserver.onNext(
        DeleteConfigResponse.newBuilder().setDeletedConfig(deletedConfigBuilder.build()).build());
    responseObserver.onCompleted();
  }

  private List<ContextSpecificConfig> getContextSpecificRuleConfigs() {
    List<ContextSpecificConfig> contextSpecificConfigList = new ArrayList<>();
    for (Triple<String, String, String> key : rateLimitingRuleConfigs.keySet()) {
      Value configValue = rateLimitingRuleConfigs.get(key);
      ContextSpecificConfig contextSpecificConfig =
          ContextSpecificConfig.newBuilder()
              .setConfig(configValue)
              .setContext(key.getRight())
              .build();
      contextSpecificConfigList.add(contextSpecificConfig);
    }
    return contextSpecificConfigList;
  }

  private List<ContextSpecificConfig> getContextSpecificAssociationConfigs() {
    List<ContextSpecificConfig> contextSpecificConfigList = new ArrayList<>();
    for (Triple<String, String, String> key : ruleEntityAssociations.keySet()) {
      Value configValue = ruleEntityAssociations.get(key);
      ContextSpecificConfig contextSpecificConfig =
          ContextSpecificConfig.newBuilder()
              .setConfig(configValue)
              .setContext(key.getRight())
              .build();
      contextSpecificConfigList.add(contextSpecificConfig);
    }
    return contextSpecificConfigList;
  }

  public Map<Triple<String, String, String>, Value> getRateLimitingRuleConfigs() {
    return rateLimitingRuleConfigs;
  }

  public void setRateLimitingRuleConfigs(
      Map<Triple<String, String, String>, Value> rateLimitingRuleConfigs) {
    this.rateLimitingRuleConfigs = rateLimitingRuleConfigs;
  }

  public Map<Triple<String, String, String>, Value> getRuleEntityAssociations() {
    return ruleEntityAssociations;
  }

  public void setRuleEntityAssociations(
      Map<Triple<String, String, String>, Value> ruleEntityAssociations) {
    this.ruleEntityAssociations = ruleEntityAssociations;
  }
}
