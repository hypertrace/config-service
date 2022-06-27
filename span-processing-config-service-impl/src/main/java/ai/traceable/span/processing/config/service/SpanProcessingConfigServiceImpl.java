package ai.traceable.span.processing.config.service;

import ai.traceable.span.processing.config.service.store.ProtectionSpanRulesConfigStore;
import ai.traceable.span.processing.config.service.store.SamplingConfigsConfigStore;
import ai.traceable.span.processing.config.service.utils.TimestampConverter;
import ai.traceable.span.processing.config.service.v1.CreateProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.CreateProtectionSpanRuleResponse;
import ai.traceable.span.processing.config.service.v1.CreateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.CreateSamplingConfigResponse;
import ai.traceable.span.processing.config.service.v1.DeleteProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteProtectionSpanRuleResponse;
import ai.traceable.span.processing.config.service.v1.DeleteSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSamplingConfigResponse;
import ai.traceable.span.processing.config.service.v1.GetAllProtectionSpanRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetAllProtectionSpanRulesResponse;
import ai.traceable.span.processing.config.service.v1.GetAllSamplingConfigsRequest;
import ai.traceable.span.processing.config.service.v1.GetAllSamplingConfigsResponse;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRule;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRuleDetails;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRuleInfo;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRuleMetadata;
import ai.traceable.span.processing.config.service.v1.SamplingConfig;
import ai.traceable.span.processing.config.service.v1.SamplingConfigDetails;
import ai.traceable.span.processing.config.service.v1.SamplingConfigInfo;
import ai.traceable.span.processing.config.service.v1.SamplingConfigMetadata;
import ai.traceable.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import ai.traceable.span.processing.config.service.v1.UpdateProtectionSpanRule;
import ai.traceable.span.processing.config.service.v1.UpdateProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.UpdateProtectionSpanRuleResponse;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfig;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfigResponse;
import ai.traceable.span.processing.config.service.validation.SpanProcessingConfigRequestValidator;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class SpanProcessingConfigServiceImpl
    extends SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceImplBase {

  private final SpanProcessingConfigRequestValidator validator;
  private final SamplingConfigsConfigStore samplingConfigsConfigStore;
  private final ProtectionSpanRulesConfigStore protectionSpanRulesConfigStore;
  private final TimestampConverter timestampConverter;

  @Inject
  SpanProcessingConfigServiceImpl(
      SamplingConfigsConfigStore samplingConfigsConfigStore,
      ProtectionSpanRulesConfigStore protectionSpanRulesConfigStore,
      SpanProcessingConfigRequestValidator requestValidator,
      TimestampConverter timestampConverter) {
    this.validator = requestValidator;
    this.protectionSpanRulesConfigStore = protectionSpanRulesConfigStore;
    this.samplingConfigsConfigStore = samplingConfigsConfigStore;
    this.timestampConverter = timestampConverter;
  }

  @Override
  public void getAllProtectionSpanRules(
      GetAllProtectionSpanRulesRequest request,
      StreamObserver<GetAllProtectionSpanRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          GetAllProtectionSpanRulesResponse.newBuilder()
              .addAllRuleDetails(this.protectionSpanRulesConfigStore.getAllData(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get all protection span rules for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createProtectionSpanRule(
      CreateProtectionSpanRuleRequest request,
      StreamObserver<CreateProtectionSpanRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      // TODO: need to handle priorities
      ProtectionSpanRule newRule =
          ProtectionSpanRule.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setRuleInfo(request.getRuleInfo())
              .build();

      responseObserver.onNext(
          CreateProtectionSpanRuleResponse.newBuilder()
              .setRuleDetails(
                  buildProtectionSpanRuleDetails(
                      this.protectionSpanRulesConfigStore.upsertObject(requestContext, newRule)))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error creating protection span rule {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateProtectionSpanRule(
      UpdateProtectionSpanRuleRequest request,
      StreamObserver<UpdateProtectionSpanRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      UpdateProtectionSpanRule updateProtectionSpanRule = request.getRule();
      ProtectionSpanRule existingRule =
          this.protectionSpanRulesConfigStore
              .getData(requestContext, updateProtectionSpanRule.getId())
              .orElseThrow(Status.NOT_FOUND::asException);
      ProtectionSpanRule updatedRule = buildUpdatedRule(existingRule, updateProtectionSpanRule);

      responseObserver.onNext(
          UpdateProtectionSpanRuleResponse.newBuilder()
              .setRuleDetails(
                  buildProtectionSpanRuleDetails(
                      this.protectionSpanRulesConfigStore.upsertObject(
                          requestContext, updatedRule)))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating protection span rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteProtectionSpanRule(
      DeleteProtectionSpanRuleRequest request,
      StreamObserver<DeleteProtectionSpanRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      // TODO: need to handle priorities
      this.protectionSpanRulesConfigStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);

      responseObserver.onNext(DeleteProtectionSpanRuleResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting protection span rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  private ProtectionSpanRuleDetails buildProtectionSpanRuleDetails(
      ContextualConfigObject<ProtectionSpanRule> configObject) {
    return ProtectionSpanRuleDetails.newBuilder()
        .setRule(configObject.getData())
        .setMetadata(
            ProtectionSpanRuleMetadata.newBuilder()
                .setCreationTimestamp(
                    timestampConverter.convert(configObject.getCreationTimestamp()))
                .setLastUpdatedTimestamp(
                    timestampConverter.convert(configObject.getLastUpdatedTimestamp()))
                .build())
        .build();
  }

  private ProtectionSpanRule buildUpdatedRule(
      ProtectionSpanRule existingRule, UpdateProtectionSpanRule updateProtectionSpanRule) {
    return ProtectionSpanRule.newBuilder(existingRule)
        .setRuleInfo(
            ProtectionSpanRuleInfo.newBuilder()
                .setName(updateProtectionSpanRule.getName())
                .setFilter(updateProtectionSpanRule.getFilter())
                .setDisabled(updateProtectionSpanRule.getDisabled())
                .build())
        .build();
  }

  @Override
  public void getAllSamplingConfigs(
      GetAllSamplingConfigsRequest request,
      StreamObserver<GetAllSamplingConfigsResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          GetAllSamplingConfigsResponse.newBuilder()
              .addAllSamplingConfigDetails(
                  this.samplingConfigsConfigStore.getAllData(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get all sampling configs for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createSamplingConfig(
      CreateSamplingConfigRequest request,
      StreamObserver<CreateSamplingConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      // TODO: need to handle priorities
      SamplingConfig newSamplingConfig =
          SamplingConfig.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setSamplingConfigInfo(request.getSamplingConfigInfo())
              .build();

      responseObserver.onNext(
          CreateSamplingConfigResponse.newBuilder()
              .setSamplingConfigDetails(
                  buildSamplingConfigDetails(
                      this.samplingConfigsConfigStore.upsertObject(
                          requestContext, newSamplingConfig)))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error creating sampling config {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateSamplingConfig(
      UpdateSamplingConfigRequest request,
      StreamObserver<UpdateSamplingConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      UpdateSamplingConfig updateSamplingConfig = request.getSamplingConfig();
      SamplingConfig existingSamplingConfig =
          this.samplingConfigsConfigStore
              .getData(requestContext, updateSamplingConfig.getId())
              .orElseThrow(Status.NOT_FOUND::asException);
      SamplingConfig updatedSamplingConfig =
          buildUpdatedSamplingConfig(existingSamplingConfig, updateSamplingConfig);

      responseObserver.onNext(
          UpdateSamplingConfigResponse.newBuilder()
              .setSamplingConfigDetails(
                  buildSamplingConfigDetails(
                      this.samplingConfigsConfigStore.upsertObject(
                          requestContext, updatedSamplingConfig)))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating sampling config: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteSamplingConfig(
      DeleteSamplingConfigRequest request,
      StreamObserver<DeleteSamplingConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      // TODO: need to handle priorities
      this.samplingConfigsConfigStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);

      responseObserver.onNext(DeleteSamplingConfigResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting sampling config: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  private SamplingConfigDetails buildSamplingConfigDetails(
      ContextualConfigObject<SamplingConfig> configObject) {
    return SamplingConfigDetails.newBuilder()
        .setSamplingConfig(configObject.getData())
        .setMetadata(
            SamplingConfigMetadata.newBuilder()
                .setCreationTimestamp(
                    timestampConverter.convert(configObject.getCreationTimestamp()))
                .setLastUpdatedTimestamp(
                    timestampConverter.convert(configObject.getLastUpdatedTimestamp()))
                .build())
        .build();
  }

  private SamplingConfig buildUpdatedSamplingConfig(
      SamplingConfig existingSamplingConfig, UpdateSamplingConfig updateSamplingConfig) {
    return SamplingConfig.newBuilder(existingSamplingConfig)
        .setSamplingConfigInfo(
            SamplingConfigInfo.newBuilder()
                .setRateLimitConfig(updateSamplingConfig.getRateLimitConfig())
                .setFilter(updateSamplingConfig.getFilter())
                .build())
        .build();
  }
}
