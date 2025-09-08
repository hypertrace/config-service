package ai.traceable.attribute.resolution.config.service.v1;

import ai.traceable.attribute.resolution.config.service.v1.AttributeResolutionConfigServiceGrpc.AttributeResolutionConfigServiceImplBase;
import ai.traceable.attribute.resolution.config.service.v1.manager.AttributeResolutionConfigManager;
import ai.traceable.attribute.resolution.config.service.v1.validation.AttributeResolutionConfigValidator;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class AttributeResolutionConfigServiceImpl extends AttributeResolutionConfigServiceImplBase {
  private final AttributeResolutionConfigValidator validator;
  private final AttributeResolutionConfigManager manager;

  @Override
  public void getAttributeResolutionConfigs(
      GetAttributeResolutionConfigsRequest request,
      StreamObserver<GetAttributeResolutionConfigsResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      validator.validateOrThrow(context, request);
      List<AttributeResolutionConfig> configs =
          manager.getAttributeResolutionConfigs(context, request.getFilter());
      GetAttributeResolutionConfigsResponse response =
          GetAttributeResolutionConfigsResponse.newBuilder().addAllConfigs(configs).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Failed to fetch attribute resolution configs", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createAttributeResolutionConfig(
      CreateAttributeResolutionConfigRequest request,
      StreamObserver<CreateAttributeResolutionConfigResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      validator.validateOrThrow(context, request);
      AttributeResolutionConfig config = manager.createAttributeResolutionConfig(context, request);
      CreateAttributeResolutionConfigResponse response =
          CreateAttributeResolutionConfigResponse.newBuilder().setConfig(config).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Failed to create attribute resolution config", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateAttributeResolutionConfig(
      UpdateAttributeResolutionConfigRequest request,
      StreamObserver<UpdateAttributeResolutionConfigResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      validator.validateOrThrow(context, request);
      AttributeResolutionConfig config = manager.updateAttributeResolutionConfig(context, request);
      UpdateAttributeResolutionConfigResponse response =
          UpdateAttributeResolutionConfigResponse.newBuilder().setConfig(config).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Failed to update attribute resolution config", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteAttributeResolutionConfig(
      DeleteAttributeResolutionConfigRequest request,
      StreamObserver<DeleteAttributeResolutionConfigResponse> responseObserver) {
    try {
      RequestContext context = RequestContext.CURRENT.get();
      validator.validateOrThrow(context, request);
      manager.deleteAttributeResolutionConfig(context, request.getId());
      responseObserver.onNext(DeleteAttributeResolutionConfigResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Failed to delete attribute resolution config", e);
      responseObserver.onError(e);
    }
  }
}
