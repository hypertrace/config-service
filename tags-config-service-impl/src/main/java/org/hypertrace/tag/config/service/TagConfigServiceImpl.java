package org.hypertrace.tag.config.service;

import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.tag.config.service.v1.CreateTag;
import org.hypertrace.tag.config.service.v1.CreateTagRequest;
import org.hypertrace.tag.config.service.v1.CreateTagResponse;
import org.hypertrace.tag.config.service.v1.DeleteTagRequest;
import org.hypertrace.tag.config.service.v1.DeleteTagResponse;
import org.hypertrace.tag.config.service.v1.GetTagRequest;
import org.hypertrace.tag.config.service.v1.GetTagResponse;
import org.hypertrace.tag.config.service.v1.GetTagsRequest;
import org.hypertrace.tag.config.service.v1.GetTagsResponse;
import org.hypertrace.tag.config.service.v1.Tag;
import org.hypertrace.tag.config.service.v1.TagConfigServiceGrpc;
import org.hypertrace.tag.config.service.v1.UpdateTagRequest;
import org.hypertrace.tag.config.service.v1.UpdateTagResponse;

@Slf4j
public class TagConfigServiceImpl extends TagConfigServiceGrpc.TagConfigServiceImplBase {
  private final ConfigServiceCoordinator configServiceCoordinator;

  public TagConfigServiceImpl(Channel configChannel) {
    configServiceCoordinator = new ConfigServiceCoordinatorImpl(configChannel);
  }

  @Override
  public void createTag(
      CreateTagRequest request, StreamObserver<CreateTagResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      CreateTag createTag = request.getTag();
      Tag tag =
          Tag.newBuilder().setId(UUID.randomUUID().toString()).setKey(createTag.getKey()).build();
      Tag createdTag = configServiceCoordinator.upsertTag(requestContext, tag);
      responseObserver.onNext(CreateTagResponse.newBuilder().setTag(createdTag).build());
      responseObserver.onCompleted();

    } catch (Exception e) {
      log.error("Create Tag RPC failed for request:{} with exception {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getTag(GetTagRequest request, StreamObserver<GetTagResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      String tagId = request.getId();
      Tag tag = configServiceCoordinator.getTag(requestContext, tagId);
      responseObserver.onNext(GetTagResponse.newBuilder().setTag(tag).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get Tag RPC failed for request:{} with exception {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getTags(GetTagsRequest request, StreamObserver<GetTagsResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      List<Tag> tagList = configServiceCoordinator.getAllTags(requestContext);
      responseObserver.onNext(GetTagsResponse.newBuilder().addAllTags(tagList).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get All Tags RPC failed for request:{} with exception {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateTag(
      UpdateTagRequest request, StreamObserver<UpdateTagResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      Tag updateTag = request.getTag();
      Tag updatedTag = configServiceCoordinator.upsertTag(requestContext, updateTag);
      responseObserver.onNext(UpdateTagResponse.newBuilder().setTag(updatedTag).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Update Tag RPC failed for request:{} with exception {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteTag(
      DeleteTagRequest request, StreamObserver<DeleteTagResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      String tagId = request.getId();
      configServiceCoordinator.deleteTag(requestContext, tagId);
      responseObserver.onNext(DeleteTagResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Delete Tag RPC failed for request:{} with exception {}", request, e);
      responseObserver.onError(e);
    }
  }
}
