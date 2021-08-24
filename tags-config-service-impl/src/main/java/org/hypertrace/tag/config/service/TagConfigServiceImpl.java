package org.hypertrace.tag.config.service;

import com.typesafe.config.Config;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
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
  private final List<String> SYSTEM_TAGS;
  private final List<String> SYSTEM_TAG_PATHS =
      Arrays.asList(
          "tag.config.service.CRITICAL",
          "tag.config.service.SENSITIVE",
          "tag.config.service.EXTERNAL",
          "tag.config.service.SENTRY");

  public TagConfigServiceImpl(Channel configChannel, Config config) {
    configServiceCoordinator = new ConfigServiceCoordinatorImpl(configChannel);
    SYSTEM_TAGS =
        SYSTEM_TAG_PATHS.stream()
            .map(tagPath -> config.getString(tagPath))
            .collect(Collectors.toList());
  }

  @Override
  public void createTag(
      CreateTagRequest request, StreamObserver<CreateTagResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      CreateTag createTag = request.getTag();
      if (SYSTEM_TAGS.contains(createTag.getKey())) {
        // Creating a tag with a name that clashes with one of system tags name
        responseObserver.onError(new StatusRuntimeException(Status.ALREADY_EXISTS));
        return;
      }
      Tag tag =
          Tag.newBuilder().setId(UUID.randomUUID().toString()).setKey(createTag.getKey()).build();
      Tag createdTag = configServiceCoordinator.upsertTag(requestContext, tag);
      responseObserver.onNext(CreateTagResponse.newBuilder().setTag(createdTag).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getTag(GetTagRequest request, StreamObserver<GetTagResponse> responseObserver) {
    String tagId = request.getId();
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      Tag tag;
      if (SYSTEM_TAGS.contains(tagId)) {
        tag = Tag.newBuilder().setId(tagId).setKey(tagId).build();
      } else {
        tag = configServiceCoordinator.getTag(requestContext, tagId);
      }
      responseObserver.onNext(GetTagResponse.newBuilder().setTag(tag).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getTags(GetTagsRequest request, StreamObserver<GetTagsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    List<Tag> tagList = configServiceCoordinator.getAllTags(requestContext);
    List<Tag> systemTagsList =
        SYSTEM_TAGS.stream()
            .map(key -> Tag.newBuilder().setId(key).setKey(key).build())
            .collect(Collectors.toList());
    tagList.addAll(systemTagsList);
    responseObserver.onNext(GetTagsResponse.newBuilder().addAllTags(tagList).build());
    responseObserver.onCompleted();
  }

  @Override
  public void updateTag(
      UpdateTagRequest request, StreamObserver<UpdateTagResponse> responseObserver) {
    Tag updatedTagInReq = request.getTag();
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      if (SYSTEM_TAGS.contains(updatedTagInReq.getId())) {
        // Updating a system tag will error
        responseObserver.onError(new StatusRuntimeException(Status.PERMISSION_DENIED));
        return;
      }
      configServiceCoordinator.getTag(requestContext, updatedTagInReq.getId());
      if (SYSTEM_TAGS.contains(updatedTagInReq.getKey())) {
        // Updating the name of some tag to name of some system tag will error
        responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
        return;
      }
      Tag updatedTagInRes = configServiceCoordinator.upsertTag(requestContext, updatedTagInReq);
      responseObserver.onNext(UpdateTagResponse.newBuilder().setTag(updatedTagInRes).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteTag(
      DeleteTagRequest request, StreamObserver<DeleteTagResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      String tagId = request.getId();
      if (SYSTEM_TAGS.contains(tagId)) {
        // Deleting a system tag
        responseObserver.onError(new StatusRuntimeException(Status.PERMISSION_DENIED));
        return;
      }
      configServiceCoordinator.getTag(requestContext, tagId);
      configServiceCoordinator.deleteTag(requestContext, tagId);
      responseObserver.onNext(DeleteTagResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }
}
