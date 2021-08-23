package org.hypertrace.tag.config.service;

import io.grpc.Channel;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.tag.config.service.v1.CreateTag;
import org.hypertrace.tag.config.service.v1.CreateTagRequest;
import org.hypertrace.tag.config.service.v1.CreateTagResponse;
import org.hypertrace.tag.config.service.v1.DeleteTagRequest;
import org.hypertrace.tag.config.service.v1.GetTagRequest;
import org.hypertrace.tag.config.service.v1.GetTagResponse;
import org.hypertrace.tag.config.service.v1.GetTagsRequest;
import org.hypertrace.tag.config.service.v1.Tag;
import org.hypertrace.tag.config.service.v1.TagConfigServiceGrpc;
import org.hypertrace.tag.config.service.v1.TagConfigServiceGrpc.TagConfigServiceBlockingStub;
import org.hypertrace.tag.config.service.v1.UpdateTagRequest;
import org.hypertrace.tag.config.service.v1.UpdateTagResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TagConfigServiceImplTest {
    TagConfigServiceBlockingStub tagConfigStub;
    MockGenericConfigService mockGenericConfigService;
    List<CreateTag> createTagsList = Arrays.asList(0,1,2,3,4).stream()
            .map(id-> CreateTag.newBuilder()
                    .setKey("Tag-"+id)
                    .build())
            .collect(Collectors.toList());

    @BeforeEach
    void setUp() {
        mockGenericConfigService = new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
        Channel channel = mockGenericConfigService.channel();
        mockGenericConfigService
                .addService(new TagConfigServiceImpl(channel))
                .start();
        tagConfigStub = TagConfigServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void afterEach() {
        mockGenericConfigService.shutdown();
    }

    @Test
    void test_createTag() {
        List<CreateTag> createdTagsList = createTagsList.stream()
                .map(createTag -> {
                    CreateTagResponse response =
                            tagConfigStub.createTag(
                                    CreateTagRequest.newBuilder().setTag(createTag).build()
                            );
                    return CreateTag.newBuilder().setKey(response.getTag().getKey()).build();
                })
                .collect(Collectors.toList());
        assertEquals(createTagsList, createdTagsList);
    }

    @Test
    void test_getTag() {
        List<Tag> createdTagsList = createTagsList.stream()
                .map(createTag -> {
                    CreateTagResponse response =
                            tagConfigStub.createTag(
                                    CreateTagRequest.newBuilder().setTag(createTag).build()
                            );
                    return response.getTag();
                })
                .collect(Collectors.toList());
        List<Tag> getTags = createdTagsList.stream()
                        .map(tag -> {
                            GetTagResponse response =
                                    tagConfigStub.getTag(
                                            GetTagRequest.newBuilder().setId(tag.getId()).build()
                                    );
                            System.out.println(response.getTag());
                            return response.getTag();
                        })
                        .collect(Collectors.toList());
        assertEquals(createdTagsList, getTags);
    }

    @Test
    void test_getTags() {
        List<Tag> createdTagsList = createTagsList.stream()
                .map(createTag -> {
                    CreateTagResponse response =
                            tagConfigStub.createTag(
                                    CreateTagRequest.newBuilder().setTag(createTag).build()
                            );
                    return response.getTag();
                })
                .collect(Collectors.toList());
        List<Tag> getTags = tagConfigStub.getTags(GetTagsRequest.newBuilder().build()).getTagsList();
        assertEquals(Set.copyOf(createdTagsList), Set.copyOf(getTags));
    }

    @Test
    void test_updateTag() {
        List<Tag> createdTagsList = createTagsList.stream()
                .map(createTag -> {
                    CreateTagResponse response =
                            tagConfigStub.createTag(
                                    CreateTagRequest.newBuilder().setTag(createTag).build()
                            );
                    return response.getTag();
                })
                .collect(Collectors.toList());
        List<Tag> updateTagsList =
                createdTagsList.stream()
                        .map(tag -> Tag.newBuilder()
                                    .setId(tag.getId())
                                    .setKey(tag.getKey() + "*")
                                    .build())
                        .collect(Collectors.toList());
        List<Tag> updatedTagsList = updateTagsList.stream()
                .map(updateTag -> {
                    UpdateTagResponse response =
                            tagConfigStub.updateTag(
                                    UpdateTagRequest.newBuilder().setTag(updateTag).build()
                            );
                    return response.getTag();
                })
                .collect(Collectors.toList());
        assertEquals(updateTagsList, updatedTagsList);
    }

    @Test
    void test_deleteTag(){
        List<Tag> createdTagsList = createTagsList.stream()
                .map(createTag -> {
                    CreateTagResponse response =
                            tagConfigStub.createTag(
                                    CreateTagRequest.newBuilder().setTag(createTag).build()
                            );
                    return response.getTag();
                })
                .collect(Collectors.toList());
        createdTagsList.stream()
                .forEach(tag -> {
                    tagConfigStub.deleteTag(DeleteTagRequest.newBuilder().setId(tag.getId()).build());
                    List<Tag> allTags = tagConfigStub.getTags(GetTagsRequest.newBuilder().build()).getTagsList();
                    assertEquals(false, allTags.contains(tag));
                });
        List<Tag> allTags = tagConfigStub.getTags(GetTagsRequest.newBuilder().build()).getTagsList();
        assertEquals(Collections.emptyList(), allTags);
    }
}
