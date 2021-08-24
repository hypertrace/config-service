package org.hypertrace.tag.config.service;

import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.tag.config.service.v1.Tag;

public interface ConfigServiceCoordinator {

  Tag upsertTag(RequestContext requestContext, Tag tag);

  Tag getTag(RequestContext requestContext, String tagId);

  List<Tag> getAllTags(RequestContext requestContext);

  void deleteTag(RequestContext requestContext, String tagId);
}
