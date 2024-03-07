package ai.traceable.data.classification.cache.client;

import ai.traceable.data.classification.cache.info.DataClassificationInfo;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest.DataTypeFilter;
import javax.annotation.Nonnull;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface DataClassificationClient {
  public DataClassificationInfo getDataClassificationInfo(RequestContext requestContext);

  public DataClassificationInfo getDataClassificationInfo(
      RequestContext requestContext, @Nonnull DataTypeFilter dataTypeFilter);
}
