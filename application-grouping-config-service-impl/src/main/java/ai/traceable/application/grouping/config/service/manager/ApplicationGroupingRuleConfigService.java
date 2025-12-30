package ai.traceable.application.grouping.config.service.manager;

import ai.traceable.application.grouping.config.service.v1.CreateApplicationGroupingRuleConfigRequest;
import ai.traceable.application.grouping.config.service.v1.DeleteApplicationGroupingRuleConfigsRequest;
import ai.traceable.application.grouping.config.service.v1.GetAllApplicationGroupingRuleConfigsRequest;
import ai.traceable.application.grouping.config.service.v1.UpdateApplicationGroupingRuleConfigRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ApplicationGroupingRuleConfigService {
  List<ai.traceable.application.grouping.config.service.v1.ApplicationGroupingRuleConfig>
      getAllApplicationGroupingRuleConfigs(
          RequestContext requestContext, GetAllApplicationGroupingRuleConfigsRequest request);

  ai.traceable.application.grouping.config.service.v1.ApplicationGroupingRuleConfig
      createApplicationGroupingRuleConfig(
          RequestContext requestContext, CreateApplicationGroupingRuleConfigRequest request);

  ai.traceable.application.grouping.config.service.v1.ApplicationGroupingRuleConfig
      updateApplicationGroupingRuleConfig(
          RequestContext requestContext, UpdateApplicationGroupingRuleConfigRequest request);

  void deleteApplicationGroupingRuleConfigs(
      RequestContext requestContext, DeleteApplicationGroupingRuleConfigsRequest request);

  boolean doesApplicationGroupingRuleConfigExist(RequestContext requestContext, String id);
}
