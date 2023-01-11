package ai.traceable.span.processing.config.service.servicenaming;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.span.processing.config.service.v1.CreateServiceNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRule;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRule.Builder;
import ai.traceable.span.processing.config.service.v1.UpdateServiceNamingRuleRequest;
import com.google.protobuf.util.Timestamps;
import java.util.List;
import javax.inject.Inject;
import lombok.RequiredArgsConstructor;
import org.hypertrace.config.objectstore.ConfigObject;

@RequiredArgsConstructor(onConstructor_ = @Inject)
class ServiceNamingRuleBuilder {
  private final UuidGenerator uuidGenerator;

  ServiceNamingRule buildNewRule(
      List<ServiceNamingRule> existingRules, CreateServiceNamingRuleRequest request) {
    int maxCurrentRank =
        existingRules.stream().mapToInt(ServiceNamingRule::getRank).max().orElse(0);
    Builder builder =
        ServiceNamingRule.newBuilder()
            .setId(uuidGenerator.generateRandomId())
            .setName(request.getName())
            .addAllConditions(request.getConditionsList())
            .setAction(request.getAction())
            .setEnabled(request.getEnabled())
            .setRank(maxCurrentRank + 1);

    // TODO explicit rank support in future

    if (request.hasDescription()) {
      builder.setDescription(request.getDescription());
    }
    if (request.hasScope()) {
      builder.setScope(request.getScope());
    }

    return builder.build();
  }

  ServiceNamingRule buildUpdatedRule(
      ServiceNamingRule previous, UpdateServiceNamingRuleRequest request) {
    Builder builder =
        ServiceNamingRule.newBuilder()
            .setId(request.getId())
            .setName(request.getName())
            .addAllConditions(request.getConditionsList())
            .setAction(request.getAction())
            .setEnabled(request.getEnabled())
            .setRank(previous.getRank());

    if (request.hasDescription()) {
      builder.setDescription(request.getDescription());
    }
    if (request.hasScope()) {
      builder.setScope(request.getScope());
    }

    return builder.build();
  }

  ServiceNamingRule buildFromConfigObject(
      ConfigObject<ServiceNamingRule> serviceNamingRuleConfigObject) {
    return serviceNamingRuleConfigObject.getData().toBuilder()
        .setCreationTimestamp(
            Timestamps.fromMillis(
                serviceNamingRuleConfigObject.getCreationTimestamp().toEpochMilli()))
        .setLastUpdatedTimestamp(
            Timestamps.fromMillis(
                serviceNamingRuleConfigObject.getLastUpdatedTimestamp().toEpochMilli()))
        .build();
  }
}
