package ai.traceable.auth.detection.config.service;

import ai.traceable.auth.detection.config.service.v1.AuthDetectionRule;
import ai.traceable.auth.detection.config.service.v1.AuthDetectionRuleFilter;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

/** This class seres to manage persistence across both default and user-defined rules */
@AllArgsConstructor(onConstructor_ = {@Inject})
class AuthDetectionRuleManager {
  private final DefaultAuthRuleConfig defaultRuleConfig;
  private final UserDefinedAuthDetectionRuleStore userDefinedRuleStore;
  private final DeletedDefaultAuthDetectionRuleStore deletedDefaultRuleStore;

  List<AuthDetectionRule> getAll(RequestContext requestContext, AuthDetectionRuleFilter filter) {
    return ImmutableList.<AuthDetectionRule>builder()
        .addAll(this.getUndeletedDefaultRules(requestContext))
        .addAll(userDefinedRuleStore.getAllConfigData(requestContext, filter))
        .build();
  }

  AuthDetectionRule create(RequestContext requestContext, AuthDetectionRule rule) {
    return this.userDefinedRuleStore.upsertObject(requestContext, rule).getData();
  }

  AuthDetectionRule update(RequestContext requestContext, AuthDetectionRule rule) {
    if (defaultRuleConfig.isDefaultConfig(rule.getId())) {
      AuthDetectionRule updatedRule =
          userDefinedRuleStore.upsertObject(requestContext, rule).getData();
      deletedDefaultRuleStore.markDefaultIdDeleted(requestContext, rule.getId());
      return updatedRule;
    }

    return userDefinedRuleStore.upsertObject(requestContext, rule).getData();
  }

  void delete(RequestContext requestContext, String id) {
    if (isUnmodifiedDefault(requestContext, id)) {
      deletedDefaultRuleStore.markDefaultIdDeleted(requestContext, id);
    } else {
      userDefinedRuleStore
          .deleteObject(requestContext, id)
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);
    }
  }

  boolean ruleExists(RequestContext requestContext, String id) {
    return isUnmodifiedDefault(requestContext, id)
        || userDefinedRuleStore.getObject(requestContext, id).isPresent();
  }

  List<AuthDetectionRule> getUndeletedDefaultRules(RequestContext requestContext) {
    Set<String> deletedIds = this.deletedDefaultRuleStore.getDeletedDefaultIds(requestContext);
    return this.defaultRuleConfig.getAll().stream()
        .filter(defaultRule -> !deletedIds.contains(defaultRule.getId()))
        .collect(Collectors.toUnmodifiableList());
  }

  private boolean isUnmodifiedDefault(RequestContext requestContext, String id) {
    return defaultRuleConfig.isDefaultConfig(id)
        && deletedDefaultRuleStore.getObject(requestContext, id).isEmpty();
  }
}
