package ai.traceable.jwt.extraction.config.service;

import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRuleFilter;
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
class JwtExtractionRuleManager {
  private final DefaultJwtExtractionRuleConfig defaultRuleConfig;
  private final UserDefinedJwtExtractionRuleStore userDefinedRuleStore;
  private final DeletedDefaultJwtExtractionRuleStore deletedDefaultRuleStore;

  List<JwtExtractionRule> getAll(RequestContext requestContext, JwtExtractionRuleFilter filter) {
    return ImmutableList.<JwtExtractionRule>builder()
        .addAll(this.getUndeletedDefaultRules(requestContext))
        .addAll(userDefinedRuleStore.getAllConfigData(requestContext, filter))
        .build();
  }

  JwtExtractionRule create(RequestContext requestContext, JwtExtractionRule rule) {
    return this.userDefinedRuleStore.upsertObject(requestContext, rule).getData();
  }

  JwtExtractionRule update(RequestContext requestContext, JwtExtractionRule rule) {
    if (defaultRuleConfig.isDefaultConfig(rule.getId())) {
      JwtExtractionRule updatedRule =
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

  List<JwtExtractionRule> getUndeletedDefaultRules(RequestContext requestContext) {
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
