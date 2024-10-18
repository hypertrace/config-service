package ai.traceable.userattribution.config.service.v2.migration;

import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest.GetUserAttributionRulesFilter;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest.GetUserAttributionRulesFilter.ScopeFilter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class UserAttributionFilterConverter {

  GetUserAttributionRulesFilter convert(
      ai.traceable.userattribution.config.service.v2.GetUserAttributionRulesRequest
              .GetUserAttributionRulesFilter
          filter) {
    GetUserAttributionRulesFilter.Builder builder = GetUserAttributionRulesFilter.newBuilder();
    if (filter.hasDisabled()) {
      builder.setDisabled(filter.getDisabled());
    }
    if (filter.hasEnvironmentFilter()) {
      builder.setScopeFilter(
          ScopeFilter.newBuilder()
              .setEnvironmentScopeFilter(
                  ScopeFilter.EnvironmentScopeFilter.newBuilder()
                      .addAllEnvironmentNames(
                          filter.getEnvironmentFilter().getEnvironmentNamesList())));
    }
    return builder.build();
  }
}
