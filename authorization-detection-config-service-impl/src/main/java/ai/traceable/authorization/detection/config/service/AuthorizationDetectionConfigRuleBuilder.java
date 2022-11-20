package ai.traceable.authorization.detection.config.service;

import ai.traceable.authorization.detection.config.service.v1.AuthorizationDetectionRule;
import ai.traceable.authorization.detection.config.service.v1.AuthorizationDetectionRule.Builder;
import ai.traceable.authorization.detection.config.service.v1.CreateAuthorizationDetectionRuleRequest;
import ai.traceable.authorization.detection.config.service.v1.UpdateAuthorizationDetectionRuleRequest;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
class AuthorizationDetectionConfigRuleBuilder {
  private final UuidGenerator uuidGenerator;

  AuthorizationDetectionRule build(CreateAuthorizationDetectionRuleRequest request) {
    Builder builder =
        AuthorizationDetectionRule.newBuilder()
            .setId(uuidGenerator.generateRandomId())
            .setAuthorizationType(request.getAuthorizationType())
            .setPredicate(request.getPredicate());

    if (request.hasScope()) {
      builder.setScope(request.getScope());
    }

    return builder.build();
  }

  AuthorizationDetectionRule build(UpdateAuthorizationDetectionRuleRequest request) {
    Builder builder =
        AuthorizationDetectionRule.newBuilder()
            .setId(request.getId())
            .setAuthorizationType(request.getAuthorizationType())
            .setPredicate(request.getPredicate());

    if (request.hasScope()) {
      builder.setScope(request.getScope());
    }

    return builder.build();
  }
}
