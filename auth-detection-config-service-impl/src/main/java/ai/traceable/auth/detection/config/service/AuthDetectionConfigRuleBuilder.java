package ai.traceable.auth.detection.config.service;

import ai.traceable.auth.detection.config.service.v1.AuthDetectionRule;
import ai.traceable.auth.detection.config.service.v1.AuthDetectionRule.Builder;
import ai.traceable.auth.detection.config.service.v1.CreateAuthDetectionRuleRequest;
import ai.traceable.auth.detection.config.service.v1.UpdateAuthDetectionRuleRequest;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
class AuthDetectionConfigRuleBuilder {
  private final UuidGenerator uuidGenerator;

  AuthDetectionRule build(CreateAuthDetectionRuleRequest request) {
    Builder builder =
        AuthDetectionRule.newBuilder()
            .setId(uuidGenerator.generateRandomId())
            .setAuthType(request.getAuthType())
            .setPredicate(request.getPredicate());

    if (request.hasScope()) {
      builder.setScope(request.getScope());
    }

    return builder.build();
  }

  AuthDetectionRule build(UpdateAuthDetectionRuleRequest request) {
    Builder builder =
        AuthDetectionRule.newBuilder()
            .setId(request.getId())
            .setAuthType(request.getAuthType())
            .setPredicate(request.getPredicate());

    if (request.hasScope()) {
      builder.setScope(request.getScope());
    }

    return builder.build();
  }
}
