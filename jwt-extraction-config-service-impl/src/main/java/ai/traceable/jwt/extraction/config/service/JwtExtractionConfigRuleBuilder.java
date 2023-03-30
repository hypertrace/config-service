package ai.traceable.jwt.extraction.config.service;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.jwt.extraction.config.service.v1.CreateJwtExtractionRuleRequest;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRule.Builder;
import ai.traceable.jwt.extraction.config.service.v1.UpdateJwtExtractionRuleRequest;
import com.google.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
class JwtExtractionConfigRuleBuilder {
  private final UuidGenerator uuidGenerator;

  JwtExtractionRule build(CreateJwtExtractionRuleRequest request) {
    Builder builder =
        JwtExtractionRule.newBuilder()
            .setId(uuidGenerator.generateRandomId())
            .setName(request.getName())
            .setPredicate(request.getPredicate())
            .addAllLocations(request.getLocationsList())
            .addAllInstructions(request.getInstructionsList());

    if (request.hasScope()) {
      builder.setScope(request.getScope());
    }

    return builder.build();
  }

  JwtExtractionRule build(UpdateJwtExtractionRuleRequest request) {
    Builder builder =
        JwtExtractionRule.newBuilder()
            .setId(request.getId())
            .setName(request.getName())
            .setPredicate(request.getPredicate())
            .addAllLocations(request.getLocationsList())
            .addAllInstructions(request.getInstructionsList())
            .setDisabled(request.getDisabled());

    if (request.hasScope()) {
      builder.setScope(request.getScope());
    }

    return builder.build();
  }
}
