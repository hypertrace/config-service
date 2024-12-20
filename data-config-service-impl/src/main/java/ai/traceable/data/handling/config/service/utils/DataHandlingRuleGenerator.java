package ai.traceable.data.handling.config.service.utils;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.data.handling.config.service.v1.CreateDataHandlingRuleRequest;
import ai.traceable.data.handling.config.service.v1.DataHandlingRule;
import jakarta.inject.Inject;

public class DataHandlingRuleGenerator {

  private final UuidGenerator uuidGenerator;

  @Inject
  public DataHandlingRuleGenerator(UuidGenerator uuidGenerator) {
    this.uuidGenerator = uuidGenerator;
  }

  public DataHandlingRule generateNewRuleWithoutRank(CreateDataHandlingRuleRequest request) {
    return DataHandlingRule.newBuilder()
        .setData(request.getData())
        .setId(this.uuidGenerator.generateRandomId())
        .build();
  }
}
