package ai.traceable.data.parsing.config.service.v1.utils;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.data.parsing.config.service.v1.CreateDataParsingRuleRequest;
import ai.traceable.data.parsing.config.service.v1.DataParsingConfig;
import jakarta.inject.Inject;

public class DataParsingConfigGenerator {

  private final UuidGenerator uuidGenerator;

  @Inject
  public DataParsingConfigGenerator(UuidGenerator uuidGenerator) {
    this.uuidGenerator = uuidGenerator;
  }

  public DataParsingConfig generateNewConfig(CreateDataParsingRuleRequest request) {
    return DataParsingConfig.newBuilder()
        .setId(this.uuidGenerator.generateRandomId())
        .mergeFrom(request.getDataParsingRule())
        .build();
  }
}
