package ai.traceable.config.utils;

import com.jayway.jsonpath.JsonPath;
import io.grpc.Status;

public class JsonPathValidator {
  public static Status validate(String jsonPath) {
    try {
      JsonPath.compile(jsonPath);
    } catch (Exception e) {
      return Status.INVALID_ARGUMENT
          .withCause(e)
          .withDescription(String.format("Invalid json path: %s", jsonPath));
    }
    return Status.OK;
  }
}
