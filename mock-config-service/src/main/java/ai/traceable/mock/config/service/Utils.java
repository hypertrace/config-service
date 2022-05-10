package ai.traceable.mock.config.service;

import static com.google.common.io.Resources.getResource;

import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Utils {
  public String readJson(String resourceName) throws IOException {
    return Resources.toString(getResource(resourceName), StandardCharsets.UTF_8);
  }
}
