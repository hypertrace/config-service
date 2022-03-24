package ai.traceable.localprocessing.config.service.apinaming.http.utils;

import java.util.Optional;
import lombok.Value;

@Value
public class ServiceIdentifier {
  String serviceName;
  Optional<String> environment;
}
