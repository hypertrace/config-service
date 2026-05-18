package ai.traceable.fraud.datamodel.event.kind.eventkind;

import ai.traceable.fraud.datamodel.event.kind.v1.DataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.EventKindFilter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.protobuf.util.JsonFormat;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** Default implementation that loads event kinds from YAML resource file. */
@Slf4j
@Singleton
public class DefaultEventKindProvider implements EventKindProvider {

  private static final String RESOURCE_FILE = "datamodel_event_kinds.yaml";
  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();

  private final List<DataModelEventKind> allEventKinds;
  private final List<DataModelEventKind> selectableEventKinds;

  @Inject
  public DefaultEventKindProvider() {
    this.allEventKinds = loadEventKinds();
    this.selectableEventKinds =
        allEventKinds.stream()
            .filter(kind -> !kind.getParentKindId().isEmpty())
            .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public List<DataModelEventKind> getEventKinds(EventKindFilter filter) {
    return selectableEventKinds;
  }

  @Override
  public List<DataModelEventKind> getAllEventKinds() {
    return allEventKinds;
  }

  private List<DataModelEventKind> loadEventKinds() {
    try {
      JsonNode root =
          YAML_MAPPER.readValue(
              getClass().getClassLoader().getResourceAsStream(RESOURCE_FILE), JsonNode.class);
      JsonNode kindsNode = root.get("kinds");

      if (kindsNode == null || !kindsNode.isArray()) {
        log.error("Invalid YAML structure in {}: missing 'kinds' array", RESOURCE_FILE);
        return Collections.emptyList();
      }

      List<DataModelEventKind> result = new ArrayList<>();
      for (JsonNode kindNode : kindsNode) {
        String json = JSON_MAPPER.writeValueAsString(kindNode);
        DataModelEventKind.Builder builder = DataModelEventKind.newBuilder();
        JSON_PARSER.merge(json, builder);
        result.add(builder.build());
      }
      return Collections.unmodifiableList(result);
    } catch (Exception e) {
      log.error("Failed to load event kinds from {}", RESOURCE_FILE, e);
      return Collections.emptyList();
    }
  }
}
