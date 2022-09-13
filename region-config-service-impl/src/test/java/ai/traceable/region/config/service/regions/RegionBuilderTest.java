package ai.traceable.region.config.service.regions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.region.config.service.utils.UuidGenerator;
import com.google.common.base.Joiner;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RegionBuilderTest {
  private static final String START_IP_INT_CSV_HEADER = "start_ip_int";
  private static final String END_IP_INT_CSV_HEADER = "end_ip_int";
  private static final String COUNTRY_CSV_HEADER = "country";

  private UuidGenerator uuidGenerator;
  private RegionBuilder regionBuilder;

  @BeforeEach
  void setup() {
    this.uuidGenerator = mock(UuidGenerator.class);
    this.regionBuilder = new RegionBuilder(uuidGenerator, "");
  }

  @Test
  void shouldBuildRegions() throws IOException {
    mockUuids();
    Map<String, Region> regionIdToRegionMap = this.regionBuilder.buildRegions(mockRecords());
    assertEquals(
        Map.of(
            "id-1",
            new Region(
                "id-1",
                "Russia",
                RegionType.COUNTRY,
                List.of(
                    new IpV4Range(3758096382L, 3758096383L),
                    new IpV4Range(2758096382L, 2758096383L))),
            "id-2",
            new Region("id-2", "China", RegionType.COUNTRY, List.of(new IpV4Range(1L, 2L)))),
        regionIdToRegionMap);
  }

  private Iterable<CSVRecord> mockRecords() throws IOException {
    List<CSVRecord> csvRecords = new ArrayList<>();

    String[][] data = {
      {"3758096382", "3758096383", "Russia"},
      {"2758096382", "2758096383", "Russia"},
      {"1", "2", "China"}
    };

    CSVFormat csvFormat =
        CSVFormat.DEFAULT.withHeader(
            START_IP_INT_CSV_HEADER, END_IP_INT_CSV_HEADER, COUNTRY_CSV_HEADER);

    for (String[] row : data) {
      String rowData = Joiner.on(",").join(row);
      CSVParser csvParser = csvFormat.parse(new StringReader(rowData));
      csvRecords.add(csvParser.iterator().next());
    }

    return csvRecords;
  }

  private void mockUuids() {
    when(uuidGenerator.generateId("Russia")).thenReturn("id-1");
    when(uuidGenerator.generateId("China")).thenReturn("id-2");
  }
}
