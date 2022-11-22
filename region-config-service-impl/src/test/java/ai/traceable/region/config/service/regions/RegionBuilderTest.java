package ai.traceable.region.config.service.regions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.LastModifiedPathFinder;
import ai.traceable.region.config.service.RegionConfigServiceConfig;
import ai.traceable.region.config.service.utils.UuidGenerator;
import com.google.common.base.Joiner;
import com.typesafe.config.ConfigFactory;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RegionBuilderTest {
  private static final String START_IP_INT_CSV_HEADER = "start_ip_int";
  private static final String END_IP_INT_CSV_HEADER = "end_ip_int";
  private static final String COUNTRY_CSV_HEADER = "country";

  private UuidGenerator uuidGenerator;
  private RegionBuilder regionBuilder;
  private LastModifiedPathFinder lastModifiedPathFinder;

  @BeforeEach
  void setup() {
    this.uuidGenerator = mock(UuidGenerator.class);
    this.lastModifiedPathFinder = mock(LastModifiedPathFinder.class);
    this.regionBuilder = new RegionBuilder(uuidGenerator, lastModifiedPathFinder);
  }

  @Test
  void shouldBuildRegions(@TempDir Path tempDir) throws Exception {
    mockUuids();
    String fileName = "countries.csv";
    createMockRecords(tempDir.resolve(fileName));
    when(lastModifiedPathFinder.get(eq(tempDir.toUri()), any())).thenReturn(Optional.of(tempDir));
    Map<String, Region> regionIdToRegionMap =
        this.regionBuilder
            .buildRegions(
                new RegionConfigServiceConfig.CountriesDataConfig(
                    ConfigFactory.parseMap(
                        Map.of(
                            "mode",
                            "VERSIONS_DIR",
                            "versions.dir",
                            tempDir.toFile().getAbsolutePath(),
                            "versions.file.name",
                            fileName,
                            "versions.refresh.duration",
                            Duration.ofSeconds(10)))))
            .get();
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

  private void createMockRecords(Path countriesFile) throws IOException {
    CSVFormat csvFormat =
        CSVFormat.DEFAULT.withHeader(
            START_IP_INT_CSV_HEADER, END_IP_INT_CSV_HEADER, COUNTRY_CSV_HEADER);
    CSVPrinter printer =
        new CSVPrinter(new FileWriter(countriesFile.toFile().getAbsolutePath()), csvFormat);
    String[][] data = {
      {"3758096382", "3758096383", "Russia"},
      {"2758096382", "2758096383", "Russia"},
      {"1", "2", "China"}
    };

    for (String[] row : data) {
      String rowData = Joiner.on(",").join(row);
      CSVParser csvParser = csvFormat.parse(new StringReader(rowData));
      printer.printRecord(csvParser.iterator().next());
    }
    printer.close(true);
  }

  private void mockUuids() {
    when(uuidGenerator.generateId("Russia")).thenReturn("id-1");
    when(uuidGenerator.generateId("China")).thenReturn("id-2");
  }
}
