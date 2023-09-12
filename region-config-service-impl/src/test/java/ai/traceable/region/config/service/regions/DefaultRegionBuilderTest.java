package ai.traceable.region.config.service.regions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.LatestInstantNamedPathFinder;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.config.utils.refresh.FileRefreshConfig;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DefaultRegionBuilderTest {
  private static final String START_IP_INT_CSV_HEADER = "start_ip_int";
  private static final String END_IP_INT_CSV_HEADER = "end_ip_int";
  private static final String COUNTRY_CSV_HEADER = "country";
  private static final String ISO_CODE_CSV_HEADER = "country_iso_code";
  private static final UuidGenerator uuidGenerator = mock(UuidGenerator.class);
  private static final LatestInstantNamedPathFinder latestInstantNamedPathFinder =
      mock(LatestInstantNamedPathFinder.class);

  @Test
  void shouldBuildRegions(@TempDir Path tempDir) throws Exception {
    mockUuids();
    String fileName = "countries.csv";
    createMockRecords(tempDir.resolve(fileName));
    when(latestInstantNamedPathFinder.get(eq(tempDir.toUri()), any()))
        .thenReturn(Optional.of(tempDir));

    FileRefreshConfig fileRefreshConfig =
        new FileRefreshConfig(
            ConfigFactory.parseMap(
                Map.of(
                    "mode",
                    "VERSIONS_DIR",
                    "versions.dir",
                    tempDir.toFile().getAbsolutePath(),
                    "versions.file.name",
                    fileName,
                    "versions.refresh.duration",
                    Duration.ofSeconds(10))));

    Region region1 =
        new Region(
            uuidGenerator.generateId("ru"),
            "russia",
            RegionType.COUNTRY,
            List.of(
                new IpV4Range(3758096382L, 3758096383L), new IpV4Range(2758096382L, 2758096383L)),
            "ru");
    Region region2 =
        new Region(
            uuidGenerator.generateId("cn"),
            "china",
            RegionType.COUNTRY,
            List.of(new IpV4Range(1L, 2L)),
            "cn");

    DefaultRegionBuilder regionBuilder =
        new DefaultRegionBuilder(
            latestInstantNamedPathFinder, uuidGenerator, fileRefreshConfig, false);

    Map<String, Region> regionIdToRegionMap = regionBuilder.getLatestDataSupplier().get();
    assertEquals(
        Map.of(uuidGenerator.generateId("ru"), region1, uuidGenerator.generateId("cn"), region2),
        regionIdToRegionMap);
  }

  private void createMockRecords(Path countriesFile) throws IOException {
    CSVFormat csvFormat =
        CSVFormat.DEFAULT.withHeader(
            START_IP_INT_CSV_HEADER,
            END_IP_INT_CSV_HEADER,
            COUNTRY_CSV_HEADER,
            ISO_CODE_CSV_HEADER);

    CSVPrinter printer =
        new CSVPrinter(new FileWriter(countriesFile.toFile().getAbsolutePath()), csvFormat);

    String[][] data = {
      {"3758096382", "3758096383", "Russia", "RU"},
      {"2758096382", "2758096383", "Russia", "RU"},
      {"1", "2", "China", "CN"}
    };

    for (String[] row : data) {
      String rowData = Joiner.on(",").join(row);
      CSVParser csvParser = csvFormat.parse(new StringReader(rowData));
      printer.printRecord(csvParser.iterator().next());
    }
    printer.close(true);
  }

  private void mockUuids() {
    when(uuidGenerator.generateId("ru")).thenReturn("id-1");
    when(uuidGenerator.generateId("cn")).thenReturn("id-2");
  }
}
