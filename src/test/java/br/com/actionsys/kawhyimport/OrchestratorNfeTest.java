package br.com.actionsys.kawhyimport;

import br.com.actionsys.kawhycommons.infra.license.LicenseService;
import br.com.actionsys.kawhycommons.integration.IntegrationItem;
import br.com.actionsys.kawhycommons.integration.metadata.MetadataService;
import br.com.actionsys.kawhyimport.impl.nfe.OrchestratorNfe;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.core.io.Resource;

@Slf4j
@SpringBootTest
@MockBean(Schedule.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OrchestratorNfeTest {

  @SpyBean private LicenseService licenseService;
  @Autowired OrchestratorNfe orchestratorNfe;

  // Teste usando a estrutura atual de validação do kawhy-nfe
  @Autowired MetadataService metadataService;

  @Value("classpath:/nfe/xml")
  private Resource nfeXmlResource;

  @Value("classpath:/nfe/metadata/validation")
  private Resource nfeMetadataValidationResource;

  @Value("classpath:/nfe/metadata/import/nfeImportTableMetadata.csv")
  private Resource nfeMetadataImportTableResource;

  @Value("classpath:/nfe/metadata/import/nfeImportFieldMetadata.csv")
  private Resource nfeMetadataImportFieldResource;

  @BeforeAll
  void before() throws Exception {

    Mockito.doReturn(true).when(licenseService).validateCnpjs(Mockito.any());

    try (Stream<Path> files =
        Files.walk(nfeXmlResource.getFile().toPath(), 99).filter(Files::isRegularFile)) {

      files.forEach(this::importFile);
    } catch (IOException e) {
      throw new RuntimeException("Erro ao listar arquivos", e);
    }
  }

  @Test
  void validateAllColumns() {

    Assertions.assertDoesNotThrow(
        () ->
            metadataService.validateFiles(
                nfeMetadataValidationResource.getFile().toPath(),
                nfeXmlResource.getFile().toPath()));
  }

  private void importFile(Path file) {
    try {
      log.info("Importado documento {}", file.getFileName());
      IntegrationItem item = new IntegrationItem(file.toFile());
      orchestratorNfe.processDocumentFile(
          item,
          nfeMetadataImportTableResource.getFile().toPath(),
          nfeMetadataImportFieldResource.getFile().toPath());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
