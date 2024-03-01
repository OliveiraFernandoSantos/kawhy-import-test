package br.com.actionsys.kawhyimport.metadata;

import br.com.actionsys.kawhycommons.integration.IntegrationItem;
import br.com.actionsys.kawhycommons.integration.metadata.MetadataService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

@Slf4j
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootApplication(scanBasePackages = "br.com.actionsys")
public class ImportServiceTest {

    @Autowired
    ImportService importService;

    @Autowired
    MetadataService metadataService;

    @Test
    @DisplayName("Testar o processamento de Nfes utilizando a estrutura atual de testes do KawhyNfe")
    void validateNfeMetadata() {

        Resource xmlFolder = new ClassPathResource("nfe/xml");
        Resource validationMetadataFolder = new ClassPathResource("nfe/metadata/validation");
        Resource importMetadataFile = new ClassPathResource("nfe/metadata/import/nfeImportMetadata.csv");

        importFiles(xmlFolder, importMetadataFile);

        Assertions.assertDoesNotThrow(() -> metadataService.validateFiles(validationMetadataFolder.getFile().toPath(), xmlFolder.getFile().toPath()));
    }

    private void importFiles(Resource xmlFolder, Resource importMetadataFile) {

        try (Stream<Path> files = Files.walk(xmlFolder.getFile().toPath(), 99).filter(Files::isRegularFile)) {

            files.forEach(file -> importFile(file, importMetadataFile));

        } catch (IOException e) {
            throw new RuntimeException("Erro ao importar arquivos", e);
        }
    }

    private void importFile(Path file, Resource importMetadataFile) {

        try {
            log.info("Importado documento {}", file.getFileName());

            IntegrationItem item = new IntegrationItem(file.toFile());

            importService.process(item, importMetadataFile.getFile().toPath());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
