package br.com.actionsys.kawhyimport.metadata;

import br.com.actionsys.kawhycommons.integration.IntegrationItem;
import br.com.actionsys.kawhycommons.integration.metadata.MetadataService;
import br.com.actionsys.kawhyimport.command.SqlCommand;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

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

@Slf4j
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootApplication(scanBasePackages = "br.com.actionsys")
public class CommandsXmlServiceTest {

    @Autowired
    private CommandsXmlService commandsXmlService;

    @Autowired
    private MetadataService metadataService;

    @Test
    @DisplayName("Testar o processamento de Nfes utilizando a estrutura atual de testes do KawhyNfe")
    void validateNfeMetadata() {

        Resource xmlResource = new ClassPathResource("nfe/xml");

        Resource validationMetadataResource = new ClassPathResource("nfe/metadata/validation");

        Resource importTableMetadataResource = new ClassPathResource("nfe/metadata/import/nfeImportTableMetadata.csv");

        Resource importFieldMetadataResource = new ClassPathResource("nfe/metadata/import/nfeImportFieldMetadata.csv");

        importFiles(xmlResource, importTableMetadataResource, importFieldMetadataResource);

        Assertions.assertDoesNotThrow(
                () ->
                        metadataService.validateFiles(
                                validationMetadataResource.getFile().toPath(), xmlResource.getFile().toPath()));
    }

    private void importFiles(
            Resource xmlResource,
            Resource importTableMetadataResource,
            Resource importFieldMetadataResource) {

        try (Stream<Path> files =
                     Files.walk(xmlResource.getFile().toPath(), 99).filter(Files::isRegularFile)) {

            files.forEach(
                    path -> importFile(path, importTableMetadataResource, importFieldMetadataResource));

        } catch (IOException e) {
            throw new RuntimeException("Erro ao importar arquivos", e);
        }
    }

    private void importFile(Path file, Resource tableMetadataResource, Resource fieldMetadataResource) {

        try {
            log.info("Importado documento {}", file.getFileName());

            IntegrationItem item = new IntegrationItem(file.toFile());

            List<SqlCommand> commands =
                    commandsXmlService.generateCommandsFromXml(
                            item,
                            tableMetadataResource.getFile().toPath(),
                            fieldMetadataResource.getFile().toPath());

            commandsXmlService.executeCommands(commands);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
