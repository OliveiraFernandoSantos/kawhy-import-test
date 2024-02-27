package br.com.actionsys.kawhyimport.metadata.validation;

import br.com.actionsys.kawhycommons.infra.util.XmlUtil;
import br.com.actionsys.kawhyimport.metadata.field.FieldMapping;
import br.com.actionsys.kawhyimport.metadata.reader.DbReaderService;
import br.com.actionsys.kawhyimport.metadata.reader.XmlReaderService;
import br.com.actionsys.kawhyimport.metadata.table.TableMapping;
import br.com.actionsys.kawhyimport.metadata.table.TableMappingService;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;

/**
 * Classe criada para validar os registros inseridos no banco de dados contra os dados do xml
 */
@Slf4j
@Service
public class MetadataValidationService {

  @Autowired private TableMappingService tableMappingService;
  @Autowired private XmlReaderService xmlReaderService;
  @Autowired private DbReaderService dbReaderService;

  public void validateFiles(Path xmlDirectory, Path tableMetadataFile, Path fieldMetadataFile) {

    List<TableMapping> tables = tableMappingService.read(tableMetadataFile, fieldMetadataFile);

    try (Stream<Path> files = Files.walk(xmlDirectory, 99).filter(Files::isRegularFile)) {
      files.forEach(file -> validateFile(file, tables));
    } catch (IOException e) {
      throw new RuntimeException("Erro ao listar arquivos", e);
    }
  }

  public void validateFile(Path file, List<TableMapping> tables) {

    log.info("Validando arquivo {}", file.getFileName());

    try {
      Document document = XmlUtil.buildDocument(file.toFile());
      tables.forEach(table -> validateTable(table, document));
    } catch (Exception e) {
      throw new RuntimeException("Erro ao validar arquivo " + file.getFileName(), e);
    }
  }

  private void validateTable(TableMapping table, Document document) {

    table.getFields().forEach(field -> validateField(table, field, document));
  }

  private void validateField(TableMapping table, FieldMapping field, Document document) {

    try {
      List<?> xmlValue =
          xmlReaderService.getListOfValues(document, field).stream()
              .filter(Objects::nonNull)
              .collect(Collectors.toList());

      List<?> databaseValue =
          dbReaderService.getDatabaseValue(table, field, document).stream()
              .filter(Objects::nonNull)
              .collect(Collectors.toList());

      // parar o metodo caso nao tenha gravado registro para a linha
      // valida se o id existe no banco de dados
      if (field.getVariable().equalsIgnoreCase(table.getIdField().getVariable())
          && databaseValue.isEmpty()) {
        log.debug("Nao foi gravado registro para o campo: " + field);
        return;
      }

      // validar em caso de sequenceLine
      FieldMapping sequenceField = table.getSequenceField();
      if (sequenceField != null && sequenceField.getVariable().equals(field.getVariable())) {
        log.debug("Validando tamanho da lista pois o campo é a sequencia {}", field);

        if (xmlValue.size() != databaseValue.size()) {
          log.error("Listas com tamanhos diferentes na validacao do campo nSequencia {}", field);
          log.error("xmlValue: " + xmlValue);
          log.error("databaseValue: " + databaseValue);
          throw new RuntimeException(
              "Listas com tamanhos diferentes na validacao do campo nSequencia");
        }
        // teste passou com sucesso
        return;
      }

      // validar caso nao seja sequencia
      if (!xmlValue.equals(databaseValue)) {

        if (xmlValue.isEmpty()
            && StringUtils.equalsAnyIgnoreCase(databaseValue.get(0).toString(), "0.0", "0")
            && StringUtils.equalsAnyIgnoreCase(field.getType(), "Integer", "Decimal")) {
          log.debug("Foi inserido zero no banco e nao foi encontrado valor do XML {}", field);
        } else {
          log.error("Linha com valores diferentes {}", field);
          log.error("xmlValue: " + xmlValue);
          log.error("databaseValue: " + databaseValue);
          throw new RuntimeException("Linha com valores diferentes");
        }
      }

    } catch (Exception e) {
      throw new RuntimeException("Erro ao validar linha: " + field, e);
    }
  }
}
