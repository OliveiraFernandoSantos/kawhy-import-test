package br.com.actionsys.kawhyimport;

import br.com.actionsys.kawhycommons.integration.IntegrationItem;
import br.com.actionsys.kawhycommons.integration.IntegrationOrchestrator;
import br.com.actionsys.kawhyimport.command.SqlCommand;
import br.com.actionsys.kawhyimport.metadata.CommandsXmlService;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

@Slf4j
public abstract class MetadataOrchestrator extends IntegrationOrchestrator {

  @Value("${file.metadata.table}")
  public String tableMetadataFile;

  @Value("${file.metadata.field}")
  public String fieldMetadataFile;

  @Autowired CommandsXmlService commandsXmlService;

  @Override
  public void processDocumentFile(IntegrationItem item) throws Exception {

    processDocumentFile(item, Paths.get(tableMetadataFile), Paths.get(fieldMetadataFile));
  }

  public void processDocumentFile(
      IntegrationItem item, Path tableMetadataFile, Path fieldMetadataFile) throws Exception {

    // gerar comandos a partir de metadados
    List<SqlCommand> sqlCommands =
        commandsXmlService.generateCommandsFromXml(item, tableMetadataFile, fieldMetadataFile);

    // gerar comandos para tabelas de controle
    sqlCommands.addAll(generateControlCommands(item));

    // executar comandos no banco
    commandsXmlService.executeCommands(sqlCommands);
  }

  public abstract List<SqlCommand> generateControlCommands(IntegrationItem item) throws Exception;
}
