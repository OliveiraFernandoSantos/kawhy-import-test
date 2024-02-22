package br.com.actionsys.kawhyimport;

import br.com.actionsys.kawhycommons.integration.IntegrationItem;
import br.com.actionsys.kawhycommons.integration.IntegrationOrchestrator;
import br.com.actionsys.kawhyimport.command.SqlCommand;
import br.com.actionsys.kawhyimport.command.SqlCommandType;
import br.com.actionsys.kawhyimport.metadata.CommandsXmlService;
import br.com.actionsys.kawhyimport.repository.GenericRepository;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public abstract class ImportOrchestrator extends IntegrationOrchestrator {

  @Autowired CommandsXmlService integrationService;
  @Autowired GenericRepository genericRepository;

  @Override
  public void processDocumentFile(IntegrationItem item) throws Exception {

    List<SqlCommand> sqlCommands = integrationService.generateCommandsFromXml(item);
    sqlCommands.addAll(generateControlCommands(item));

    // TODO IMPLEMENTAR UPDATE OU COMANDOS DE CONTROLE FICARÃO EM OUTRO LUGAR?
    sqlCommands.forEach(
        sqlCommand -> {
          log.trace("Executando comando: " + sqlCommand);

          if (SqlCommandType.INSERT == sqlCommand.getType()) {
            genericRepository.insert(sqlCommand.getTableName(), sqlCommand.getValues());

          } else {
            log.warn(
                "Tipo de comando não suportado, comando={} tabela={}",
                sqlCommand.getType(),
                sqlCommand.getTableName());
          }
        });
  }

  public abstract List<SqlCommand> generateControlCommands(IntegrationItem item) throws Exception;
}
