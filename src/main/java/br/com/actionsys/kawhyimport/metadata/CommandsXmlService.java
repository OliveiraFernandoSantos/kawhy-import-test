package br.com.actionsys.kawhyimport.metadata;

import br.com.actionsys.kawhycommons.integration.IntegrationItem;
import br.com.actionsys.kawhyimport.command.SqlCommand;
import br.com.actionsys.kawhyimport.command.SqlCommandType;
import br.com.actionsys.kawhyimport.metadata.field.FieldMapping;
import br.com.actionsys.kawhyimport.metadata.table.TableMapping;
import br.com.actionsys.kawhyimport.metadata.table.TableMappingService;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class CommandsXmlService {

  @Autowired TableMappingService tableMappingService;
  @Autowired ReaderXmlService readerXmlService;

  public List<SqlCommand> generateCommandsFromXml(IntegrationItem item) {

    log.trace("Gerando comandos a partir do xml para o documento " + item.getId());

    List<TableMapping> tableMappings = tableMappingService.read();

    return generateCommands(tableMappings, item);
  }

  private List<SqlCommand> generateCommands(List<TableMapping> tables, IntegrationItem item) {

    ArrayList<SqlCommand> commands = new ArrayList<>();

    for (TableMapping table : tables) {

      log.debug("Processando tabela: " + table.getTableName());
      log.trace(table.toString());

      commands.addAll(
          "-".equals(table.getParentAPath())
              ? generateCommands(table, item, 0)
              : generateCommandsWithParent(table, item));
    }

    if (log.isTraceEnabled()) {
      commands.forEach(sqlCommand -> log.info(sqlCommand.toString()));
    }

    return commands;
  }

  private List<SqlCommand> generateCommands(
      TableMapping table, IntegrationItem item, int parentSequence) {

    List<SqlCommand> commands = new ArrayList<>();

    if (item.getId() == null) {
      item.setId(
          (String) readerXmlService.getValue(table, item.getDocument(), table.getIdField(), 1, 0));
    }

    int countItens = readerXmlService.count(item.getDocument(), table, parentSequence);

    for (int sequence = 1; sequence <= countItens; sequence++) {

      Map<String, Object> values = new HashMap<>();

      for (FieldMapping field : table.getFields()) {

        log.debug("Processando coluna: " + field.getColumn());
        log.trace(field.toString());

        Object value =
            switch (field.getAPath()) {
              case "${sequence}" -> sequence;
              case "${parentSequence}" -> parentSequence;
              case "${compWhere}" -> StringUtils.substringBetween(field.getWhereComplement(), "'");
              default ->
                  readerXmlService.getValue(
                      table, item.getDocument(), field, sequence, parentSequence);
            };

        if (value != null) {
          values.put(field.getColumn(), value);
        } else {
          log.debug("Valor não encontrado para o campo: " + field);
        }
      }

      SqlCommand sqlCommand = new SqlCommand();
      sqlCommand.setType(SqlCommandType.INSERT);
      sqlCommand.setTableName(table.getTableName());
      sqlCommand.setValues(values);
      commands.add(sqlCommand);
    }

    commands.removeIf(sqlCommand -> !commandIsValid(table, sqlCommand));

    return commands;
  }

  private List<SqlCommand> generateCommandsWithParent(
      TableMapping tableMapping, IntegrationItem item) {

    List<SqlCommand> commands = new ArrayList<>();

    int countParentItens =
        readerXmlService.count(item.getDocument(), tableMapping.getParentAPath());

    log.trace(
        "{} registros encontrados para o parentAPath {}",
        countParentItens,
        tableMapping.getParentAPath());

    for (int parentSequence = 1; parentSequence <= countParentItens; parentSequence++) {
      commands.addAll(generateCommands(tableMapping, item, parentSequence));
    }
    return commands;
  }

  private boolean commandIsValid(TableMapping table, SqlCommand command) {

    FieldMapping idField = table.getIdField();

    FieldMapping sequenceField =
        table.getSequenceField() == null ? new FieldMapping() : table.getSequenceField();

    for (Map.Entry<String, Object> commandValues : command.getValues().entrySet()) {

      // nao utiliza id e sequencia na validacao
      if (!StringUtils.equalsAnyIgnoreCase(
          commandValues.getKey(), idField.getColumn(), sequenceField.getColumn())) {

        // caso possua algum valor preenchido retorna true
        if (commandValues.getValue() != null) {
          return true;
        }
      }
    }

    return false;
  }
}
