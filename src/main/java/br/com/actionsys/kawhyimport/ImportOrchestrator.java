package br.com.actionsys.kawhyimport;

import br.com.actionsys.kawhycommons.integration.IntegrationItem;
import br.com.actionsys.kawhycommons.integration.IntegrationOrchestrator;
import br.com.actionsys.kawhyimport.command.SqlCommand;
import br.com.actionsys.kawhyimport.command.SqlCommandType;
import br.com.actionsys.kawhyimport.metadata.ReaderXmlService;
import br.com.actionsys.kawhyimport.metadata.field.FieldMapping;
import br.com.actionsys.kawhyimport.metadata.table.TableMapping;
import br.com.actionsys.kawhyimport.metadata.table.TableMappingService;
import br.com.actionsys.kawhyimport.repository.GenericRepository;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public abstract class ImportOrchestrator extends IntegrationOrchestrator {

  @Autowired TableMappingService tableMappingService;
  @Autowired ReaderXmlService readerXmlService;
  @Autowired GenericRepository genericRepository;

  @Override
  public void processDocumentFile(IntegrationItem item) throws Exception {

    log.trace("Processando documento " + item);
    List<TableMapping> tableMappings = tableMappingService.readAndChain();

    List<SqlCommand> sqlCommands = buildCommandsFromTables(tableMappings, item);
    sqlCommands.forEach(
        sqlCommand -> {
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

  private boolean commandIsValid(TableMapping tableMapping, SqlCommand sqlCommand) {

    FieldMapping idField = tableMapping.getIdField();

    FieldMapping sequenceField =
        tableMapping.getSequenceField() == null
            ? new FieldMapping()
            : tableMapping.getSequenceField();

    for (Map.Entry<String, Object> commandValues : sqlCommand.getValues().entrySet()) {
      // TODO VERIFICAR SE É PRECISO INCLUIR CAMPOS DE FUNÇÃO NA EXCLUSÃO
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

  public List<SqlCommand> buildCommandsFromTables(
      List<TableMapping> tableMappings, IntegrationItem item) {

    ArrayList<SqlCommand> sqlCommands = new ArrayList<>();

    for (TableMapping tableMapping : tableMappings) {

      List<SqlCommand> tableCommands = buildCommandsFromTable(tableMapping, item, null);

      tableCommands.removeIf(sqlCommand -> !commandIsValid(tableMapping, sqlCommand));

      sqlCommands.addAll(tableCommands);
    }

    return sqlCommands;
  }

  private List<SqlCommand> buildCommandsFromTable(
      TableMapping tableMapping, IntegrationItem item, Integer parentSequence) {

    log.debug("Criando comandos para a tabela {} parentSequence {}", tableMapping, parentSequence);
    try {
      List<SqlCommand> tableCommands = new ArrayList<>();

      if (item.getId() == null) {
        item.setId(
            (String)
                readerXmlService.getValue(
                    tableMapping, item.getDocument(), tableMapping.getIdField(), 1, 0));
      }

      // TODO PREENCHER APATH DAS TABELAS
      int tableRecors = readerXmlService.count(item.getDocument(), tableMapping, parentSequence);

      for (Integer sequence = 1; sequence <= tableRecors; sequence++) {
        {
          Map<String, Object> values = new HashMap<>();

          for (TableMapping childTable : tableMapping.getChildTables()) {
            tableCommands.addAll(buildCommandsFromTable(childTable, item, sequence));
          }

          for (FieldMapping field : tableMapping.getFields()) {

            log.debug("Processando coluna: " + field.getColumn());
            log.trace("Processando coluna: " + field);

            Object value =
                switch (field.getAPath()) {
                  case "${sequence}" -> sequence;
                  case "${parentSequence}" -> parentSequence;
                  case "${compWhere}" ->
                      StringUtils.substringBetween(field.getWhereComplement(), "'");
                  default ->
                      readerXmlService.getValue(
                          tableMapping, item.getDocument(), field, sequence, parentSequence);
                };

            if (value != null) {
              values.put(field.getColumn(), value);
            } else {
              log.debug("Valor não encontrado para o campo: " + field);
            }
          }

          SqlCommand sqlCommand = new SqlCommand();
          sqlCommand.setType(SqlCommandType.INSERT);
          sqlCommand.setTableName(tableMapping.getTableName());
          sqlCommand.setValues(values);
          tableCommands.add(sqlCommand);
        }
      }

      return tableCommands;

    } catch (Exception e) {
      throw new RuntimeException(e); // TODO MENSAGEM DE ERRO
    }
  }
}
