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

    List<TableMapping> tableMappings = tableMappingService.read();

    List<SqlCommand> sqlCommands = generateCommands(tableMappings, item);
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

  public List<SqlCommand> generateCommands(List<TableMapping> tableMappings, IntegrationItem item) {

    ArrayList<SqlCommand> sqlCommands = new ArrayList<>();

    for (TableMapping tableMapping : tableMappings) {

      log.debug("Processando tabela: " + tableMapping);

      List<SqlCommand> tableCommands = generateCommands(tableMapping, item);

      tableCommands.removeIf(sqlCommand -> !commandIsValid(tableMapping, sqlCommand));

      sqlCommands.addAll(tableCommands);
    }

    return sqlCommands;
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

  private List<SqlCommand> generateCommands(TableMapping tableMapping, IntegrationItem item) {

    try {
      List<SqlCommand> tableCommands = new ArrayList<>();

      if (item.getId() == null) {
        item.setId(
            (String) readerXmlService.getValue(tableMapping, item.getDocument(), tableMapping.getIdField(), 1));
      }

      int sequence = 1; // TODO VERIFICAR COMO VAI SER O SEQUENCE

      // TODO PREENCHER APATH DAS TABELAS
      int tableRecors = readerXmlService.count(item.getDocument(), tableMapping);

      for (int index = 1; index <= tableRecors; index++) {

        Map<String, Object> values = new HashMap<>();

        for (FieldMapping field : tableMapping.getFields()) {

          log.debug("Processando campo: " + field);

          Object value;
          if (field.getAPath().equals("${sequence}")) {
            value = sequence;
          } else {
            value = readerXmlService.getValue(tableMapping, item.getDocument(), field, index);
          }

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

        sequence++;
      }

      return tableCommands;

    } catch (Exception e) {
      throw new RuntimeException(e); // TODO MENSAGEM DE ERRO
    }
  }
}
