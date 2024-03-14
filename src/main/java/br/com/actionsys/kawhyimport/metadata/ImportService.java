package br.com.actionsys.kawhyimport.metadata;

import br.com.actionsys.kawhycommons.integration.IntegrationItem;
import br.com.actionsys.kawhyimport.command.SqlCommand;
import br.com.actionsys.kawhyimport.metadata.field.FieldMapping;
import br.com.actionsys.kawhyimport.metadata.reader.XmlReaderService;
import br.com.actionsys.kawhyimport.metadata.table.TableMapping;
import br.com.actionsys.kawhyimport.metadata.table.TableMappingService;
import br.com.actionsys.kawhyimport.repository.GenericRepository;
import br.com.actionsys.kawhyimport.util.MetadataFunctions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class ImportService {

    @Autowired
    TableMappingService tableMappingService;

    @Autowired
    XmlReaderService xmlReaderService;

    @Autowired
    GenericRepository genericRepository;

    public void process(IntegrationItem item, Path metadataFile) {

        try {
            List<TableMapping> tableMappings = tableMappingService.read(metadataFile);

            item.setId(getDocumentId(item, tableMappings));

            List<SqlCommand> commands = processTables(tableMappings, item);

            commands.forEach(sqlCommand -> {
                log.debug("Executando comando: " + sqlCommand);
                genericRepository.insert(sqlCommand.getTableName(), sqlCommand.getValues());
            });
        } catch (Exception e) {
            throw new RuntimeException("Erro ao processar arquivo " + item.getFile().getAbsolutePath(), e);
        }
    }

    private String getDocumentId(IntegrationItem item, List<TableMapping> tableMappings) {

        try {
            // Para preencher o id pelo menos uma das colunas de id deve ter o APath preenchido
            TableMapping table = tableMappings.stream()
                    .filter(t -> !StringUtils.startsWith(t.getIdField().getAPath(), MetadataFunctions.FUNCTION_PREFIX))
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("Não foi encontrado uma coluna do tipo id com o APath preenchido"));

            return (String) xmlReaderService.getValue(table, item, table.getIdField(), 1, 0);

        } catch (Exception e) {
            throw new RuntimeException("Erro ao recuperar chave de acesso", e);
        }
    }

    private List<SqlCommand> processTables(List<TableMapping> tables, IntegrationItem item) {

        ArrayList<SqlCommand> commands = new ArrayList<>();

        for (TableMapping table : tables) {

            log.debug("Processando tabela: " + table);

            List<SqlCommand> tableCommands = table.getParentAPath() == null
                    ? processTable(table, item, 0)
                    : processTableWithParent(table, item);

            commands.addAll(tableCommands);
        }

        return commands;
    }

    private List<SqlCommand> processTable(TableMapping table, IntegrationItem item, int parentSequence) {

        List<SqlCommand> commands = new ArrayList<>();

        if (item.getId() == null) {
            item.setId((String) xmlReaderService.getValue(table, item, table.getIdField(), 1, 0));
        }

        int countItens = xmlReaderService.count(item.getDocument(), table, parentSequence);

        for (int sequence = 1; sequence <= countItens; sequence++) {

            Map<String, Object> values = new HashMap<>();

            for (FieldMapping field : table.getFields()) {

                log.debug("Processando coluna: " + field);

                Object value = xmlReaderService.getValue(table, item, field, sequence, parentSequence);

                if (value != null) {
                    values.put(field.getColumn(), value);
                } else {
                    log.debug("Valor não encontrado para o campo: " + field);
                }
            }

            SqlCommand sqlCommand = new SqlCommand();
            sqlCommand.setTableName(table.getTableName());
            sqlCommand.setValues(values);
            commands.add(sqlCommand);
        }

        commands.removeIf(sqlCommand -> !commandIsValid(table, sqlCommand));

        return commands;
    }

    private List<SqlCommand> processTableWithParent(TableMapping tableMapping, IntegrationItem item) {

        List<SqlCommand> commands = new ArrayList<>();

        int countParentItens = xmlReaderService.count(item.getDocument(), tableMapping.getParentAPath());

        log.debug("{} registros encontrados para o parentAPath {}", countParentItens, tableMapping.getParentAPath());

        for (int parentSequence = 1; parentSequence <= countParentItens; parentSequence++) {
            commands.addAll(processTable(tableMapping, item, parentSequence));
        }

        return commands;
    }

    private boolean commandIsValid(TableMapping table, SqlCommand command) {

        FieldMapping idField = table.getIdField();

        FieldMapping sequenceField = table.getSequenceField() == null ? new FieldMapping() : table.getSequenceField();

        for (Map.Entry<String, Object> commandValues : command.getValues().entrySet()) {

            // nao utiliza id e sequencia na validacao
            if (!StringUtils.equalsAnyIgnoreCase(commandValues.getKey(), idField.getColumn(), sequenceField.getColumn())) {

                // caso possua algum valor preenchido retorna true
                if (commandValues.getValue() != null) {
                    return true;
                }
            }
        }

        return false;
    }
}
