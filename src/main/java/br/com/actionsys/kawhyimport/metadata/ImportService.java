package br.com.actionsys.kawhyimport.metadata;

import br.com.actionsys.kawhycommons.infra.util.DateUtil;
import br.com.actionsys.kawhycommons.integration.IntegrationItem;
import br.com.actionsys.kawhycommons.types.KawhyType;
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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

@Slf4j
@Service
public class ImportService {

    @Autowired
    TableMappingService tableMappingService;

    @Autowired
    XmlReaderService xmlReaderService;

    @Autowired
    GenericRepository genericRepository;

    public void process(IntegrationItem item, KawhyType kawhyType) {

        try {
            Map<String, String> tempVariables = item.getTempVariables();

            // AUDIT_SERVICE_NAME
            tempVariables.put(MetadataFunctions.AUDIT_SERVICE_NAME, kawhyType.getServiceName());

            // AUDIT_USER
            tempVariables.put(MetadataFunctions.AUDIT_USER, kawhyType.getServiceName());

            // AUDIT_HOST
            try {
                tempVariables.put(MetadataFunctions.AUDIT_HOST, InetAddress.getLocalHost().getHostAddress());
            } catch (UnknownHostException e) {
                tempVariables.put(MetadataFunctions.AUDIT_HOST, "");
            }

            // AUDIT_DATE e AUDIT_TIME
            Date date = new Date();
            tempVariables.put(MetadataFunctions.AUDIT_DATE, DateUtil.formatDateToDb(date));
            tempVariables.put(MetadataFunctions.AUDIT_TIME, DateUtil.formatTimeToDb(date));

        } catch (Exception e) {
            log.warn("Erro ao preencher variaveis", e);
        }

        process(item);
    }

    public void process(IntegrationItem item) {

        try {
            List<TableMapping> tableMappings = tableMappingService.read();

            List<SqlCommand> commands = processTables(tableMappings, item);

            commands.forEach(sqlCommand -> {
                log.debug("Executando comando: " + sqlCommand);
                genericRepository.insert(sqlCommand.getTableName(), sqlCommand.getValues());
            });
        } catch (Exception e) {
            throw new RuntimeException("Erro ao processar arquivo " + item.getFile().getAbsolutePath(), e);
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

        //TODO remover essa validação?!
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
