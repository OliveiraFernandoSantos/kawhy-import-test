package br.com.actionsys.kawhyimport.metadata.table;

import br.com.actionsys.kawhycommons.infra.util.FilesUtil;
import br.com.actionsys.kawhyimport.metadata.field.FieldMapping;
import br.com.actionsys.kawhyimport.metadata.field.FieldMappingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class TableMappingService {

    @Autowired
    private FieldMappingService fieldMappingService;

    public List<TableMapping> read() {

        try {
            List<FieldMapping> allFields = fieldMappingService.read();

            return allFields.stream()
                    .collect(Collectors.groupingBy(FieldMapping::getTableId))
                    .values().stream().map(this::build)
                    .collect(Collectors.toList());

        } catch (Exception e) {
            // TODO PARAR PROCESSAMENTO?
            throw new RuntimeException("Erro ao ler arquivos de metadados ", e);
        }
    }

    private FieldMapping getIdField(List<FieldMapping> tableFields, String tableId) {

        return tableFields.stream()
                .filter(field -> "id".equalsIgnoreCase(field.getVariable()))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Metadado id da tabela " + tableId + " não encontrado"));
    }

    private FieldMapping getSequenceField(List<FieldMapping> tableFields) {

        return tableFields.stream()
                .filter(field -> "nSequencia".equalsIgnoreCase(field.getVariable()))
                .findFirst()
                .orElse(null);
    }

    private TableMapping build(List<FieldMapping> tableFields) {

        String tableId = tableFields.stream().findFirst().get().getTableId();

        FieldMapping idField = getIdField(tableFields, tableId);

        TableMapping table = new TableMapping();
        table.setTableId(tableId);
        table.setTableName(idField.getTable());
        table.setWhereComplement(idField.getWhereComplement());
        table.setTableAPath(idField.getTableAPath());
        table.setParentAPath(idField.getParentAPath());
        table.setIdField(idField);
        table.setSequenceField(getSequenceField(tableFields));
        table.setFields(tableFields);

        return table;
    }

}
