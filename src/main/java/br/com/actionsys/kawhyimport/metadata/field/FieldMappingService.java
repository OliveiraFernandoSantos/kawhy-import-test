package br.com.actionsys.kawhyimport.metadata.field;

import br.com.actionsys.kawhycommons.infra.util.FilesUtil;
import br.com.actionsys.kawhyimport.util.ImportConstants;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class FieldMappingService {

    @Value("${arquivo.metadados}")
    public Resource arquivoMetadados;

    public List<FieldMapping> read() {

        Stream<String> lines = FilesUtil.readLines(arquivoMetadados);

        return lines
                .skip(1) // remove linha de cabecalho
                .map(this::build)
                .filter(field -> !isIncomplete(field))
                .collect(Collectors.toList());

    }

    private boolean isIncomplete(FieldMapping fieldMapping) {

        return fieldMapping.getAPath() == null
                || "-".equals(fieldMapping.getAPath())
                || fieldMapping.getTable() == null
                || fieldMapping.getColumn() == null;
    }

    private FieldMapping build(String csvLine) {

        try {
            String[] columns = csvLine.split(ImportConstants.CSV_SEPARATOR, -1);

            FieldMapping fieldMapping = new FieldMapping();
            fieldMapping.setTable(getColumnValue(columns, 0));
            fieldMapping.setColumn(getColumnValue(columns, 1));
            fieldMapping.setVariable(getColumnValue(columns, 2));
            fieldMapping.setAPath(getColumnValue(columns, 3));
            fieldMapping.setType(getColumnValue(columns, 4));
            fieldMapping.setWhereComplement(getColumnValue(columns, 5));
            fieldMapping.setDbColumnSize(getColumnValue(columns, 6));
            fieldMapping.setTableAPath(getColumnValue(columns, 7));
            fieldMapping.setParentAPath(getColumnValue(columns, 8));

            fieldMapping.setTableId(
                    fieldMapping.getWhereComplement() == null
                            ? fieldMapping.getTable()
                            : fieldMapping.getTable() + "_" + fieldMapping.getWhereComplement());

            return fieldMapping;

        } catch (Exception e) {
            throw new RuntimeException("Erro ao realizar leitura da linha: " + csvLine, e);
        }
    }

    private String getColumnValue(String[] columns, int columnIndex) {
        String value = columns[columnIndex].trim();
        return StringUtils.isBlank(value) ? null : value;
    }
}
