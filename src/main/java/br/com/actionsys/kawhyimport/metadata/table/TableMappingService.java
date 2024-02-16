package br.com.actionsys.kawhyimport.metadata.table;

import br.com.actionsys.kawhycommons.infra.util.FilesUtil;
import br.com.actionsys.kawhyimport.metadata.field.FieldMapping;
import br.com.actionsys.kawhyimport.metadata.field.FieldMappingService;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class TableMappingService {

  @Value("${file.metadata.table}")
  public Path tableMetadataFile;

  @Autowired private FieldMappingService fieldMappingService;

  public List<TableMapping> read() {

    List<FieldMapping> allFields = fieldMappingService.read();

    try {
      return FilesUtil.readLines(tableMetadataFile).stream()
          .map(csvLine -> build(csvLine, allFields))
          .collect(Collectors.toList());

    } catch (Exception e) {
      throw new RuntimeException(e); // TODO MENSAGEM DE ERRO
    }
  }

  private FieldMapping getIdField(List<FieldMapping> tableFields, String tableId) {
    return tableFields.stream()
        .filter(field -> "id".equalsIgnoreCase(field.getVariable()))
        .findFirst()
        .orElseThrow(
            () -> new RuntimeException("Metadado id da tabela " + tableId + " não encontrado"));
  }

  private FieldMapping getSequenceField(List<FieldMapping> tableFields) {
    return tableFields.stream()
        .filter(field -> "nSequencia".equalsIgnoreCase(field.getVariable()))
        .findFirst()
        .orElse(null);
  }

  private TableMapping build(String csvLine, List<FieldMapping> allFields) {

    try {
      String[] columns = csvLine.split(",");

      TableMapping tableMapping = new TableMapping();
      tableMapping.setTableName(getColumnValue(columns, 0));
      tableMapping.setAPath(getColumnValue(columns, 1));

      if (columns.length > 2) {
        tableMapping.setWhereComplement(getColumnValue(columns, 2));
      }

      tableMapping.setTableId(
          tableMapping.getTableName() + "_" + tableMapping.getWhereComplement());

      List<FieldMapping> tableFields =
          allFields.stream()
              .filter(fieldMapping -> tableMapping.getTableId().equals(fieldMapping.getTableId()))
              .toList();

      tableMapping.setIdField(getIdField(tableFields, tableMapping.getTableId()));
      tableMapping.setSequenceField(getSequenceField(tableFields));
      tableMapping.setFields(tableFields);

      return tableMapping;

    } catch (Exception e) {
      throw new RuntimeException("Erro ao realizar leitura da linha: " + csvLine, e);
    }
  }

  private String getColumnValue(String[] columns, int columnIndex) {
    String value = columns[columnIndex].trim();
    return StringUtils.isBlank(value) ? null : value;
  }
}
