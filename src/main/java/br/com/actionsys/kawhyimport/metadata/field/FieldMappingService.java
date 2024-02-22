package br.com.actionsys.kawhyimport.metadata.field;

import br.com.actionsys.kawhycommons.infra.util.FilesUtil;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class FieldMappingService {

  @Value("${file.metadata.field}")
  public String fieldMetadataFile;

  public List<FieldMapping> read() {

    try {
      return FilesUtil.readLines(Path.of(fieldMetadataFile)).stream()
          .map(this::build)
          .filter(field -> !isIncomplete(field))
          .collect(Collectors.toList());

    } catch (Exception e) {
      // TODO PARAR PROCESSAMENTO?
      throw new RuntimeException("Erro ao ler arquivos de metadados de colunas " + fieldMetadataFile);
    }
  }

  private boolean isIncomplete(FieldMapping fieldMapping) {

    return fieldMapping.getAPath() == null
        || "-".equals(fieldMapping.getAPath())
        || fieldMapping.getTable() == null
        || fieldMapping.getColumn() == null;
  }

  private FieldMapping build(String csvLine) {

    try {
      String[] columns = csvLine.split(",", -1);

      FieldMapping fieldMapping = new FieldMapping();
      fieldMapping.setTable(getColumnValue(columns, 0));
      fieldMapping.setColumn(getColumnValue(columns, 1));
      fieldMapping.setVariable(getColumnValue(columns, 2));
      fieldMapping.setAPath(getColumnValue(columns, 3));
      fieldMapping.setType(getColumnValue(columns, 4));
      fieldMapping.setWhereComplement(getColumnValue(columns, 5));
      fieldMapping.setRegex(getColumnValue(columns, 6));

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
