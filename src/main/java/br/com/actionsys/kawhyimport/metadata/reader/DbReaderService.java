package br.com.actionsys.kawhyimport.metadata.reader;

import static br.com.actionsys.kawhyimport.util.MetadataConstants.GENERATE_ID_NFSE;

import br.com.actionsys.kawhyimport.metadata.field.FieldMapping;
import br.com.actionsys.kawhyimport.metadata.table.TableMapping;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;

@Slf4j
@Service
public class DbReaderService {

  @Autowired private JdbcTemplate jdbcTemplate;
  @Autowired private XmlReaderService xmlReaderService;

  public List<?> getDatabaseValue(TableMapping table, FieldMapping field, Document document) {

    if (GENERATE_ID_NFSE.equals(field.getAPath())) {
      return Collections.emptyList();
    }

    FieldMapping idField = table.getIdField();

    StringBuilder query =
        new StringBuilder("select ")
            .append(idField.getVariable().equalsIgnoreCase(field.getVariable()) ? "distinct " : "")
            .append(field.getColumn())
            .append(" from ")
            .append(field.getTable())
            .append(" where ")
            .append(idField.getColumn())
            .append(" like ?");

    List<?> args =
        Collections.singletonList(xmlReaderService.getValue(table, document, field, 0, 0) + "%");

    if (field.getWhereComplement() != null) {
      query.append(" and ").append(field.getWhereComplement());
    }

    if (table.getSequenceField() != null && !idField.equals(field)) {
      query.append(" order by ").append(table.getSequenceField().getColumn());
    }

    List<?> listValuesDb =
        jdbcTemplate.queryForList(query.toString(), Object.class, args.toArray());
    if (StringUtils.containsAnyIgnoreCase(field.getType(), "String", "Date", "Time")) {

      return listValuesDb.stream()
          .map(
              value -> {
                if (value == null) {
                  return null;
                }
                if (field.getRegex() != null) {
                  return value.toString().trim().replaceAll(field.getRegex(), "");
                }
                return value.toString().trim();
              })
          .collect(Collectors.toList());

    } else if (StringUtils.containsAnyIgnoreCase(field.getType(), "Decimal", "Integer")) {
      return listValuesDb.stream()
          .map(o -> o == null ? null : BigDecimal.valueOf(Double.parseDouble(o.toString())))
          .collect(Collectors.toList());
    } else if (StringUtils.containsAnyIgnoreCase(field.getType(), "Blob")) {
      return listValuesDb.stream()
          .map(
              value -> {
                if (value == null) {
                  return null;
                }
                return new String((byte[]) value);
              })
          .collect(Collectors.toList());
    }
    return listValuesDb;
  }
}
