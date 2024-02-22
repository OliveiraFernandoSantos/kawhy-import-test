package br.com.actionsys.kawhyimport.metadata;

import br.com.actionsys.kawhyimport.metadata.field.FieldMapping;
import br.com.actionsys.kawhyimport.metadata.table.TableMapping;
import br.com.actionsys.kawhyimport.util.APathUtil;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.xml.xpath.XPathExpressionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;

@Slf4j
@Service
public class ReaderXmlService {

  public Object getValue(
      TableMapping table, Document document, FieldMapping field, int index, int parentIndex) {

    try {
      String aPath = field.getAPath();

      if (aPath == null) {
        return null;
      }

      if (StringUtils.isNoneBlank(table.getTableAPath())) {
        aPath = insertIndex(aPath, table.getTableAPath(), index);
      }

      if (parentIndex > 0 && StringUtils.isNoneBlank(table.getParentAPath())) {
        aPath = insertIndex(aPath, table.getParentAPath(), parentIndex);
      }

      return formatValue(APathUtil.getString(document, aPath), field);

    } catch (Exception e) {
      throw new RuntimeException("Erro ao recuperar valor do campo: " + field, e);
    }
  }

  private String insertIndex(String aPath, String parentAPath, Integer index) {
    return StringUtils.replace(aPath, parentAPath, parentAPath + "[" + index + "]");
  }

  public int count(Document document, String aPath) {
    try {
      return APathUtil.count(document, aPath);
    } catch (Exception e) {
      throw new RuntimeException("Erro ao realizar contagem de registros", e);
    }
  }

  public int count(Document document, TableMapping tableMapping, int parentIndex) {

    String tableAPath = tableMapping.getTableAPath();

    if (tableAPath == null) {
      return 0;
    }

    if (parentIndex > 0 && StringUtils.isNoneBlank(tableMapping.getParentAPath())) {
      tableAPath = insertIndex(tableAPath, tableMapping.getParentAPath(), parentIndex);
    }

    try {
      return APathUtil.count(document, tableAPath);
    } catch (Exception e) {
      throw new RuntimeException(
          "Erro ao realizar contagem de registros para a tabela: " + tableMapping, e);
    }
  }

  public List<?> getListOfValues(Document document, FieldMapping field)
      throws XPathExpressionException {

    if (field.getAPath() == null) {
      return Collections.emptyList();
    }

    List<String> valueXmlStr = APathUtil.getStringList(document, field.getAPath());

    return valueXmlStr.stream()
        .map(value -> formatValue(value, field))
        .collect(Collectors.toList());
  }

  private Object formatValue(String value, FieldMapping field) {

    if (value == null || value.trim().isEmpty()) {
      return null;

    } else if (field.getType().equals("Date")) {
      return value.substring(0, 10);

    } else if (field.getType().equals("Time")) {
      return value.substring(11, 19);

    } else if (field.getType().equals("Decimal")) {
      return BigDecimal.valueOf(Double.parseDouble(value));

    } else if (field.getType().equals("Integer")) {
      return new BigInteger(value);

    } else {

      if (field.getRegex() != null) { // TODO REVISAR REGEX
        value = value.trim().replaceAll(field.getRegex(), "");
      }

      if (field.getMaxSize() != null) {
        try {
          value = StringUtils.substring(value, 0, Integer.parseInt(field.getMaxSize()));
        } catch (Exception e) {
          throw new RuntimeException("Erro processar maxSize", e);
        }
      }

      return value.trim();
    }
  }
}
