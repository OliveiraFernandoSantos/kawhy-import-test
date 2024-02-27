package br.com.actionsys.kawhyimport.metadata;

import br.com.actionsys.kawhycommons.infra.function.ChaveAcessoNfseUtil;
import br.com.actionsys.kawhycommons.infra.function.NumeroNfseUtil;
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
public class XmlReaderService {

  public Object getValue(
      TableMapping table, Document document, FieldMapping field, int sequence, int parentSequence) {

    try {
      String aPath = field.getAPath();

      if (aPath == null) {
        return null;
      }

      if (aPath.equals(MetadataConstants.SEQUENCE)) {
        return sequence;
      }

      if (aPath.equals(MetadataConstants.PARENT_SEQUENCE)) {
        return parentSequence;
      }

      if (aPath.equals(MetadataConstants.COMP_WHERE)) {
        return StringUtils.substringBetween(field.getWhereComplement(), "'");
      }

      if (MetadataConstants.GENERATE_ID_NFSE.equals(field.getAPath())) {
        String cnpjPrest = APathUtil.getString(document, "IntegracaoMidas/DadosPrestador/Cnpj");
        String data = APathUtil.getString(document, "IntegracaoMidas/DtEmissao");
        String numNf = APathUtil.getString(document, "IntegracaoMidas/Numero");

        return Collections.singletonList(
            ChaveAcessoNfseUtil.generateNfseId(data, cnpjPrest, numNf));
      }

      if (MetadataConstants.GENERATE_NUMBER_NFSE.equals(field.getAPath())) {
        // Quando receber o NNF é feita a validação e alteração do número conforme regra no método
        // generateNfseNumber
        String numNf = APathUtil.getString(document, "IntegracaoMidas/Numero");

        return Collections.singletonList(
            BigDecimal.valueOf(Double.parseDouble(NumeroNfseUtil.generateNfseNumber(numNf, ""))));
      }

      if (StringUtils.isNoneBlank(table.getTableAPath())) {
        aPath = insertIndex(aPath, table.getTableAPath(), sequence);
      }

      if (parentSequence > 0 && StringUtils.isNoneBlank(table.getParentAPath())) {
        aPath = insertIndex(aPath, table.getParentAPath(), parentSequence);
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

  public int count(Document document, TableMapping tableMapping, int parentSequence) {

    String tableAPath = tableMapping.getTableAPath();

    if (tableAPath == null) {
      return 0;
    }

    if (parentSequence > 0 && StringUtils.isNoneBlank(tableMapping.getParentAPath())) {
      tableAPath = insertIndex(tableAPath, tableMapping.getParentAPath(), parentSequence);
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
