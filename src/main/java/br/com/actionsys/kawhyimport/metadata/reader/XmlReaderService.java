package br.com.actionsys.kawhyimport.metadata.reader;

import br.com.actionsys.kawhycommons.infra.function.ChaveAcessoNfseUtil;
import br.com.actionsys.kawhycommons.infra.function.NumeroNfseUtil;
import br.com.actionsys.kawhycommons.integration.IntegrationItem;
import br.com.actionsys.kawhyimport.metadata.field.FieldMapping;
import br.com.actionsys.kawhyimport.metadata.field.FieldType;
import br.com.actionsys.kawhyimport.metadata.table.TableMapping;
import br.com.actionsys.kawhyimport.util.APathUtil;
import br.com.actionsys.kawhyimport.util.MetadataFunctions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;

import javax.xml.xpath.XPathExpressionException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class XmlReaderService {

    public Object getValue(TableMapping table, IntegrationItem item, FieldMapping field, int sequence, int parentSequence) {

        try {
            String aPath = field.getAPath();

            if (aPath == null) {
                return null;
            }

            if (aPath.equals(MetadataFunctions.DOCUMENT_ID)) {
                return item.getId();
            }

            if (aPath.equals(MetadataFunctions.SEQUENCE)) {
                return sequence;
            }

            if (aPath.equals(MetadataFunctions.PARENT_SEQUENCE)) {
                return parentSequence;
            }

            if (aPath.equals(MetadataFunctions.COMP_WHERE)) {
                return StringUtils.substringBetween(field.getWhereComplement(), "'");
            }

            if (MetadataFunctions.GENERATE_ID_NFSE.equals(field.getAPath())) {
                String cnpjPrest = APathUtil.getString(item.getDocument(), "IntegracaoMidas/DadosPrestador/Cnpj");
                String data = APathUtil.getString(item.getDocument(), "IntegracaoMidas/DtEmissao");
                String numNf = APathUtil.getString(item.getDocument(), "IntegracaoMidas/Numero");

                return Collections.singletonList(ChaveAcessoNfseUtil.generateNfseId(data, cnpjPrest, numNf));
            }

            if (MetadataFunctions.GENERATE_NUMBER_NFSE.equals(field.getAPath())) {
                // Quando receber o NNF é feita a validação e alteração do número conforme regra no método
                // generateNfseNumber
                String numNf = APathUtil.getString(item.getDocument(), "IntegracaoMidas/Numero");

                return Collections.singletonList(BigDecimal.valueOf(Double.parseDouble(NumeroNfseUtil.generateNfseNumber(numNf, ""))));
            }

            if (StringUtils.isNoneBlank(table.getTableAPath())) {
                aPath = insertIndex(aPath, table.getTableAPath(), sequence);
            }

            if (parentSequence > 0 && StringUtils.isNoneBlank(table.getParentAPath())) {
                aPath = insertIndex(aPath, table.getParentAPath(), parentSequence);
            }

            return formatValue(APathUtil.getString(item.getDocument(), aPath), field);

        } catch (Exception e) {
            throw new RuntimeException("Erro ao recuperar valor do campo: " + field, e);
        }
    }

    private String insertIndex(String aPath, String parentAPath, int index) {
        if (index < 1) {
            index = 1;
        }
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
            throw new RuntimeException("Erro ao realizar contagem de registros para a tabela: " + tableMapping, e);
        }
    }

    public List<?> getListOfValues(Document document, FieldMapping field) throws XPathExpressionException {

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

        } else if (FieldType.DATE.name().equals(field.getType().toUpperCase())) {
            return value.substring(0, 10);

        } else if (FieldType.TIME.name().equals(field.getType().toUpperCase())) {
            return value.substring(11, 19);

        } else if (FieldType.DECIMAL.name().equals(field.getType().toUpperCase())) {
            return BigDecimal.valueOf(Double.parseDouble(value));

        } else if (FieldType.INTEGER.name().equals(field.getType().toUpperCase())) {
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
