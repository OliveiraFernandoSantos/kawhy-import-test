package br.com.actionsys.kawhyimport.metadata.reader;

import br.com.actionsys.kawhycommons.infra.function.NumeroNfseFunction;
import br.com.actionsys.kawhycommons.integration.IntegrationContext;
import br.com.actionsys.kawhyimport.metadata.field.FieldMapping;
import br.com.actionsys.kawhyimport.metadata.field.FieldType;
import br.com.actionsys.kawhyimport.metadata.table.TableMapping;
import br.com.actionsys.kawhyimport.util.APathUtil;
import br.com.actionsys.kawhyimport.util.ImportConstants;
import br.com.actionsys.kawhyimport.util.MetadataFunctions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Autowired
    private NumeroNfseFunction numeroNfseFunction;

    public Object getValue(TableMapping table, IntegrationContext item, FieldMapping field, int sequence, int parentSequence) {

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
                return StringUtils.substringBetween(field.getWhereComplement(), ImportConstants.VALUE_SEPARATOR);
            }

            if (MetadataFunctions.GENERATE_NUMBER_NFSE.equals(aPath)) {
                // Quando receber o NNF é feita a validação e alteração do número conforme regra no método
                // generateNfseNumber
                String numNf = APathUtil.getString(item.getDocument(), "IntegracaoMidas/Numero");
                String dtEmissao = APathUtil.getString(item.getDocument(), "IntegracaoMidas/DtEmissao");

                return Collections.singletonList(BigDecimal.valueOf(Double.parseDouble(numeroNfseFunction.generateNfseNumber(numNf, dtEmissao))));
            }

            if (MetadataFunctions.AUDIT_HOST.equals(aPath)) {
                return item.getTempVariables().get(MetadataFunctions.AUDIT_HOST);
            }

            if (MetadataFunctions.AUDIT_DATE.equals(aPath)) {
                return item.getTempVariables().get(MetadataFunctions.AUDIT_DATE);
            }

            if (MetadataFunctions.AUDIT_TIME.equals(aPath)) {
                return item.getTempVariables().get(MetadataFunctions.AUDIT_TIME);
            }

            if (MetadataFunctions.AUDIT_SERVICE_NAME.equals(aPath)) {
                return item.getTempVariables().get(MetadataFunctions.AUDIT_SERVICE_NAME);
            }

            if (MetadataFunctions.AUDIT_USER.equals(aPath)) {
                return item.getTempVariables().get(MetadataFunctions.AUDIT_USER);
            }

            // Caso nao encontre nenhuma funcao e tenha o prefixo foi cadastrado um valor fixo
            // exemplo: ${'rem'}
            if (StringUtils.contains(aPath, MetadataFunctions.FIXED_VALUE_PREFIX)) {
                return StringUtils.substringBetween(field.getAPath(), ImportConstants.VALUE_SEPARATOR);
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

        value = StringUtils.trim(value);

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
            if (field.getDbColumnSize() != null) {
                try {
                    int dbSize = Integer.parseInt(field.getDbColumnSize());
                    if (StringUtils.length(value) > dbSize) {
                        log.warn("Feito substring para o campo=" + field.getColumn() + " tabela=" + field.getTable() + " tamanhoColuna=" + field.getDbColumnSize() + " valor original=" + value);
                        value = StringUtils.substring(value, 0, dbSize);
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Erro processar maxSize", e);
                }
            }
            return value.trim();
        }
    }
}
