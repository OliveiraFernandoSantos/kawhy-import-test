package br.com.actionsys.kawhyimport.metadata.table;

import br.com.actionsys.kawhyimport.metadata.field.FieldMapping;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Data
@Getter
@Setter
public class TableMapping {

    private String tableId;
    private String tableName;
    private String whereComplement;
    private String tableAPath;
    private String parentAPath;
    private FieldMapping idField;
    private FieldMapping sequenceField;
    private List<FieldMapping> fields;
}
