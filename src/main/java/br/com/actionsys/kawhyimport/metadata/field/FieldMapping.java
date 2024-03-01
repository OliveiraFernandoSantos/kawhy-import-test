package br.com.actionsys.kawhyimport.metadata.field;

import lombok.Data;

@Data
public class FieldMapping {

    private String tableId;
    private String table;
    private String variable;
    private String column;
    private String aPath;
    private String type;
    private String whereComplement;
    private String regex;
    private String maxSize;
    private String tableAPath;
    private String parentAPath;
}
