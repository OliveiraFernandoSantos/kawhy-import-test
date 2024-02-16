package br.com.actionsys.kawhyimport.command;

import lombok.Data;

import java.util.Map;

@Data
public class SqlCommand {

    private SqlCommandType type;
    private String tableName;
    private Map<String, Object> values;
    private Map<String, Object> where;
}
