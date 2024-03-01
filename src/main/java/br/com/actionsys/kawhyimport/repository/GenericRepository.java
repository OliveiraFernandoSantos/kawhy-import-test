package br.com.actionsys.kawhyimport.repository;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.util.Map;

@Slf4j
@Repository
public class GenericRepository {

    @Autowired
    DataSource dataSource;

    public void insert(String tableName, Map<String, Object> columnsAndValues) {
        try {
            SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
            simpleJdbcInsert.setTableName(tableName);
            simpleJdbcInsert.execute(columnsAndValues);

        } catch (DuplicateKeyException e) {
            log.debug("Registro duplicado {} columnsAndValues {}", tableName, columnsAndValues);
            throw e;
        }
    }

}
