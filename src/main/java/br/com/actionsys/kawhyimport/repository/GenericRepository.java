package br.com.actionsys.kawhyimport.repository;

import java.util.Map;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.stereotype.Repository;

@Slf4j
@Repository
public class GenericRepository {

  @Autowired DataSource dataSource;

  public void insert(String tableName, Map<String, Object> columnsAndValues) {
    try {
      SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);

      // TODO VERIFICAR SCHEMA
      //            simpleJdbcInsert.setSchemaName(properties.getDefaultSchema());
      simpleJdbcInsert.setTableName(tableName);
      simpleJdbcInsert.execute(columnsAndValues);

    } catch (DuplicateKeyException e) {
      log.debug("Registro duplicado {} columnsAndValues {}", tableName, columnsAndValues);
      throw e;
    }
  }

  public void update(String sql) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    jdbcTemplate.execute(sql);
  }

  public Integer selectCount(String sql) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject(sql, Integer.class);
  }
}
