/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.db2;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.db2.DB2TypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.db2.DB2TypeMapper;

import org.apache.commons.lang3.StringUtils;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DB2Catalog extends AbstractJdbcCatalog {

    protected final Map<String, Connection> connectionMap;

    private static final String SELECT_COLUMNS_SQL =
            "SELECT NAME AS column_name,\n"
                    + "       TYPENAME AS type_name,\n"
                    + "       TYPENAME AS full_type_name,\n"
                    + "       (CASE WHEN LENGTH = -1 THEN LONGLENGTH ELSE LENGTH END) AS column_length,\n"
                    + "       SCALE AS column_scale,\n"
                    + "       REMARKS AS column_comment,\n"
                    + "       DEFAULT  AS default_value,\n"
                    + "       NULLS AS is_nullable\n"
                    + "FROM SYSIBM.SYSCOLUMNS WHERE TBCREATOR = '%s' AND  TBNAME = '%s' ORDER BY COLNO ASC";

    public DB2Catalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
        this.connectionMap = new ConcurrentHashMap<>();
    }

    @SneakyThrows
    @Override
    public List<String> listDatabases() throws CatalogException {
        try (ResultSet resultSet =
                getConnection(getUrlFromDatabaseName(defaultDatabase))
                        .getMetaData()
                        .getCatalogs()) {
            return Collections.singletonList(resultSet.getMetaData().getCatalogName(1));
        }
    }

    protected String getTableWithConditionSql(TablePath tablePath) {
        return getListTableSql(tablePath.getDatabaseName())
                + "  and TABSCHEMA = '"
                + tablePath.getSchemaName()
                + "' and TABNAME = '"
                + tablePath.getTableName()
                + "'";
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(
                SELECT_COLUMNS_SQL, tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SELECT TABSCHEMA , TABNAME FROM SYSCAT.TABLES WHERE TABSCHEMA NOT IN ('SYSCAT','SYSIBM','SYSIBMADM','SYSPUBLIC','SYSSTAT','SYSTOOLS')";
    }

    @Override
    protected String getCreateTableSql(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        String createTableSql = new DB2CreateTableSqlBuilder(table).build(tablePath);
        return CatalogUtils.getFieldIde(createTableSql, table.getOptions().get("fieldIde"));
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format(
                "DROP TABLE IF EXISTS %s.%s ",
                tablePath.getSchemaName(), "\"" + tablePath.getTableName() + "\"");
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) {
        return String.format(
                "ALTER TABLE %s.%s ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE",
                tablePath.getSchemaName(), "\"" + tablePath.getTableName() + "\"");
    }

    protected Optional<PrimaryKey> getPrimaryKey(
            DatabaseMetaData metaData, String database, String schema, String table)
            throws SQLException {
        List<String> fields = getPrimaryKeyFieldList(schema, table);
        if (fields == null || fields.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(PrimaryKey.of(getPrimaryKeyName(schema, table), fields));
    }

    private List<String> getPrimaryKeyFieldList(String schema, String table) {
        String getPrimaryKeyFieldSql =
                String.format(
                        "SELECT COLNAME FROM SYSCAT.KEYCOLUSE WHERE TABSCHEMA = '%s' AND TABNAME = '%s';",
                        schema, table);
        Connection connection = getConnection(getUrlFromDatabaseName(defaultDatabase));
        List<String> primaryKeyColNameList = new ArrayList<>();
        try (Statement ps = connection.createStatement();
                ResultSet resultSet = ps.executeQuery(getPrimaryKeyFieldSql)) {

            while (resultSet.next()) {
                String primaryKeyColName = resultSet.getString("COLNAME");
                primaryKeyColNameList.add(primaryKeyColName);
            }
            return primaryKeyColNameList;
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed getPrimaryKeyFieldList table %s", table), e);
        }
    }

    private String getPrimaryKeyName(String schema, String table) {
        String getPrimaryKeyNameSql =
                String.format(
                        "SELECT INDNAME FROM SYSCAT.INDEXES WHERE UNIQUERULE = 'P' AND TABSCHEMA  = '%s' AND TABNAME = '%s' ;",
                        schema, table);
        Connection connection = getConnection(getUrlFromDatabaseName(defaultDatabase));
        try (Statement ps = connection.createStatement();
                ResultSet resultSet = ps.executeQuery(getPrimaryKeyNameSql)) {

            while (resultSet.next()) {
                String primaryKeyColName = resultSet.getString("INDNAME");
                if (StringUtils.isNotEmpty(primaryKeyColName)) {
                    return primaryKeyColName;
                }
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed getPrimaryKeyName table %s", table), e);
        }
        return null;
    }

    @Override
    public String getExistDataSql(TablePath tablePath) {
        return String.format(
                "select * from %s.%s FETCH FIRST 1 ROW ONLY;",
                tablePath.getSchemaName(), "\"" + tablePath.getTableName() + "\"");
    }

    public Connection getConnection(String url) {
        if (connectionMap.containsKey(url)) {
            return connectionMap.get(url);
        }
        try {
            Connection connection = DriverManager.getConnection(url, username, pwd);
            connectionMap.put(url, connection);
            return connection;
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed connecting to %s via JDBC.", url), e);
        }
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("column_name");
        String typeName = resultSet.getString("type_name").trim();
        String fullTypeName = resultSet.getString("full_type_name").trim();
        long columnLength = resultSet.getLong("column_length");
        int columnScale = resultSet.getInt("column_scale");
        String columnComment = resultSet.getString("column_comment");
        Object defaultValue = resultSet.getObject("default_value");
        boolean isNullable = resultSet.getString("is_nullable").equals("Y");

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(fullTypeName)
                        .dataType(typeName)
                        .length(columnLength)
                        .precision(columnLength)
                        .scale(columnScale)
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(columnComment)
                        .build();
        return DB2TypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        return CatalogUtils.getCatalogTable(
                getConnection(getUrlFromDatabaseName(defaultDatabase)),
                sqlQuery,
                new DB2TypeMapper());
    }
}
