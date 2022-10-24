/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.clickhouse;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;

public class ClickhouseIT extends TestSuiteBase implements TestResource {
    private static final Logger LOG = LoggerFactory.getLogger(ClickhouseIT.class);
    private static final String CLICKHOUSE_DOCKER_IMAGE = "yandex/clickhouse-server:latest";
    private static final String HOST = "clickhouse";
    private static final String DRIVER_CLASS = "com.clickhouse.jdbc.ClickHouseDriver";
    private static final String INIT_CLICKHOUSE_PATH = "/init/clickhouse_init.conf";
    private static final String CLICKHOUSE_JOB_CONFIG = "/clickhouse_to_clickhouse.conf";
    private static final String DATABASE = "default";
    private static final String SOURCE_TABLE = "source_table";
    private static final String SINK_TABLE = "sink_table";
    private static final String INSERT_SQL = "insert_sql";
    private static final String COMPARE_SQL = "compare_sql";
    private static final Tuple2<SeaTunnelRowType, List<SeaTunnelRow>> TEST_DATASET = generateTestDataSet();
    private static final Config CONFIG = getInitClickhouseConfig();
    private ClickHouseContainer container;
    private Connection connection;

    @TestTemplate
    public void testClickhouse(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob(CLICKHOUSE_JOB_CONFIG);
        Assertions.assertEquals(0, execResult.getExitCode());
        assertHasData(SINK_TABLE);
        compareResult();
        clearSinkTable();
    }

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        this.container = new ClickHouseContainer(CLICKHOUSE_DOCKER_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases(HOST)
            .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(CLICKHOUSE_DOCKER_IMAGE)));
        Startables.deepStart(Stream.of(this.container)).join();
        LOG.info("Clickhouse container started");
        Class.forName(DRIVER_CLASS);
        Awaitility.given()
            .ignoreExceptions()
            .await()
            .atMost(180L, TimeUnit.SECONDS)
            .untilAsserted(this::initConnection);
        this.initializeClickhouseTable();
        this.batchInsertData();
    }

    private void initializeClickhouseTable() {
        try {
            Statement statement = this.connection.createStatement();
            statement.execute(CONFIG.getString(SOURCE_TABLE));
            statement.execute(CONFIG.getString(SINK_TABLE));
        } catch (SQLException e) {
            throw new RuntimeException("Initializing Clickhouse table failed!", e);
        }
    }

    private void initConnection() throws SQLException {
        this.connection = DriverManager.getConnection(
            this.container.getJdbcUrl(),
            this.container.getUsername(),
            this.container.getPassword()
        );
    }

    private static Config getInitClickhouseConfig() {
        File file = ContainerUtil.getResourcesFile(INIT_CLICKHOUSE_PATH);
        Config config = ConfigFactory.parseFile(file);
        assert config.hasPath(SOURCE_TABLE) && config.hasPath(SINK_TABLE) && config.hasPath(INSERT_SQL) && config.hasPath(COMPARE_SQL);
        return config;
    }

    private Array toSqlArray(Object value) throws SQLException {
        Object[] elements = null;
        String sqlType = null;
        if (String[].class.equals(value.getClass())) {
            sqlType = "TEXT";
            elements = (String[]) value;
        } else if (Boolean[].class.equals(value.getClass())) {
            sqlType = "BOOLEAN";
            elements = (Boolean[]) value;
        } else if (Byte[].class.equals(value.getClass())) {
            sqlType = "TINYINT";
            elements = (Byte[]) value;
        } else if (Short[].class.equals(value.getClass())) {
            sqlType = "SMALLINT";
            elements = (Short[]) value;
        } else if (Integer[].class.equals(value.getClass())) {
            sqlType = "INTEGER";
            elements = (Integer[]) value;
        } else if (Long[].class.equals(value.getClass())) {
            sqlType = "BIGINT";
            elements = (Long[]) value;
        } else if (Float[].class.equals(value.getClass())) {
            sqlType = "REAL";
            elements = (Float[]) value;
        } else if (Double[].class.equals(value.getClass())) {
            sqlType = "DOUBLE";
            elements = (Double[]) value;
        }
        if (sqlType == null) {
            throw new IllegalArgumentException("array inject error, not supported data type: " + value.getClass());
        }
        return connection.createArrayOf(sqlType, elements);
    }

    private void batchInsertData() {
        String sql = CONFIG.getString(INSERT_SQL);
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = this.connection.prepareStatement(sql);
            for (SeaTunnelRow row : TEST_DATASET._2()) {
                preparedStatement.setLong(1, (Long) row.getField(0));
                preparedStatement.setObject(2, row.getField(1));
                preparedStatement.setArray(3, toSqlArray(row.getField(2)));
                preparedStatement.setArray(4, toSqlArray(row.getField(3)));
                preparedStatement.setArray(5, toSqlArray(row.getField(4)));
                preparedStatement.setArray(6, toSqlArray(row.getField(5)));
                preparedStatement.setArray(7, toSqlArray(row.getField(6)));
                preparedStatement.setArray(8, toSqlArray(row.getField(7)));
                preparedStatement.setString(9, (String) row.getField(8));
                preparedStatement.setBoolean(10, (Boolean) row.getField(9));
                preparedStatement.setByte(11, (Byte) row.getField(10));
                preparedStatement.setShort(12, (Short) row.getField(11));
                preparedStatement.setInt(13, (Integer) row.getField(12));
                preparedStatement.setLong(14, (Long) row.getField(13));
                preparedStatement.setFloat(15, (Float) row.getField(14));
                preparedStatement.setDouble(16, (Double) row.getField(15));
                preparedStatement.setBigDecimal(17, (BigDecimal) row.getField(16));
                preparedStatement.setDate(18, Date.valueOf((LocalDate) row.getField(17)));
                preparedStatement.setTimestamp(19, Timestamp.valueOf((LocalDateTime) row.getField(18)));
                preparedStatement.setInt(20, (Integer) row.getField(19));
                preparedStatement.setString(21, (String) row.getField(20));
                preparedStatement.setArray(22, toSqlArray(row.getField(21)));
                preparedStatement.setArray(23, toSqlArray(row.getField(22)));
                preparedStatement.setArray(24, toSqlArray(row.getField(23)));
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            preparedStatement.clearBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Batch insert data failed!", e);
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static Tuple2<SeaTunnelRowType, List<SeaTunnelRow>> generateTestDataSet() {
        SeaTunnelRowType rowType = new SeaTunnelRowType(
            new String[]{
                "id",
                "c_map",
                "c_array_string",
                "c_array_short",
                "c_array_int",
                "c_array_long",
                "c_array_float",
                "c_array_double",
                "c_string",
                "c_boolean",
                "c_int8",
                "c_int16",
                "c_int32",
                "c_int64",
                "c_float32",
                "c_float64",
                "c_decimal",
                "c_date",
                "c_datetime",
                "c_nullable",
                "c_lowcardinality",
                "c_nested.int",
                "c_nested.double",
                "c_nested.string"
            },
            new SeaTunnelDataType[]{
                BasicType.LONG_TYPE,
                new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE),
                ArrayType.STRING_ARRAY_TYPE,
                ArrayType.SHORT_ARRAY_TYPE,
                ArrayType.INT_ARRAY_TYPE,
                ArrayType.LONG_ARRAY_TYPE,
                ArrayType.FLOAT_ARRAY_TYPE,
                ArrayType.DOUBLE_ARRAY_TYPE,
                BasicType.STRING_TYPE,
                BasicType.BOOLEAN_TYPE,
                BasicType.BYTE_TYPE,
                BasicType.SHORT_TYPE,
                BasicType.INT_TYPE,
                BasicType.LONG_TYPE,
                BasicType.FLOAT_TYPE,
                BasicType.DOUBLE_TYPE,
                new DecimalType(9, 4),
                LocalTimeType.LOCAL_DATE_TYPE,
                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                BasicType.INT_TYPE,
                BasicType.STRING_TYPE,
                ArrayType.INT_ARRAY_TYPE,
                ArrayType.DOUBLE_ARRAY_TYPE,
                ArrayType.STRING_ARRAY_TYPE
            });
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            SeaTunnelRow row = new SeaTunnelRow(
                new Object[]{
                    (long) i,
                    Collections.singletonMap("key", Integer.parseInt("1")),
                    new String[]{"string"},
                    new Short[]{Short.parseShort("1")},
                    new Integer[]{Integer.parseInt("1")},
                    new Long[]{Long.parseLong("1")},
                    new Float[]{Float.parseFloat("1.1")},
                    new Double[]{Double.parseDouble("1.1")},
                    "string",
                    Boolean.FALSE,
                    Byte.parseByte("1"),
                    Short.parseShort("1"),
                    Integer.parseInt("1"),
                    Long.parseLong("1"),
                    Float.parseFloat("1.1"),
                    Double.parseDouble("1.1"),
                    BigDecimal.valueOf(11L, 1),
                    LocalDate.now(),
                    LocalDateTime.now(),
                    i,
                    "string",
                    new Integer[]{Integer.parseInt("1")},
                    new Double[]{Double.parseDouble("1.1")},
                    new String[]{"1"}
                });
            rows.add(row);
        }
        return Tuple2.apply(rowType, rows);
    }

    private void compareResult() throws SQLException, IOException {
        String sourceSql = "select * from " + SOURCE_TABLE;
        String sinkSql = "select * from " + SINK_TABLE;
        List<String> columnList = Arrays.stream(generateTestDataSet()._1().getFieldNames()).collect(Collectors.toList());
        Statement sourceStatement = connection.createStatement();
        Statement sinkStatement = connection.createStatement();
        ResultSet sourceResultSet = sourceStatement.executeQuery(sourceSql);
        ResultSet sinkResultSet = sinkStatement.executeQuery(sinkSql);
        Assertions.assertEquals(sourceResultSet.getMetaData().getColumnCount(), sinkResultSet.getMetaData().getColumnCount());
        SeaTunnelDataType<?>[] fieldTypes = TEST_DATASET._1().getFieldTypes();
        while (sourceResultSet.next()) {
            if (sinkResultSet.next()) {
                for (int i = 0; i < columnList.size(); i++) {
                    String column = columnList.get(i);
                    SeaTunnelDataType<?> dataType = fieldTypes[i];
                    switch (dataType.getSqlType()) {
                        case STRING:
                            Assertions.assertEquals(sourceResultSet.getString(column), sinkResultSet.getString(column));
                            break;
                        case TINYINT:
                            Assertions.assertEquals(sourceResultSet.getByte(column), sinkResultSet.getByte(column));
                            break;
                        case SMALLINT:
                            Assertions.assertEquals(sourceResultSet.getShort(column), sinkResultSet.getShort(column));
                            break;
                        case INT:
                            Assertions.assertEquals(sourceResultSet.getInt(column), sinkResultSet.getInt(column));
                            break;
                        case BIGINT:
                            Assertions.assertEquals(sourceResultSet.getLong(column), sinkResultSet.getLong(column));
                            break;
                        case BOOLEAN:
                            Assertions.assertEquals(sourceResultSet.getBoolean(column), sinkResultSet.getBoolean(column));
                            break;
                        case FLOAT:
                            Assertions.assertEquals(sourceResultSet.getFloat(column), sinkResultSet.getFloat(column));
                            break;
                        case DOUBLE:
                            Assertions.assertEquals(sourceResultSet.getDouble(column), sinkResultSet.getDouble(column));
                            break;
                        case DECIMAL:
                            Assertions.assertEquals(sourceResultSet.getBigDecimal(column), sinkResultSet.getBigDecimal(column));
                            break;
                        case DATE:
                            Assertions.assertEquals(sourceResultSet.getDate(column), sinkResultSet.getDate(column));
                            break;
                        case TIME:
                            Assertions.assertEquals(sourceResultSet.getTime(column), sinkResultSet.getTime(column));
                            break;
                        case TIMESTAMP:
                            Assertions.assertEquals(sourceResultSet.getTimestamp(column), sinkResultSet.getTimestamp(column));
                            break;
                        default:
                            Assertions.assertTrue(Objects.deepEquals(sourceResultSet.getObject(column), sinkResultSet.getObject(column)));

                    }
                }
            }
        }
        String columns = String.join(",", generateTestDataSet()._1().getFieldNames());
        Assertions.assertTrue(compare(String.format(CONFIG.getString(COMPARE_SQL), columns, columns)));
    }

    private Boolean compare(String sql) {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                return false;
            }
            return true;
        } catch (SQLException e) {
            throw new RuntimeException("result compare error", e);
        }
    }

    private void assertHasData(String table) {
        try (Statement statement = connection.createStatement()) {
            String sql = String.format("select * from %s.%s limit 1", DATABASE, table);
            ResultSet source = statement.executeQuery(sql);
            Assertions.assertTrue(source.next());
        } catch (SQLException e) {
            throw new RuntimeException("test clickhouse server image error", e);
        }
    }

    private void clearSinkTable() {
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format("truncate table %s.%s", DATABASE, SINK_TABLE));
        } catch (SQLException e) {
            throw new RuntimeException("Test clickhouse server image error", e);
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (this.connection != null) {
            this.connection.close();
        }
        if (this.container != null) {
            this.container.stop();
        }
    }
}
