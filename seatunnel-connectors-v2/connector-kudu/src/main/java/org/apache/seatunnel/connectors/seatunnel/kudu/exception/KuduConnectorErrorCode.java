package org.apache.seatunnel.connectors.seatunnel.kudu.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum KuduConnectorErrorCode implements SeaTunnelErrorCode {
    GET_ROW_TYPE_INFO_FAILD("KUDU-01", "Get row type information failed"),
    GET_KUDUSCAN_OBJECT_FAILED("KUDU-02", "Get the Kuduscan object for each splice failed"),
    CLOSE_KUDU_CLIENT_FAILED("KUDU-03", "Close Kudu client failed"),
    GET_KUDU_VALUE_FAILED("KUDU-04", "Get Kudu value failed"),
    TRANSFORM_TO_KUDU_DATA_TYPE_FAILED("KUDU-05", "Unsupported Kudu datatype to be transformed"),
    DATA_TYPE_CAST_FILED("KUDU-06", "Value type does not match column type"),
    KUDU_UPSERT_FAILED("KUDU-07", "Upsert data to Kudu failed"),
    KUDU_INSERT_FAILED("KUDU-08", "Insert data to Kudu failed"),
    INIT_KUDU_CLIENT_FAILED("KUDU-09", "Initialize the Kudu client failed"),
    GENERATE_KUDU_PARAMETERS_FAILED("KUDU-10", "Generate Kudu Parameters in the preparation phase failed")
    ;



    private final String code;

    private final String description;

    KuduConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }
}
