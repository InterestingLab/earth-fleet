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

package org.apache.seatunnel.connectors.seatunnel.http.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpPage;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.JsonField;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;

import com.google.common.base.Strings;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class HttpSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    protected final SingleSplitReaderContext context;
    protected final HttpParameter httpParameter;
    protected HttpClientProvider httpClient;
    private final DeserializationCollector deserializationCollector;
    private static final Option[] DEFAULT_OPTIONS = {
        Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST, Option.DEFAULT_PATH_LEAF_TO_NULL
    };
    private JsonPath[] jsonPaths;
    private final JsonField jsonField;
    private final String contentJson;
    private final Configuration jsonConfiguration =
            Configuration.defaultConfiguration().addOptions(DEFAULT_OPTIONS);

    public HttpSourceReader(
            HttpParameter httpParameter,
            SingleSplitReaderContext context,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            JsonField jsonField,
            String contentJson) {
        this.context = context;
        this.httpParameter = httpParameter;
        this.deserializationCollector = new DeserializationCollector(deserializationSchema);
        this.jsonField = jsonField;
        this.contentJson = contentJson;
    }

    @Override
    public void open() {
        httpClient = new HttpClientProvider(httpParameter);
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(httpClient)) {
            httpClient.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        HttpResponse response = null;
        try {
            if (this.httpParameter.getHttpPage() != null) {
                HttpPage httpPage = this.httpParameter.getHttpPage();
                List<String> pageNumbers = getPageNumbers(httpPage.getPageNo());
                Map<String, String> params = this.httpParameter.getParams();

                params.put(httpPage.getPageSizeField(), httpPage.getPageSize());
                for (String pageNumber : pageNumbers) {
                    params.put(httpPage.getPageNoField(), pageNumber);
                    response = httpClient.execute(httpParameter);
                    ValidityParametersAndSubmit(response, output);
                }
            } else {
                response = httpClient.execute(httpParameter);
                ValidityParametersAndSubmit(response, output);
            }
        } catch (Exception e) {
            assert response != null;
            throw new HttpConnectorException(
                    CommonErrorCode.ILLEGAL_ARGUMENT,
                    String.format(
                            "http client execute exception, http response status code:[%d], content:[%s]",
                            response.getCode(), response.getContent()));
        } finally {
            if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded http source");
                context.signalNoMoreElement();
            } else {
                if (httpParameter.getPollIntervalMillis() > 0) {
                    Thread.sleep(httpParameter.getPollIntervalMillis());
                }
            }
        }
    }

    private List<Map<String, String>> parseToMap(List<List<String>> datas, JsonField jsonField) {
        List<Map<String, String>> decodeDatas = new ArrayList<>(datas.size());
        String[] keys = jsonField.getFields().keySet().toArray(new String[] {});

        for (List<String> data : datas) {
            Map<String, String> decodeData = new HashMap<>(jsonField.getFields().size());
            final int[] index = {0};
            data.forEach(
                    field -> {
                        decodeData.put(keys[index[0]], field);
                        index[0]++;
                    });
            decodeDatas.add(decodeData);
        }

        return decodeDatas;
    }

    private List<List<String>> decodeJSON(String data) {
        ReadContext jsonReadContext = JsonPath.using(jsonConfiguration).parse(data);
        List<List<String>> results = new ArrayList<>(jsonPaths.length);
        for (JsonPath path : jsonPaths) {
            List<String> result = jsonReadContext.read(path);
            results.add(result);
        }
        for (int i = 1; i < results.size(); i++) {
            List<?> result0 = results.get(0);
            List<?> result = results.get(i);
            if (result0.size() != result.size()) {
                throw new HttpConnectorException(
                        HttpConnectorErrorCode.FIELD_DATA_IS_INCONSISTENT,
                        String.format(
                                "[%s](%d) and [%s](%d) the number of parsing records is inconsistent.",
                                jsonPaths[0].getPath(),
                                result0.size(),
                                jsonPaths[i].getPath(),
                                result.size()));
            }
        }

        return dataFlip(results);
    }

    private String getPartOfJson(String data) {
        ReadContext jsonReadContext = JsonPath.using(jsonConfiguration).parse(data);
        return JsonUtils.toJsonString(jsonReadContext.read(JsonPath.compile(contentJson)));
    }

    private List<List<String>> dataFlip(List<List<String>> results) {

        List<List<String>> datas = new ArrayList<>();
        for (int i = 0; i < results.size(); i++) {
            List<String> result = results.get(i);
            if (i == 0) {
                for (Object o : result) {
                    String val = o == null ? null : o.toString();
                    List<String> row = new ArrayList<>(jsonPaths.length);
                    row.add(val);
                    datas.add(row);
                }
            } else {
                for (int j = 0; j < result.size(); j++) {
                    Object o = result.get(j);
                    String val = o == null ? null : o.toString();
                    List<String> row = datas.get(j);
                    row.add(val);
                }
            }
        }
        return datas;
    }

    private void initJsonPath(JsonField jsonField) {
        jsonPaths = new JsonPath[jsonField.getFields().size()];
        for (int index = 0; index < jsonField.getFields().keySet().size(); index++) {
            jsonPaths[index] =
                    JsonPath.compile(
                            jsonField.getFields().values().toArray(new String[] {})[index]);
        }
    }

    public static List<String> getPageNumbers(String pageNumbersString) {
        List<String> pageNumbers = new ArrayList<>();
        String[] ranges = pageNumbersString.split(",");
        for (String range : ranges) {
            String[] limits = range.split("-");
            if (limits.length == 1) {
                String pageNumber = limits[0].trim();
                pageNumbers.add(pageNumber);
            } else if (limits.length == 2) {
                int start = Integer.parseInt(limits[0].trim());
                int end = Integer.parseInt(limits[1].trim());
                for (int i = start; i <= end; i++) {
                    pageNumbers.add(String.valueOf(i));
                }
            }
        }
        return pageNumbers;
    }

    @SneakyThrows
    public void ValidityParametersAndSubmit(HttpResponse response, Collector<SeaTunnelRow> output) {
        if (HttpResponse.STATUS_OK == response.getCode()) {
            String content = response.getContent();
            if (!Strings.isNullOrEmpty(content)) {
                if (contentJson != null) {
                    content = JsonUtils.stringToJsonNode(getPartOfJson(content)).toString();
                }
                if (jsonField != null) {
                    this.initJsonPath(jsonField);
                    content =
                            JsonUtils.toJsonNode(parseToMap(decodeJSON(content), jsonField))
                                    .toString();
                }
                deserializationCollector.collect(content.getBytes(), output);
            }
        }
    }
}
