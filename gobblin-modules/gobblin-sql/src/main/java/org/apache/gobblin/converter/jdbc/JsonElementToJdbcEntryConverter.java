package org.apache.gobblin.converter.jdbc;

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;

import java.util.List;
import java.util.Map;

public class JsonElementToJdbcEntryConverter<SI, SO> extends Converter<SI, SO, JsonElement, JdbcEntryData> {
    @Override
    public SO convertSchema(SI inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
        return (SO)inputSchema;
    }

    @Override
    public Iterable<JdbcEntryData> convertRecord(SO outputSchema, JsonElement inputRecord, WorkUnitState workUnit)
            throws DataConversionException {
        JsonObject record = inputRecord.getAsJsonObject();
        List<JdbcEntryDatum> jdbcEntryData = Lists.newArrayList();
        for(Map.Entry<String, JsonElement> entry: record.entrySet()){
            jdbcEntryData.add(new JdbcEntryDatum(entry.getKey(), entry.getValue().toString().replaceAll("^\"|\"$", "")));
        }
        JdbcEntryData outputRecord =  new JdbcEntryData(jdbcEntryData);
        return new SingleRecordIterable<>(outputRecord);
    }
}