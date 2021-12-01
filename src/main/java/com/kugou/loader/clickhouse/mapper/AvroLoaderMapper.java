package com.kugou.loader.clickhouse.mapper;

import com.kugou.loader.clickhouse.config.ClickhouseConfiguration;
import com.kugou.loader.clickhouse.config.ClusterNodes;
import com.kugou.loader.clickhouse.mapper.decode.DefaultRowRecordDecoder;
import com.kugou.loader.clickhouse.mapper.decode.RowRecordDecoder;
import com.kugou.loader.clickhouse.mapper.record.decoder.TextRecordDecoder;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.codehaus.jettison.json.JSONException;

import java.io.IOException;
import java.util.IllegalFormatException;

/**
 * Created by jaykelin on 2016/11/29.
 */

public class AvroLoaderMapper extends AbstractClickhouseLoaderMapper<AvroKey, NullWritable, Text, Text> {

    private static final Log log = LogFactory.getLog(AbstractClickhouseLoaderMapper.class);

    @Override
    public RowRecordDecoder getRowRecordDecoder(Configuration config) {
        return new DefaultRowRecordDecoder<>(config, new TextRecordDecoder(new ClickhouseConfiguration(config)));
    }

    @Override
    protected void map(AvroKey key, NullWritable value, Context context) throws IOException, InterruptedException {
        try {
            RowRecordDecoder rowRecordDecoder = getRowRecordDecoder(this.getConfig());
            rowRecordDecoder.setRowRecord("", key.datum().toString());
            write(key, readRowRecord(rowRecordDecoder, context), context);
        } catch (IllegalFormatException e) {
            log.error(e.getMessage(), e);
            context.getCounter(CLICKHOUSE_COUNTERS_GROUP, "Illegal format records").increment(1);
        } catch (JSONException e) {
            context.getCounter(CLICKHOUSE_COUNTERS_GROUP, "Illegal format records").increment(1);
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public void write(ClusterNodes nodes, String mapTaskIdentify, String tempTable, String tempDatabase, Context context) throws IOException, InterruptedException {
        for (String host : nodes.getNodeDataStatus().keySet()) {
            if (nodes.getNodeDataStatus().get(host)) {
                if (!tempTable.contains(".")) {
                    tempTable = tempDatabase + "." + tempTable;
                }
                log.info("Output result: " + mapTaskIdentify + "@" + host + "-->" + tempTable);
                context.write(new Text(mapTaskIdentify + "@" + host), new Text(tempTable));
            }
        }
    }
}
