/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package storm.benchmark.lib.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import org.apache.log4j.Logger;
import storm.benchmark.tools.FileReader;


import java.util.Map;
import java.util.logging.Level;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import storm.benchmark.metrics.Latencies;
import storm.benchmark.tools.HDFSFileReader;
import storm.benchmark.util.BenchmarkUtils;

public class FileReadSpout extends BaseRichSpout {

    private static final Logger LOG = Logger.getLogger(FileReadSpout.class);
    private static final long serialVersionUID = -2582705611472467172L;
 
    //public static final String DEFAULT_FILE = "/resources/A_Tale_of_Two_City.txt";
    public static final String DEFAULT_FILE = "hdfs://nimbus1:9000/tweet_data.txt";
    
    public static final boolean DEFAULT_ACK = false;
    public static final String FIELDS = "sentence";

    private long averageDelayNano;
    private String distribution;
    private long nextTupleNano;

    public static final String SPOUT_TARGET_THROUGHPUT = "component.spout_target_throughput";
    public static final String SPOUT_RATE_DISTRIBUTION = "component.spout_rate_distribution";
    public static final String SPOUT_NUM = "component.spout_num";
    public static final int DEFAULT_TARGET_THROUGHPUT = 50000;
    public static final String DEFAULT_RATE_DISTRIBUTION = "constant";
    public static final int DEFAULT_SPOUT_NUM = 4;

    public final boolean ackEnabled;
    public HDFSFileReader reader;
    private SpoutOutputCollector collector;
    public static Configuration conf = new Configuration();
    private long count = 0;
    
    transient Latencies _latencies;
    public static final HashMap<Long, Long> timeStamps
            = new HashMap<Long, Long>();

    public FileReadSpout() {
        this(DEFAULT_ACK, DEFAULT_FILE);
    }

    public FileReadSpout(boolean ackEnabled) {
        this(ackEnabled, DEFAULT_FILE);
    }

    public FileReadSpout(boolean ackEnabled, String file) {
        this.ackEnabled = ackEnabled;
        this.reader =
                new HDFSFileReader("hdfs://nimbus1:9000" + file);

	int throughput = conf.getInt(SPOUT_TARGET_THROUGHPUT, DEFAULT_TARGET_THROUGHPUT);
	int spout_num = conf.getInt(SPOUT_NUM, DEFAULT_SPOUT_NUM);

	this.averageDelayNano = spout_num * (long) (1e9 / throughput);
	this.distribution = conf.get(SPOUT_RATE_DISTRIBUTION, DEFAULT_RATE_DISTRIBUTION);
	this.nextTupleNano = 0;
    }

    public FileReadSpout(boolean ackEnabled, HDFSFileReader reader) {
        this.ackEnabled = ackEnabled;
        this.reader = reader;
    }

    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {
        //_latencies = new Latencies();
        //context.registerMetric("latencies", _latencies, 10);
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        if (this.distribution.equals("constant")) {
            if (this.nextTupleNano - System.nanoTime() < 0) {
                emit();
                this.nextTupleNano = System.nanoTime() + this.averageDelayNano;
            } else {
                return;
            }
        } else {
            throw new UnsupportedOperationException("Only uniform interarrival time is supported");
        }
    }

    private void emit() {
        if (ackEnabled) {
            collector.emit(new Values(reader.nextLine(),System.currentTimeMillis()), count);
            count++;
        } else {
            collector.emit(new Values(reader.nextLine()));
        }
    }

    @Override
    public void ack(Object msgId) {
        //Long id = (Long) msgId;
        //_latencies.add((int) (System.currentTimeMillis() - timeStamps.get(id)));
        //System.out.println(System.currentTimeMillis() - timeStamps.get(id));
        //timeStamps.put(id, System.currentTimeMillis()-timeStamps.get(id));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FIELDS,"timestamp"));
    }

}

