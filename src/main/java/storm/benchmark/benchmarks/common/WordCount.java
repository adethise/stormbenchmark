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

package storm.benchmark.benchmarks.common;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.log4j.Logger;
import storm.benchmark.lib.operation.WordSplit;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.metrics.Latencies;

import java.util.HashMap;
import java.util.Map;

public abstract class WordCount extends StormBenchmark {
  private static final Logger LOG = Logger.getLogger(WordCount.class);

  public static final String SPOUT_ID = "spout";
  public static final String SPOUT_NUM = "component.spout_num";
  public static final String SPLIT_ID = "split";
  public static final String SPLIT_NUM = "component.split_bolt_num";
  public static final String COUNT_ID = "count";
  public static final String COUNT_NUM = "component.count_bolt_num";
  public static final int DEFAULT_SPOUT_NUM = 8;
  public static final int DEFAULT_SPLIT_BOLT_NUM = 4;
  public static final int DEFAULT_COUNT_BOLT_NUM = 4;

  protected IRichSpout spout;

  @Override
  public StormTopology getTopology(Config config) {

    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    final int spBoltNum = BenchmarkUtils.getInt(config, SPLIT_NUM, DEFAULT_SPLIT_BOLT_NUM);
    final int cntBoltNum = BenchmarkUtils.getInt(config, COUNT_NUM, DEFAULT_COUNT_BOLT_NUM);

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(SPLIT_ID, new SplitSentence(), spBoltNum).localOrShuffleGrouping(
            SPOUT_ID);
    builder.setBolt(COUNT_ID, new Count(), cntBoltNum).fieldsGrouping(SPLIT_ID,
      new Fields(SplitSentence.FIELDS));

    return builder.createTopology();
  }

  public IRichSpout getSpout() {
    return spout;
  }

  public static class SplitSentence extends BaseBasicBolt {

    public static final String FIELDS = "word";
    private long count = 0;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
      for (String word : WordSplit.splitSentence(input.getString(0))) {
        collector.emit(new Values(word,input.getValue(1)));
      }
      count++;
      if (count % 1000 == 0) {
	      System.out.println("Collected " + count + " tuples");
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(FIELDS,"timestamp"));
    }

  }

  public static class ACount extends BaseBasicBolt {
    public static final String FIELDS_WORD = "word";
    public static final String FIELDS_COUNT = "count";
    transient Latencies _latencies;

    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
     _latencies = new Latencies();
     context.registerMetric("latencies", _latencies, 5);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count=count+ (Integer) tuple.getValue(1);
      counts.put(word, count);
    long creation = (Long) tuple.getValue(2);
    long time = System.currentTimeMillis();
    _latencies.add((int) (time-creation));
      //collector.emit(new Values(word, count, creation));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(FIELDS_WORD, FIELDS_COUNT, "timestamp"));
    }
  }

public static class Count extends BaseBasicBolt {
    public static final String FIELDS_WORD = "word";
    public static final String FIELDS_COUNT = "count";

    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      collector.emit(new Values(word, count, tuple.getValue(1)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(FIELDS_WORD, FIELDS_COUNT, "timestamp"));
    }
  }
}
