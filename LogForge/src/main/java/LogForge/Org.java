package logforge;

import java.util.Properties;
import java.lang.String;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import logforge.Log ;
import logforge.InfluxDBForge ;

public class Org
{
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("zookeeper.connect", "127.0.0.1:2181");
		properties.setProperty("group.id", "OrgForge");
		DataStream<String> v_LogMinerOutStream = env.addSource(new FlinkKafkaConsumer<>("LogMinerOut", new SimpleStringSchema(), properties));
		DataStream<Log> v_Log = v_LogMinerOutStream.flatMap(new MakeLog());
		DataStream<Tuple3<String , String , Integer>> v_FieldCounterOrg = v_Log.flatMap(new FieldCounterOrg()).keyBy(1).timeWindow(Time.seconds(10)).sum(2);
		DataStream<String> v_FieldCounterOrg_string = v_FieldCounterOrg.flatMap(new TupleToStringOrg());
		env.execute("OrgForge Is Proccessing On Logs");
	}
	public static class MakeLog implements FlatMapFunction<String,Log> {
		@Override
		public void flatMap(String stream, Collector<Log> out) throws Exception {
			JSONParser parser = new JSONParser();
			JSONObject json = (JSONObject) parser.parse(stream);
			String organization = (String) json.get("organization");
			Log v_Log = new Log();
			v_Log.organization = organization ;
			out.collect(v_Log);
		}
	}
	public static class FieldCounterOrg implements FlatMapFunction<Log,Tuple3<String,String,Integer>> {
		@Override
		public void flatMap(Log v_Log, Collector<Tuple3<String,String,Integer>> out) throws Exception {
			String organization = (String) v_Log.organization ;
			out.collect(new Tuple3<String,String,Integer>("organization",organization,1));
		}
	}
	public static class TupleToStringOrg implements FlatMapFunction<Tuple3<String,String,Integer>,String> {
		@Override
		public void flatMap(Tuple3<String,String,Integer> v_Tuple3,  Collector<String> out) throws Exception {
			String measurement = v_Tuple3.getField(0) ;
			String organization = v_Tuple3.getField(1) ;
			String count = Integer.toString(v_Tuple3.getField(2)) ;
			CInfluxDB v_InfluxDB = new CInfluxDB();
		 	v_InfluxDB.insertOrg(measurement,organization,count);
		}
	}
	public static class CInfluxDB {
		public void insertOrg(String measurement , String index , String count) throws Exception {
			InfluxDBForge influxdb = new InfluxDBForge();
			influxdb.insertOrg(measurement,index,count);
		}
	}
} 