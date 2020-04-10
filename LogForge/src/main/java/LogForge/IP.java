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

public class IP
{
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("zookeeper.connect", "127.0.0.1:2181");
		properties.setProperty("group.id", "IPForge");
		DataStream<String> v_LogMinerOutStream = env.addSource(new FlinkKafkaConsumer<>("LogMinerOut", new SimpleStringSchema(), properties));
		DataStream<Log> v_Log = v_LogMinerOutStream.flatMap(new MakeLog());
		DataStream<Tuple3<String , String , Integer>> v_FieldCounterIP = v_Log.flatMap(new FieldCounterIP()).keyBy(1).timeWindow(Time.seconds(10)).sum(2);
		DataStream<String> v_FieldCounterIP_string = v_FieldCounterIP.flatMap(new TupleToStringIP());
		DataStream<Tuple3<String , String , Integer>> v_FieldIPByte = v_Log.flatMap(new FieldIPByte()).keyBy(1).timeWindow(Time.seconds(10)).sum(2);
		DataStream<String> v_FieldIPByte_string = v_FieldIPByte.flatMap(new TupleToStringIPByte());
		env.execute("IPForge Is Proccessing On Logs");
	}
	public static class MakeLog implements FlatMapFunction<String,Log> {
		@Override
		public void flatMap(String stream, Collector<Log> out) throws Exception {
			JSONParser parser = new JSONParser();
			JSONObject json = (JSONObject) parser.parse(stream);
			String user_ip = (String) json.get("user_ip");
			String body_bytes_sent = String.valueOf(json.get("body_bytes_sent"));
			Log v_Log = new Log();
			v_Log.user_ip = user_ip ;
			v_Log.body_bytes_sent = body_bytes_sent ;
			out.collect(v_Log);
		}
	}
	public static class FieldCounterIP implements FlatMapFunction<Log,Tuple3<String,String,Integer>> {
		@Override
		public void flatMap(Log v_Log, Collector<Tuple3<String,String,Integer>> out) throws Exception {
			String user_ip = (String) v_Log.user_ip ;
			out.collect(new Tuple3<String,String,Integer>("user_ip",user_ip,1));
		}
	}
	public static class FieldIPByte implements FlatMapFunction<Log,Tuple3<String,String,Integer>> {
		@Override
		public void flatMap(Log v_Log, Collector<Tuple3<String,String,Integer>> out) throws Exception {
			String user_ip = (String) v_Log.user_ip ;
			String body_bytes_sent = (String) v_Log.body_bytes_sent ;
			out.collect(new Tuple3<String,String,Integer>("ip_byte",user_ip,Integer.valueOf(body_bytes_sent)));
		}
	}
	public static class TupleToStringIP implements FlatMapFunction<Tuple3<String,String,Integer>,String> {
		@Override
		public void flatMap(Tuple3<String,String,Integer> v_Tuple3,  Collector<String> out) throws Exception {
			String measurement = v_Tuple3.getField(0);
			String ip = v_Tuple3.getField(1);
			String count = Integer.toString(v_Tuple3.getField(2));
			CInfluxDB v_InfluxDB = new CInfluxDB();
		 	v_InfluxDB.insertIP(measurement,ip,count);
		}
	}
	public static class TupleToStringIPByte implements FlatMapFunction<Tuple3<String,String,Integer>,String> {
		@Override
		public void flatMap(Tuple3<String,String,Integer> v_Tuple3,  Collector<String> out) throws Exception {
			String measurement = v_Tuple3.getField(0);
			String ip = v_Tuple3.getField(1);
			String body_bytes_sent = Integer.toString(v_Tuple3.getField(2));
			CInfluxDB v_InfluxDB = new CInfluxDB();
		 	v_InfluxDB.insertIPByte(measurement,ip,body_bytes_sent);
		}
	}
	public static class CInfluxDB {
		public void insertIP(String measurement , String ip , String count) throws Exception {
			InfluxDBForge influxdb = new InfluxDBForge();
			influxdb.insertIP(measurement,ip,count);
		}
		public void insertIPByte(String measurement , String ip , String body_bytes_sent) throws Exception {
			InfluxDBForge influxdb = new InfluxDBForge();
			influxdb.insertIP(measurement,ip,body_bytes_sent);
		}
	}
} 