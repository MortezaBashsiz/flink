package logforge;

import java.util.Properties;
import java.lang.String;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import logforge.Log ;
import logforge.InfluxDBForge ;

public class Country 
{
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("zookeeper.connect", "127.0.0.1:2181");
		properties.setProperty("group.id", "CountryForge");
		DataStream<String> v_LogMinerOutStream = env.addSource(new FlinkKafkaConsumer<>("LogMinerOut", new SimpleStringSchema(), properties));
		DataStream<Log> v_Log = v_LogMinerOutStream.flatMap(new MakeLog());
		DataStream<Tuple6<String , String , String , Integer , String , String>> v_FieldCounterCountry = v_Log.flatMap(new FieldCounterCountry()).keyBy(1,2).timeWindow(Time.seconds(10)).sum(3);
		DataStream<String> v_FieldCounterCountry_string = v_FieldCounterCountry.flatMap(new TupleToStringCountry());
		env.execute("CountryForge Is Proccessing On Logs");
	}
	public static class MakeLog implements FlatMapFunction<String,Log> {
		@Override
		public void flatMap(String stream, Collector<Log> out) throws Exception {
			JSONParser parser = new JSONParser();
			JSONObject json = (JSONObject) parser.parse(stream);
			String country = (String) json.get("country");
			String latitude = String.valueOf(json.get("latitude"));
			String longitude = String.valueOf(json.get("longitude"));
			String token_md5 = (String) json.get("token_md5");
			Log v_Log = new Log();
			v_Log.country = country ;
			v_Log.latitude = latitude ;
			v_Log.longitude = longitude ;
			v_Log.token_md5 = token_md5 ;
			out.collect(v_Log);
		}
	}
	public static class FieldCounterCountry implements FlatMapFunction<Log,Tuple6<String,String,String,Integer,String,String>> {
		@Override
		public void flatMap(Log v_Log, Collector<Tuple6<String,String,String,Integer,String,String>> out) throws Exception {
			String latitude = (String) v_Log.latitude ;
			String longitude = (String) v_Log.longitude ;
			String country = (String) v_Log.country ;
			String token_md5 = (String) v_Log.token_md5 ;
			if (!latitude.equals("EMPTY"))
			{
				out.collect(new Tuple6<String,String,String,Integer,String,String>("country",country,token_md5,1,latitude,longitude));
			}
		}
	}
	public static class TupleToStringCountry implements FlatMapFunction<Tuple6<String , String , String , Integer , String , String>,String> {
		@Override
		public void flatMap(Tuple6<String,String,String,Integer,String,String> v_Tuple6,  Collector<String> out) throws Exception {
			String measurement = v_Tuple6.getField(0) ;
			String field = v_Tuple6.getField(1) ;
			String token_md5 = v_Tuple6.getField(1) ;
			String count = Integer.toString(v_Tuple6.getField(3)) ;
			String latitude = v_Tuple6.getField(4);
			String longitude = v_Tuple6.getField(5);
			CInfluxDB v_InfluxDB = new CInfluxDB();
		 	v_InfluxDB.insertCountry(measurement,field,token_md5,count,latitude,longitude);
		}
	}
	public static class CInfluxDB {
		public void insertCountry(String measurement,String field,String token_md5,String count,String latitude,String longitude) throws Exception {
			InfluxDBForge influxdb = new InfluxDBForge();
			influxdb.insertCountry(measurement,field,token_md5,count,latitude,longitude);
		}
	}
} 