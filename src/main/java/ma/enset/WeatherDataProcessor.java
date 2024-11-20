package ma.enset;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class WeatherDataProcessor {
    public static void main(String[] args) {
        // Kafka Streams Configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-data-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Read data from 'weather-data'
        KStream<String, String> weatherDataStream = builder.stream("weather-data");

        // Filter temperatures > 30°C
        KStream<String, String> highTempStream = weatherDataStream.filter((key, value) -> {
            try {
                String[] data = value.split(",");
                return Double.parseDouble(data[1]) > 30;
            } catch (Exception e) {
                System.err.println("Invalid data: " + value);
                return false;
            }
        });

        // Convert temperatures to Fahrenheit
        KStream<String, String> convertedStream = highTempStream.mapValues(value -> {
            String[] data = value.split(",");
            double celsius = Double.parseDouble(data[1]);
            double fahrenheit = (celsius * 9 / 5) + 32;
            return data[0] + "," + fahrenheit + "," + data[2];
        });

        // Group by station and calculate averages
        KGroupedStream<String, String> groupedStream = convertedStream.groupBy((key, value) -> value.split(",")[0]);
        groupedStream.aggregate(
                () -> "0,0,0",
                (station, newValue, aggregate) -> {
                    String[] newData = newValue.split(",");
                    String[] aggData = aggregate.split(",");
                    double newTemp = Double.parseDouble(newData[1]);
                    double aggTemp = Double.parseDouble(aggData[1]);
                    double newHumidity = Double.parseDouble(newData[2]);
                    double aggHumidity = Double.parseDouble(aggData[2]);
                    int count = Integer.parseInt(aggData[0]);
                    return (count + 1) + "," + ((aggTemp + newTemp) / (count + 1)) + "," + ((aggHumidity + newHumidity) / (count + 1));
                },
                Materialized.with(Serdes.String(), Serdes.String())
        ).toStream().to("station-averages");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
