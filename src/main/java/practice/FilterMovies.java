package practice;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple3;

import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Stream;

public class FilterMovies {

    public static void main(String[] args) throws Exception{

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<org.apache.flink.api.java.tuple.Tuple3<Long, String, String>> lines =
                env.readCsvFile("/Users/soonam/workspace/flinkpractice/src/main/resources/Movies.csv")
                        .ignoreFirstLine()
                        .parseQuotedStrings('"')
                        .ignoreInvalidLines()
                        .types(Long.class,String.class,String.class);


        DataSet<Movie> movies = lines.map(new MapFunction<org.apache.flink.api.java.tuple.Tuple3<Long, String, String>, Movie>() {
            @Override
            public Movie map(org.apache.flink.api.java.tuple.Tuple3<Long, String, String> csvLine) throws Exception {
                String movieName = csvLine.f1;
                String[] genres = csvLine.f2.split("\\|");
                return new Movie(movieName, new HashSet<>(Arrays.asList(genres)));
            }
        });

        DataSet<Movie> filteredMovies =  movies.filter(new FilterFunction<Movie>() {
            @Override
            public boolean filter(Movie movie) throws Exception {
                return movie.getGenres().contains("Action");
            }
        });

        filteredMovies.writeAsText("/Users/soonam/workspace/flinkpractice/src/main/resources/FilteredMovies.csv");

        env.execute("Filter Movies");
    }

}
