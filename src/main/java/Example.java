import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Person> flintstones  = env.fromElements(new Person("Fred", 35),
                new Person("Wilma", 35),
                new Person("ASD", 35),
                new Person("ASD", 70),
                new Person("Wilma", 2));

        SingleOutputStreamOperator<Person> result = flintstones.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person person) throws Exception {
                return person.name.equals("Wilma")|person.name.equals("ASD");
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> personMap = result.map(new MapFunction<Person, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Person person) throws Exception {

                String name = person.name;
                Integer age = person.age * 10;
                Tuple2<String, Integer> of = Tuple2.of(name, age);
                return of;
            }
        });

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> tuple3SingleOutputStreamOperator = personMap.flatMap(new FlatMapFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {
            @Override
            public void flatMap(Tuple2<String, Integer> stringIntegerTuple2, Collector<Tuple3<String, Integer, Integer>> collector) throws Exception {
                collector.collect(Tuple3.of(stringIntegerTuple2.f0, stringIntegerTuple2.f1, 1));
            }
        });

        tuple3SingleOutputStreamOperator.map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple3<String, Integer, Integer> stringIntegerIntegerTuple3) throws Exception {

                return Tuple2.of(stringIntegerIntegerTuple3.f0,stringIntegerIntegerTuple3.f1);
            }
        }).keyBy(value -> value.f0).sum(1).print("saaaaaaa");


        result.print();
        env.execute("test");
    }

    public static class Person {
        public String name;
        public Integer age;
        public Person() {}

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        public String toString() {
            return this.name.toString() + ": age " + this.age.toString();
        }
    }
}
