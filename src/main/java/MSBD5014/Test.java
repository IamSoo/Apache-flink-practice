package MSBD5014;

import javax.sound.midi.Soundbank;
import java.util.stream.IntStream;

public class Test {

    public static void main(String[] args) {
        int a =5;
        int b = 3;
        int n = 5;
       IntStream.range(0,n)
                .map( x -> {
                    int sum = a;
                    for(int i = 0 ; i <= x ; i ++){
                        sum += ((int)Math.pow(2,i))* b;
                    }
                    return sum;
                }).forEach(i1 -> System.out.printf(String.valueOf(i1)+" "));
    }

}
