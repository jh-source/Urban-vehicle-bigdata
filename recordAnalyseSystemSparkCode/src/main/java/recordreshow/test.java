package recordreshow;

import scala.Tuple2;
import scala.Tuple4;

import java.util.Iterator;

public class test {
    public static void main(String[] args) {
        setRecord st=new setRecord();
        st.setFind("33041100016539");
        st.init();
        Iterator<Tuple4<String,String,String,String>>temp=st.getMy();
        while(temp.hasNext())
        {
            Tuple4<String,String,String,String>t=temp.next();
            System.out.println("time:" + t._1() + "longtitude ;" + t._2() + "latitude:" +t._3() + "address:" + t._4());

        }
        System.out.println("***********************");
    }
}
