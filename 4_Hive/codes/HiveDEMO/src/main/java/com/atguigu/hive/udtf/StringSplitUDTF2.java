package com.atguigu.hive.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义UDTF函数
 *
 * 需要继承 GenericUDTF类，并重写抽象方法
 *
 * 函数的使用: select myudtf('hive,hadoop,flume,kafka',',');
 *      结果:  word
 *
 *            hive
 *            hadoop
 *            flume
 *            kafka
 *
 *
 *  扩展:
 *  select myudtf2('hadoop-niupi,java-lihai,songsong-kuai,dahai-lang',',','-');
 *  结果: word1    word2
 *
 *       hadoop   niupi
 *       java     lihai
 *       songsong kuai
 *       dahai    lang
 */
public class StringSplitUDTF2 extends GenericUDTF {

    private ArrayList<String> outList  = new ArrayList<>();
    /**
     * 初始化方法  约定函数的返回值类型  和 函数的返回值列名
     * @param argOIs
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //基本判断
        List<? extends StructField> fieldRefs = argOIs.getAllStructFieldRefs();
        if(fieldRefs.size()!=3){
            throw new UDFArgumentException("Input Args Length Error!!!");
        }

        //约定函数返回的列的名字
        ArrayList<String> fieldNames = new ArrayList<>();
        fieldNames.add("word1");
        fieldNames.add("word2");
        //约定函数返回的列的类型
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * 函数的逻辑处理
     * @param args
     * @throws HiveException
     */
    @Override
    public void process(Object[] args) throws HiveException {
        //获取参数
        String argsData = args[0].toString(); // hadoop-niupi,java-lihai,songsong-kuai,dahai-lang
        String rowSplit = args[1].toString(); // ,
        String colSplit = args[2].toString(); // -
        //切分数据
        String[] rows = argsData.split(rowSplit);
        for (String row : rows) {
            //因为集合是复用的，使用前先清空
            outList.clear();
            // row : hadoop-niupi
            String[] cols = row.split(colSplit);
            for (String word : cols) {
                outList.add(word);
            }
            //写出
            forward(outList);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
