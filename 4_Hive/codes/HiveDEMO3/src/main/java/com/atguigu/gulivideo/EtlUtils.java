package com.atguigu.gulivideo;

/**
 * ETL工具类:
 *
 * gulivideo场景video数据的清洗规则:
 * 1. 数据的长度必须大于等于9
 * 2. 将每个视频的类别中的 空格去掉
 * 3. 将每个视频的关联视频id通过&拼接
 */
public class EtlUtils {

    /**
     * 原始数据:  SDNkMu8ZT68	w00dy911	630	People & Blogs	186	10181	3.49	494	257	rjnbgpPJUks udr9sLkoZ0s	3IU1GyX_zio
     */
    public  static  String etlData(String data){
        //1. 判断数据的长度
        String[] dataArray = data.split("\t");
        if(dataArray.length < 9){
            //数据不合法
            return null ;
        }
        //2. 去掉视频类别中的空格
        dataArray[3] = dataArray[3].replaceAll(" ","");
        //3. 将每个视频的关联视频id通过&拼接
        StringBuffer  sbs = new StringBuffer();
        for (int i = 0; i < dataArray.length; i++) {
           //3.1 处理关联视频之前的数据
            if(i < 9){
               if(i == dataArray.length-1){
                   //没有关联视频
                   sbs.append(dataArray[i]);
               }else{
                   sbs.append(dataArray[i]).append("\t");
               }
            }else{
                //处理关联视频
                if(i == dataArray.length -1){
                    sbs.append(dataArray[i]);
                }else{
                    sbs.append(dataArray[i]).append("&");
                }
            }
        }
        return sbs.toString();
    }

    public static void main(String[] args) {
        System.out.println(etlData("SDNkMu8ZT68\tw00dy911\t630\tPeople & Blogs\t186\t10181\t494\t257"));
    }
}




