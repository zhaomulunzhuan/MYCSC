package com.chen;

import java.io.IOException;
import java.util.List;

public class test_CSC_BF {
    public static void main(String[] args) throws IOException {
        String filePath = "D:\\Code\\Idea_Codes\\CSC_FILE\\CSC_inputfiles.txt";

        List<String> inputFiles = Utils.readInputFiles(filePath);

        int n=100;//数据集数量
//        int b=n/10;//每个repetition的分区数量
        int b=n/10;//每个repetition的分区数量
        int r=4;//repetition 数量
        int k=3;//哈希函数个数
//        int m=268_211_884;
        int m=1_900_000_000;

        //直接构建
//        long startbuild=System.nanoTime();
//
//        CSC_BF_Index.BuildIndex(r,b,k,m,inputFiles);
//
//        long endbuild=System.nanoTime();
//        long buildtime=(endbuild-startbuild)/1_000_000_000;
//        System.out.println("构建的时间"+buildtime+"s");
//
//        String serializeFile="D:/Code/Idea_Codes/CSC_FILE/serializeFIle"+"/"+"CSC_BF_index.ser";
//        CSC_BF_Index.serialize(serializeFile);

        //从序列化文件中构建
        long startbuild=System.nanoTime();

        CSC_BF_Index.BuildFromSER();

        long endbuild=System.nanoTime();
        long buildtime=(endbuild-startbuild)/1_000_000_000;
        System.out.println("反序列化构建的时间"+buildtime+"s");


        //查询
        long startquery=System.nanoTime();
        CSC_BF_Index.queryFile("D:\\Code\\Idea_Codes\\CSC_FILE\\query.txt");
        long endquery=System.nanoTime();
        long querytime=(endquery-startquery)/1_000_000;
        System.out.println("查询时间"+querytime+"ms");


    }
}
