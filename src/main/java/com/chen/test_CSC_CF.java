package com.chen;

import java.io.IOException;
import java.util.List;

public class test_CSC_CF {
    public static void main(String[] args) throws IOException {
        String filePath = "D:\\Code\\Idea_Codes\\CSC_FILE\\CSC_inputfiles.txt";

        List<String> inputFiles = Utils.readInputFiles(filePath);

        int n=100;//数据集数量
//        int b=n/10;//每个repetition的分区数量
        int b=3;//每个repetition的分区数量
        int r=4;//repetition 数量

        //15个数据集，元素的最大频数是127，两个桶的slot数量之和需要大于127，这里设置为90
        int bucket_num=(int) Math.pow(2, 19);
        int slot_num=90;
        float fpr=0.01F;
        int fingerprint_length=10;

        //原始构建
//        long startbuild=System.nanoTime();
//
//        CSC_CF_Index.BuildIndex(r,b,bucket_num,slot_num,fingerprint_length,inputFiles);
//
//        long endbuild=System.nanoTime();
//        long buildtime=(endbuild-startbuild)/1_000_000_000;
//        System.out.println("构建时间"+buildtime+"s");
//
//        String serializeFile="D:/Code/Idea_Codes/CSC_FILE/serializeFIle"+"/"+"CSC_CF_index.ser";
//        CSC_CF_Index.serialize(serializeFile);

        //反序列化构建
        long startbuild=System.nanoTime();

        CSC_CF_Index.BuildFromSER();

        long endbuild=System.nanoTime();
        long buildtime=(endbuild-startbuild)/1_000_000_000;
        System.out.println("构建时间"+buildtime+"s");

        String serializeFile="D:/Code/Idea_Codes/CSC_FILE/serializeFIle"+"/"+"CSC_CF_index.ser";
        CSC_CF_Index.serialize(serializeFile);


//        for(int i=0;i<r;i++){
//            System.out.println(r+":占用率");
//            System.out.println(CSC_CF_Index.getindex().getCuckooFilterList().get(i).calculateOccupancyRate());
//            System.out.println("插入元素个数"+CSC_CF_Index.getindex().getCuckooFilterList().get(i).getcounterandcap()[0]);
//            System.out.println("容量"+CSC_CF_Index.getindex().getCuckooFilterList().get(i).getcounterandcap()[1]);
//        }
////
////        //查询
//        long startquery=System.nanoTime();
//        CSC_CF_Index.queryFile("D:\\Code\\Idea_Codes\\CSC_FILE\\query.txt");
//        long endquery=System.nanoTime();
//        long querytime=(endquery-startquery)/1_000_000;
//        System.out.println("查询时间"+querytime+"ms");

        CSC_CF_Index.querySequence("ATTTATTTTCAAATAACTATGATAATAGTTCAATGTGTAACCCTTTATTTTTATTTGGTAAAGTTGGTGTTGGTAAAACGCATATCGTGGCTGCTGCTGGTAATCGTTTTGCTAATAGTAATCCTAATTTAAAAATTTATTATTATGAAGGGCAAGATTTTTTT");
    }
}
