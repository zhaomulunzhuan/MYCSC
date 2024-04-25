package com.chen;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CSC_CF implements Serializable {
    // 实现 Serializable 接口
    private static final long serialVersionUID = 1L;
    private static int R;//repetition数量
    private static int B;//每次repetition的分区数量
    private static int bucketCapacity;//桶的数量

    private static int slotNum;//一个桶中的槽的数量

    private static float FPR;//误报率

    private static int fingerpringLength;

    private static List<CuckooFilter> cuckooFilterList;
    private static List<List<Integer>> r_and_b_to_datasetindexs;

    private static List<List<Long>> idx_and_r_to_b;

    private static List<String> idx_to_name;

    private static Map<String,Integer> name_to_idx;

    public CSC_CF(int repetition_num,int partition_num,int bucketcapacity,int slotnum,int fingerprint_length){
        R=repetition_num;
        B=partition_num;
        bucketCapacity=bucketcapacity;
        slotNum=slotnum;
//        FPR=fpr;
        fingerpringLength=fingerprint_length;


        cuckooFilterList = new ArrayList<>(); // 初始化 bloomFilterList
        r_and_b_to_datasetindexs = new ArrayList<>();
        for(int i=0;i<R*B;i++){
            r_and_b_to_datasetindexs.add(new ArrayList<>());
        }
        idx_to_name=new ArrayList<>();
        name_to_idx=new HashMap<>();
        idx_and_r_to_b=new ArrayList<>();

        for(int i=0;i<R;i++){
            cuckooFilterList.add(new CuckooFilter(bucketCapacity,slotNum,fingerpringLength));
        }
    }

    // 设置 repetition 数量
    public static void setR(int repetition_num) {
        R = repetition_num;
    }

    // 设置每次 repetition 的分区数量
    public static void setB(int partition_num) {
        B = partition_num;
    }

    public void setBucketCapacity(int capacity) {
        bucketCapacity = capacity;
    }

    public void setSlotNum(int num) {
        slotNum = num;
    }

    public void setFingerprintlength(int fingerprintlength) {
        fingerpringLength=fingerprintlength;
    }

    // 设置 CuckooFilterList
    public static void setCuckooFilterList(List<CuckooFilter> filters) {
        cuckooFilterList=filters;
    }

    // 设置 r_and_b_to_datasetindexs
    public static void setRAndBToDatasetindexs(List<List<Integer>> indexes) {
        r_and_b_to_datasetindexs = indexes;
    }

    // 设置 idx_and_r_to_b
    public static void setIdxAndRToB(List<List<Long>> indexes) {
        idx_and_r_to_b = indexes;
    }

    // 设置 idx_to_name
    public static void setIdxToName(List<String> names) {
        idx_to_name = names;
    }

    // 设置 name_to_idx
    public static void setNameToIdx(Map<String, Integer> mappings) {
        name_to_idx = mappings;
    }
    public int getR() {
        return R;
    }

    public int getB() {
        return B;
    }

    public int getBucketCapacity() {
        return bucketCapacity;
    }

    public int getSlotNum() {
        return slotNum;
    }

    public float getFPR() {
        return FPR;
    }

    public Map<String, Integer> getNameToIdx(){
        return name_to_idx;
    }

    public int getIdxByName(String filename){
        return name_to_idx.get(filename);
    }

    public  List<String> getIdxToName(){
        return idx_to_name;
    }

    public String getNameByIdx(int datasetindex){
        return idx_to_name.get(datasetindex);
    }

    public List<List<Long>> getIdx_and_r_to_b(){
        return idx_and_r_to_b;
    }

    public List<List<Integer>> getRAndBToDatasetindexs() {
        return r_and_b_to_datasetindexs;
    }

    public List<CuckooFilter> getCuckooFilterList(){
        return cuckooFilterList;
    }

    public void addFileName(String filename){
        name_to_idx.put(filename,idx_to_name.size());
        idx_to_name.add(filename);
    }

    // 序列化函数
    public static void serialize(String filePath) {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath))) {
            oos.writeInt(R);
            oos.writeInt(B);
            oos.writeInt(bucketCapacity);
            oos.writeInt(slotNum);
            oos.writeFloat(fingerpringLength);
            oos.writeObject(cuckooFilterList);
            oos.writeObject(r_and_b_to_datasetindexs);
            oos.writeObject(idx_and_r_to_b);
            oos.writeObject(idx_to_name);
            oos.writeObject(name_to_idx);
            System.out.println("CSC_BF 对象已成功序列化到 " + filePath + " 文件中");
        } catch (IOException e) {
            System.err.println("序列化失败：" + e.getMessage());
        }
    }
}
