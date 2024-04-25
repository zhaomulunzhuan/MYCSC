package com.chen;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class CuckooFilter implements Serializable {
    // 实现 Serializable 接口
    private static final long serialVersionUID = 1L;
    private int capacity;//槽的总数量
    private int bucket_num;//桶的数量
    private int slotPerBucket;//每个桶中的槽的数量
    private int fingerprint_length;//指纹长度
    private Bucket[] buckets;//桶数组
    private int mask;
    private boolean isFull;
    private boolean isEmpty;
    private int counter;
    private CuckooFilter next;
    private CuckooFilter front;
    private Victim victim;
    private static final int MaxNumKicks = 500;

    public CuckooFilter(int bucket_num, int slot_num,int fingerprint_length) {
        this.bucket_num = bucket_num;
        this.fingerprint_length = fingerprint_length;
        this.capacity = bucket_num*slot_num;
        this.slotPerBucket=slot_num;
        this.buckets = new Bucket[bucket_num];
        for (int i = 0; i < bucket_num; i++) {
            buckets[i] = new Bucket(slot_num);
        }
//        this.mask = (1 << fingerprint_length) - 1;//创建一个具有 fingerprint_size 位的掩码，即一个所有位都为 1 的二进制数。
        this.isFull = false;//初始为空
        this.isEmpty = true;
        this.counter = 0;
        this.next = null;
        this.front = null;
        victim = new Victim(-1,0);
        //初始化踢出元素桶索引为-1
        //初始化踢出元素指纹，正常指纹不为0
    }

    private class Victim implements Serializable {
        // 实现 Serializable 接口
        private static final long serialVersionUID = 1L;
        int index;//桶索引
        int fingerprint;//指纹

        Victim(int index, int fingerprint) {
            this.index = index;
            this.fingerprint = fingerprint;
        }
    }

    private class Bucket implements Serializable {
        // 实现 Serializable 接口
        private static final long serialVersionUID = 1L;
        int[] slots;
        Bucket(int slotPerbucket) {
            this.slots = new int[slotPerBucket];
            // 初始化桶中的每个槽位
            for (int i = 0; i < slotPerBucket; i++) {
                this.slots[i] = 0; // 这里可以根据需要初始化为其他值
            }
        }
    }

    public int[] getcounterandcap(){
        int[] result=new int[2];
        result[0]=counter;
        result[1]=capacity;
        return result;
    }

    public boolean insert(String item) {
        int index, altIndex;
        int fingerprint;
        int[] result=generateIF(item);
        index = result[0];
        fingerprint = result[1];
        for (int count = 0; count < MaxNumKicks; count++) {
            boolean kickout = (count != 0);
            if (insertImpl(index, fingerprint, kickout)) {
                return true;
            }
            if (kickout) {
                index = victim.index;
                fingerprint = victim.fingerprint;
                altIndex = generate_altIndex(index, fingerprint);
                index = altIndex;
            } else {
                altIndex = generate_altIndex(index, fingerprint);
                index = altIndex;
            }
        }
        return false;
    }

    public boolean insert2(int first_bucket_index,int fingerprinthash) {
        int index, altIndex;
        int fingerprint;
        index = first_bucket_index;
        fingerprint = fingerprinthash;
        for (int count = 0; count < MaxNumKicks; count++) {
            boolean kickout = (count != 0);
            if (insertImpl(index, fingerprint, kickout)) {
                return true;
            }
            if (kickout) {
                index = victim.index;
                fingerprint = victim.fingerprint;
                altIndex = generate_altIndex(index, fingerprint);
                index = altIndex;
            } else {
                altIndex = generate_altIndex(index, fingerprint);
                index = altIndex;
            }
        }
        return false;
    }


    public boolean insertImpl(int index, int fingerprint, boolean kickout) {
        for (int pos = 0; pos < slotPerBucket; pos++) {
            if (read(index, pos) == 0) {
                write(index, pos, fingerprint);
                counter++;
                if (counter == capacity) {
                    isFull = true;
                }
                if (counter > 0) {
                    isEmpty = false;
                }
                return true;
            }
        }
        if (kickout) {
            Random rand = new Random();
            int j = rand.nextInt(slotPerBucket);
            victim.index = index;
            victim.fingerprint = read(index, j);
            write(index, j, fingerprint);
        }
        return false;
    }

    public boolean query(String item){
        int[] result=generateIF(item);
        int first_index = result[0];
        int  fingerprint = result[1];
        for(int i=0;i<slotPerBucket;i++){
            if (read(first_index,i)==fingerprint){
                return true;
            }
        }
        int alt_index= generate_altIndex(first_index, fingerprint);
        for(int i=0;i<slotPerBucket;i++){
            if (read(alt_index,i)==fingerprint){
                return true;
            }
        }
        return false;
    }

    public boolean query2(int first_bucket_index,int fingerprinthash){
        int first_index = first_bucket_index;
        int  fingerprint = fingerprinthash;
        for(int i=0;i<slotPerBucket;i++){
            if (read(first_index,i)==fingerprint){
                return true;
            }
        }
        int alt_index= generate_altIndex(first_index, fingerprint);
        for(int i=0;i<slotPerBucket;i++){
            if (read(alt_index,i)==fingerprint){
                return true;
            }
        }
        return false;
    }


    public int[] generateIF(String item) {
        int[] result = new int[2]; // 用于存储结果的数组，第一个元素为 index，第二个元素为 fingerprint

        // 计算 index
        long hash=bobHash(item.getBytes(StandardCharsets.UTF_8), 1234) % bucket_num;
        int first_bucket_index= (int) ((hash + bucket_num) % bucket_num);
        result[0] = first_bucket_index;

        // 计算 fingerprint
        long fingerprint_hash = bobHash(item.getBytes(StandardCharsets.UTF_8), 3456);
        fingerprint_hash &= ((1L << fingerprint_length) - 1);
        fingerprint_hash += (fingerprint_hash == 0) ? 1 : 0;
        result[1] = (int) fingerprint_hash;

        return result;
    }

    public int generate_altIndex(int index, int fingerprint) {
        long hash = (index ^ ((long)fingerprint * 0x5bd1e995L)) % bucket_num;
        int altIndex= (int) ((hash + bucket_num) % bucket_num);
//        if (altIndex==index){
//            altIndex = (altIndex+1)% bucket_num;
//        }
        return altIndex;
    }

    public double calculateOccupancyRate(){
        return  ((double) counter / capacity) * 100;
    }

    private long bobHash(byte[] data, int seed) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(data);
            md.update(ByteBuffer.allocate(4).putInt(seed).array());
            byte[] hashBytes = md.digest();
            return ByteBuffer.wrap(hashBytes).getLong();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return 0;
        }
    }

    // 读取指定位置的指纹值
    private int read(int index, int pos) {
        Bucket bucket= buckets[index];
        return bucket.slots[pos];
    }

    // 写入指定位置的指纹值
    private void write(int index, int pos, int fingerprint) {
//        System.out.println("写入"+index+" "+pos+" "+fingerprint);
        Bucket bucket= buckets[index];
        bucket.slots[pos]=fingerprint;
//        System.out.println("读取"+read(index,pos));
//        if (read(index,pos)!=fingerprint){
//            System.err.println("写入与读取不一致");
//        }
    }

    private void test_hash() throws IOException {
        CuckooFilter cuckooFilter = new CuckooFilter((int) Math.pow(2, 23),4,12);

        String inputfilePath = "D:\\Code\\Idea_Codes\\CSC_FILE\\CSC_inputfiles.txt";

        List<String> inputFiles = Utils.readInputFiles(inputfilePath);

        boolean flag=true;
        for(String  filePath:inputFiles){
            try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    int hash1 = cuckooFilter.generateIF(line)[0];
                    int fingerprint = cuckooFilter.generateIF(line)[1];
                    int hash2 = cuckooFilter.generate_altIndex(hash1, fingerprint);
                    boolean result= (hash1==cuckooFilter.generate_altIndex(hash2, fingerprint));
                    if(!result){
                        flag=false;
                        System.out.println(line);
                        System.err.println("hash1和hash2没有相互映射");
                        System.out.println("指纹"+fingerprint);
                        System.out.println("第一个候选桶哈希"+hash1);
                        System.out.println("第二个候选桶哈希"+hash2);
                        System.out.println("第一个候选桶哈希转换所得"+cuckooFilter.generate_altIndex(hash1,fingerprint));
                        System.out.println("第二个候选桶哈希转换所得"+cuckooFilter.generate_altIndex(hash2,fingerprint));
                    }
                    if (hash1==hash2){
                        flag=false;
                        System.err.println("hash1和hash2错误相等");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (flag){
            System.out.println("全部映射正确");
        }
    }




    public static void main(String[] args) throws IOException {
        CuckooFilter cuckooFilter = new CuckooFilter((int) Math.pow(2, 19),90,10);
        String inputfilePath = "D:\\Code\\Idea_Codes\\CSC_FILE\\CSC_inputfiles.txt";

        List<String> inputFiles = Utils.readInputFiles(inputfilePath);

        int insertcount=0;
        boolean insertflag=true;
        int insertfail=0;
        List<String> kmers=new ArrayList<>();
        for(String  filePath:inputFiles){
            try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if(!cuckooFilter.insert(line)){
                        System.out.println("插入失败");
                        insertflag=false;
                        insertfail++;
                        break;
                    }else {
                        kmers.add(line);
//                        System.out.println("插入成功");
                        insertcount++;
                    }
                }
                if(!insertflag){
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (insertflag){
            System.out.println("完全成功插入"+insertcount+"个元素");
            System.out.println("占用率"+cuckooFilter.calculateOccupancyRate());
        }
        System.out.println("插入成功"+insertcount+"个元素");
        System.out.println("插入失败"+insertfail+"个元素");
        double failureRate = (double) insertfail / (insertcount + insertfail) * 100;
        String formattedFailureRate = String.format("%.2f%%", failureRate);
        System.out.println("占用率"+cuckooFilter.calculateOccupancyRate());

        int querycount=0;
        int noquerycount=0;

//        for(String kmer:kmers){
//            if(cuckooFilter.query(kmer)){
//                querycount++;
//            }else{
//                noquerycount++;
//            }
//        }

        for(String  filePath:inputFiles){
            try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if(cuckooFilter.query(line)){
                        querycount++;
                    }else{
                        noquerycount++;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("查询到元素"+querycount);
        System.out.println("未查询到元素"+noquerycount);


    }


}
