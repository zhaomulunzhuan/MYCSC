package com.chen;

import com.google.common.hash.Hashing;
import java.io.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class CSC_CF_Index {
    private static CSC_CF index; // 将 CSC_CF 声明为静态变量

    public static CSC_CF getindex(){
        return index;
    }
    public static void BuildIndex(int repetition_num,int partition_num,int bucketcapacity,int slotnum,int fingerprint_length, List<String> inputfiles) throws IOException {
        index=new CSC_CF(repetition_num,partition_num,bucketcapacity,slotnum,fingerprint_length);

        for(int i=0;i<inputfiles.size();i++){
            insert(inputfiles.get(i),repetition_num, partition_num,bucketcapacity,slotnum,fingerprint_length);
        }

    }

    public static void BuildFromSER(){
        String serializeFile="D:/Code/Idea_Codes/CSC_FILE/serializeFIle"+"/"+"CSC_CF_index.ser";
        index = deserialize(serializeFile);
    }

    public static void insert(String inputFile,int repetition_num,int partition_num,int bucketcapacity,int slotnum,int fingerprint_length) throws IOException {
        //获取文件名，不包含扩展名
        Path inputPath = Paths.get(inputFile);
        // 获取文件名（包含扩展名）
        String fileNameWithExtension = inputPath.getFileName().toString();
        // 获取不包含扩展名的文件名
        String fileName = fileNameWithExtension.substring(0, fileNameWithExtension.lastIndexOf('.'));

        List<Long> hashValues=new ArrayList<>();
        if(index.getNameToIdx().containsKey(fileName)){
            System.err.println(fileName+"already in CSC_CF index!");
        }else{
            index.addFileName(fileName);
            hashValues=partitionHash(fileName,repetition_num,partition_num);//在r个分区的分区号
            index.getIdx_and_r_to_b().add(hashValues);
//            System.out.println(index.getIdx_and_r_to_b());
        }

        List<String> kmers=Utils.getKmers(inputFile);

        for(int r=0;r<repetition_num;r++){
            int dataset_index=index.getIdxByName(fileName);
            int cur_partition_num= Math.toIntExact(index.getIdx_and_r_to_b().get(dataset_index).get(r));//在repetition r时dataset_index数据集的分区
            index.getRAndBToDatasetindexs().get(r*partition_num+cur_partition_num).add(dataset_index);

            CuckooFilter cur_cf=index.getCuckooFilterList().get(r);

            for(String kmer:kmers){
                int[] hashresult=cur_cf.generateIF(kmer);
                int anchor=hashresult[0];//锚点是桶索引
                int offset=cur_partition_num;
                int bucket_index=(anchor+offset)%bucketcapacity;//第一个桶索引
                int fingerprint=hashresult[1];//指纹

                if(!cur_cf.insert2(bucket_index,fingerprint)){
                    System.err.println("插入失败");
                    return;
                }

            }
        }

    }

    public static List<String> querykmer(String kmer){
//        System.out.println("查询元素"+kmer);

//        System.out.println("idx_to_name");
//        for (String name: index.getIdxToName()){
//            System.out.println(name);
//        }
//        System.out.println("name_to_idx");
//        for (Map.Entry<String, Integer> entry : index.getNameToIdx().entrySet()) {
//            System.out.println(entry.getKey() + ": " + entry.getValue());
//        }
//        System.out.println("idx_and_r_to_b");
//        System.out.println(index.getIdx_and_r_to_b());
//        System.out.println("r_and_b_to_indexs:");
//        System.out.println(index.getRAndBToDatasetindexs());

        int R= index.getR();
        int B=index.getB();
        int bucketcapacity=index.getBucketCapacity();
        int slot_num=index.getSlotNum();
        List<String> result_samples=new ArrayList<>();//结果数据集名称
        Set<Integer> result=new HashSet<>();//结果数据集索引列表
        Map<Integer,Integer> result_vote=new HashMap<>();


        for(int r=0;r<R;r++) {
            CuckooFilter cur_cf = index.getCuckooFilterList().get(r);

            int[] hashresult=cur_cf.generateIF(kmer);
            int anchor = hashresult[0];//第一个桶索引
            int fingerprint = hashresult[1];//指纹

//            System.out.println("第一个桶索引" + anchor);
//            System.out.println("指纹" + fingerprint);

            List<Boolean> cur_repetition_result = new ArrayList<>();
            //检查其后连续b个桶
            int startIndex = anchor;
            int endsIndex = anchor + B - 1;

//            System.out.println("repetition:" + r);
            for (int check_bucket = startIndex; check_bucket <= endsIndex; check_bucket++) {
//                System.out.println("检查" + check_bucket + "桶");
                if (cur_cf.query2(check_bucket, fingerprint)) {
                    cur_repetition_result.add(true);
                } else {
                    cur_repetition_result.add(false);
                }
            }
            List<Integer> candidate_dataset = new ArrayList<>();
            for (int partition_index = 0; partition_index < B; partition_index++) {
                boolean bit = cur_repetition_result.get(partition_index);
                if (bit) {//如果是1，当前r的这个分区有包含查询元素的数据集
                    candidate_dataset.addAll(index.getRAndBToDatasetindexs().get(r * B + partition_index));//r下的partition_index存储的数据集索引列表
                }
            }
            if (r == 0) {
                result.addAll(candidate_dataset);
//                System.out.println("r=0时候的结果");
//                System.out.println(result);
            } else {
                result.retainAll(candidate_dataset);
//                System.out.println("r="+r+"时候的结果");
//                System.out.println(result);
            }
        }
        for (int datasteindex : result) {
            result_samples.add(index.getNameByIdx(datasteindex));
        }
        return result_samples;
    }

    public static void querySequence(String sequence) throws IOException {//查找长序列，每个kmer都存在才报告序列存在
        int kmersize=31;//根据数据集kmer长度简单写死
        List<String> kmerList=new ArrayList<>();
        // 切割sequence并将长度为kmersize的子字符串加入kmerList
        for (int i = 0; i <= sequence.length() - kmersize; i++) {
            String kmer = sequence.substring(i, i + kmersize);
            kmerList.add(kmer);
        }

        List<String> result=new ArrayList<>(querykmer(kmerList.get(0)));
        for(String kmer:kmerList){
            result.retainAll(querykmer(kmer));
        }

        if (!result.isEmpty()){
            for (String datasetName : result) {
                System.out.println(datasetName);
            }
        }else {
            System.out.println("未查询到包含查询序列的数据集");
        }

    }

    public static void querySequence(String sequence, BufferedWriter writer) throws IOException {//查找长序列，每个kmer都存在才报告序列存在
        int kmersize=31;//根据数据集kmer长度简单写死
        List<String> kmerList=new ArrayList<>();
        // 切割sequence并将长度为kmersize的子字符串加入kmerList
        for (int i = 0; i <= sequence.length() - kmersize; i++) {
            String kmer = sequence.substring(i, i + kmersize);
            kmerList.add(kmer);
        }

        List<String> result=new ArrayList<>(querykmer(kmerList.get(0)));
        for(String kmer:kmerList){
            result.retainAll(querykmer(kmer));
        }

        writer.write("查询结果\n");
        // 将查询结果写入到结果文件
        if (!result.isEmpty()){
            for (String datasetName : result) {
                writer.write(datasetName + "\n");
            }
        }else {
            writer.write("未查询到包含查询序列的数据集"+"\n");
        }

    }

    public static void queryFile(String filePath){//一个文件中有多个查询长序列，查询每一个并把查询结果写入输出文件
        String queryresultFile = "D:/Code/Idea_Codes/CSC_FILE"+"/"+"CSC_CF_query_result.txt";//存放查询结果
        try(
                BufferedReader reader=new BufferedReader(new FileReader(filePath));
                BufferedWriter writer=new BufferedWriter(new FileWriter(queryresultFile))){
            String line;
            String sequence="";
            while ((line=reader.readLine())!=null){
                if(line.startsWith(">")){
                    //查询
                    if (!sequence.isEmpty()){
                        writer.write(sequence+"\n");
                        querySequence(sequence,writer);
                        writer.write(line+"\n");
                    }else {
                        writer.write(line+"\n");
                    }
                    sequence="";
                }else {
                    sequence+=line.trim().toUpperCase();
                }
            }
            if(!sequence.isEmpty()){
                writer.write(sequence + "\n");
                //查询最后一段序列
                querySequence(sequence,writer);
            }
        }catch (IOException e){
            System.err.println(e);
        }
    }


    public static List<Long> partitionHash(String key, int R, int B) {//分区，进行R次哈希，得到每个数据集在每个repetition存储的分区号b
        List<Long> hashValues = new ArrayList<>();

        // Iterate over k hash functions
        for (int i = 0; i < R; i++) {
            // Calculate hash using MurmurHash algorithm
            long hash = Hashing.murmur3_128(i).hashString(key, StandardCharsets.UTF_8).asLong();

            // Map hash value to the specified range
            long mappedHash = Math.abs(hash) % B;

            hashValues.add(mappedHash);
        }

        return hashValues;
    }

    public static void serialize(String filePath) {
        index.serialize(filePath);
    }

    public static CSC_CF deserialize(String filePath) {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
            int R = ois.readInt();
            int B = ois.readInt();
            int bucketcapacity = ois.readInt();
            int slotnum = ois.readInt();
            int fingerprintlength = ois.readInt();
            List<CuckooFilter> temp_cuckooFilterList = (List<CuckooFilter>) ois.readObject();
            List<List<Integer>> temp_r_and_b_to_datasetindexs = (List<List<Integer>>) ois.readObject();
            List<List<Long>> temp_idx_and_r_to_b = (List<List<Long>>) ois.readObject();
            List<String> temp_idx_to_name = (List<String>) ois.readObject();
            Map<String, Integer> temp_name_to_idx = (Map<String, Integer>) ois.readObject();

            CSC_CF index=new CSC_CF(R,B,bucketcapacity,slotnum,fingerprintlength);
            index.setR(R);
            index.setB(B);
            index.setBucketCapacity(bucketcapacity);
            index.setSlotNum(slotnum);
            index.setFingerprintlength(fingerprintlength);
            index.setCuckooFilterList(temp_cuckooFilterList);
            index.setRAndBToDatasetindexs(temp_r_and_b_to_datasetindexs);
            index.setIdxAndRToB(temp_idx_and_r_to_b);
            index.setIdxToName(temp_idx_to_name);
            index.setNameToIdx(temp_name_to_idx);

            System.out.println("成功从文件 " + filePath + " 中反序列化 CSC_CF 对象");
            return index;
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("反序列化失败：" + e.getMessage());
            return null;
        }
    }
}
