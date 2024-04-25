package com.chen;

import com.google.common.hash.Hashing;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class CSC_BF_Index {
    private static CSC_BF index; // 将 CSC_BF 声明为静态变量

    public static void BuildIndex(int repetition_num, int partition_num, int hash_num, int bf_size, List<String> inputfiles) throws IOException {
        index=new CSC_BF(repetition_num,partition_num,hash_num,bf_size);

        for(int i=0;i<inputfiles.size();i++){
            insert(inputfiles.get(i),repetition_num, partition_num,hash_num,bf_size);
        }

    }


    public static void BuildFromSER(){
        String serializeFile="D:/Code/Idea_Codes/CSC_FILE/serializeFIle"+"/"+"CSC_BF_index.ser";
        index = deserialize(serializeFile);
    }

    public static void insert(String inputFile,int repetition_num, int partition_num, int hash_num, int bf_size) throws IOException {
        //获取文件名，不包含扩展名
        Path inputPath = Paths.get(inputFile);
        // 获取文件名（包含扩展名）
        String fileNameWithExtension = inputPath.getFileName().toString();
        // 获取不包含扩展名的文件名
        String fileName = fileNameWithExtension.substring(0, fileNameWithExtension.lastIndexOf('.'));


        List<Long> hashValues=new ArrayList<>();
        if(index.getNameToIdx().containsKey(fileName)){
            System.err.println(fileName+"already in CSC_BF index!");
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


            BloomFilter cur_bf=index.getBloomfilterList().get(r);

            for(String kmer:kmers){
                List<Integer> anchors=kmerhash(kmer,hash_num,bf_size,r);//k个锚点哈希值  H (x)%m
                //锚点+偏移 (H (x)%m + gj (Si ))%m 是最终的哈希位置，偏移就是分区 partttion_num
                int offset=cur_partition_num;
                List<Integer> k_hashvalues=new ArrayList<>();
                for (Integer anchor : anchors) {
                    // 将锚点哈希值加上偏移量
                    int newAnchor = anchor + offset;
                    // 如果新的哈希位置超过了布隆过滤器的大小，取模处理
                    if (newAnchor >= bf_size) {
                        newAnchor %= bf_size;
                    }
                    // 将新的哈希位置存入新的列表中
                    k_hashvalues.add(newAnchor);
                }
                cur_bf.insert(k_hashvalues);

            }

        }

    }


    public static List<String> querykmer(String kmer){

        int R= index.getR();
        int B=index.getB();
        int k= index.getK();
        int m=index.getM();
        List<String> result_samples=new ArrayList<>();//结果数据集名称
        List<Integer> result=new ArrayList<>();//结果数据集索引列表
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
        for(int r=0;r<R;r++){
            BloomFilter cur_bf=index.getBloomfilterList().get(r);
            List<Integer> anchors=kmerhash(kmer,k,m,r);//k个锚点哈希值  H (x)%m
            List<Boolean> cur_repetition_result = new ArrayList<>(Collections.nCopies(B, true));//存储在这个repetition的查询结果
            for(int anchor:anchors){//一个kmer在一次repetition哈希k次
                int startIndex= anchor;
                int endsIndex = anchor + B - 1;
//                System.out.println("start"+startIndex);
//                System.out.println("end"+endsIndex);
                List<Boolean> cur_result=cur_bf.getM_bits().getBitsInRange(startIndex,endsIndex);

//                System.out.println("r:"+r+"anchor:"+anchor);
//                System.out.println(cur_result);

                // 将 cur_result 与 cur_repetition_result 进行按位与操作，并将结果保存在 cur_repetition_result 中
                // 确保长度相同
                if (cur_repetition_result.size() != cur_result.size()) {
                    throw new IllegalArgumentException("Lists cur_repetition_result and cur_result must have the same length.");
                }

                // 逐个按位进行或操作，并将结果保存到列表 cur_repetition_result 中
                for (int i = 0; i < B; i++) {
                    boolean value = cur_repetition_result.get(i) & cur_result.get(i);
                    cur_repetition_result.set(i, value);
                }
//                System.out.println("r"+r+"结果"+cur_repetition_result);
                //cur_repetition_result长度为B，bit为1表示对应分区包含查询元素
            }
            List<Integer> candidate_dataset=new ArrayList<>();
            for(int partition_index=0;partition_index<B;partition_index++){
                boolean bit=cur_repetition_result.get(partition_index);
                if(bit){//如果是1，当前r的这个分区有包含查询元素的数据集
                    candidate_dataset.addAll(index.getRAndBToDatasetindexs().get(r*B+partition_index));//r下的partition_index存储的数据集索引列表
                }
            }
            if(r==0){
                result.addAll(candidate_dataset);
//                System.out.println("r=0时候的结果");
//                System.out.println(result);
            }else{
                result.retainAll(candidate_dataset);
//                System.out.println("r="+r+"时候的结果");
//                System.out.println(result);
            }
        }
        for(int datasteindex:result){
            result_samples.add(index.getNameByIdx(datasteindex));
        }
        return result_samples;
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
        String queryresultFile = "D:/Code/Idea_Codes/CSC_FILE"+"/"+"CSC_BF_query_result.txt";//存放查询结果
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


    public static List<Integer> kmerhash(String key, int k, int range, int seed) {//对kmer计算k个哈希值
        List<Integer> hashvals = new ArrayList<>();

        XXHashFactory factory = XXHashFactory.fastestInstance();
        XXHash64 hash64 = factory.hash64();

        for (int i = 0; i < k; i++) {
            long hash = hash64.hash(
                    key.getBytes(StandardCharsets.UTF_8),
                    0,
                    key.length(),
                    seed + i * 31
            );

            // 对哈希值取模得到范围内的整数，并将其添加到结果列表中
            hashvals.add((int) (Math.abs(hash) % range));
        }

        return hashvals;
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

    // 反序列化函数
    public static CSC_BF deserialize(String filePath) {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
            int R = ois.readInt();
            int B = ois.readInt();
            int k = ois.readInt();
            int m = ois.readInt();
            List<BloomFilter> temp_bloomFilterList = (List<BloomFilter>) ois.readObject();
            List<List<Integer>> temp_r_and_b_to_datasetindexs = (List<List<Integer>>) ois.readObject();
            List<List<Long>> temp_idx_and_r_to_b = (List<List<Long>>) ois.readObject();
            List<String> temp_idx_to_name = (List<String>) ois.readObject();
            Map<String, Integer> temp_name_to_idx = (Map<String, Integer>) ois.readObject();

            CSC_BF index=new CSC_BF(R,B,k,m);
            index.setR(R);
            index.setB(B);
            index.setK(k);
            index.setM(m);
            index.setBloomFilterList(temp_bloomFilterList);
            index.setRAndBToDatasetindexs(temp_r_and_b_to_datasetindexs);
            index.setIdxAndRToB(temp_idx_and_r_to_b);
            index.setIdxToName(temp_idx_to_name);
            index.setNameToIdx(temp_name_to_idx);

            System.out.println("成功从文件 " + filePath + " 中反序列化 CSC_BF 对象");
            return index;
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("反序列化失败：" + e.getMessage());
            return null;
        }
    }


}
