package com.chen;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class BitArray implements Serializable {
    // 实现 Serializable 接口
    private static final long serialVersionUID = 1L;
    private byte[] A;
    private int ar_size;

    public BitArray(int size) {
        this.ar_size = size;//bit数
        this.A = new byte[(ar_size / 8) + 1];
        for (int i = 0; i < A.length; i++) {
            A[i] = 0; // Clear the bit array
        }
    }

    public byte[] getA(){
        return A;
    }

    public void ANDop(BitArray B) {//A与B按位与结果存在A里面
        if (A.length != B.A.length) {
            throw new IllegalArgumentException("BitArrays must be of equal length");
        }
        for (int i = 0; i < A.length; i++) {
            A[i] &= B.A[i];
        }
    }

    public boolean empty() {//判断bitarray为空
        for (byte b : A) {
            if (b != 0) {
                return false;
            }
        }
        return true;
    }


    public int getcount() {//统计字节数组 A 中所有字节的二进制表示中的 1 的个数总和
        int count = 0;
        for (byte b : A) {
            count += Integer.bitCount(b & 0xFF);
        }
        return count;
    }

    public void serializeBitAr(Path BF_file) throws IOException {//序列化
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(BF_file.toFile()))) {
            out.write(A);
        }
    }

    public void deserializeBitAr(Path BF_file) throws IOException {//反序列化
        try (DataInputStream in = new DataInputStream(new FileInputStream(BF_file.toFile()))) {
            in.readFully(A);
        }
    }

    public void set(int idx, boolean value) {//设置bitarray对应bit为1

        int byteIndex = idx / 8;
        int bitIndex = idx % 8;

        if (value) {
            A[byteIndex] |= (1 << bitIndex); // Set the bit at idx to 1
        } else {
            A[byteIndex] &= ~(1 << bitIndex); // Set the bit at idx to 0
        }
    }

    public boolean get(int idx) {//获取对应bit位置
        int byteIndex = idx / 8;
        int bitIndex = idx % 8;
        return ((A[byteIndex] >> bitIndex) & 1) == 1; // Check if the bit at idx is 1
    }


    public List<Boolean> getBitsInRange(int startIndex, int endIndex) {//取连续的bit
        List<Boolean> values=new ArrayList<>();
        if (startIndex < 0 || endIndex >= ar_size || startIndex > endIndex) {
            throw new IllegalArgumentException("Invalid range");
        }
        for (int i = startIndex; i <= endIndex; i++){
            values.add(get(i));
        }
        return values;
    }


}
