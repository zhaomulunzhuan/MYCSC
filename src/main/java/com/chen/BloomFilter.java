package com.chen;

import com.google.common.hash.Hashing;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class BloomFilter implements Serializable {
    // 实现 Serializable 接口
    private static final long serialVersionUID = 1L;
    private int n; // size
    private int k;
    private BitArray m_bits;

    public BloomFilter(int sz, int _k) {
        this.n = sz;
        this.k = _k;
        this.m_bits = new BitArray(sz);
    }

    public BitArray getM_bits(){
        return m_bits;
    }

    public void insert(List<Integer> a) {//a是索引数组
        for (int idx : a) {
            m_bits.set(idx, true);
        }
    }

    public boolean test(List<Integer> a) {//检查元素是否存在，a是元素对应的索引位置数组
        for (int idx : a) {
            if (!m_bits.get(idx)) {
                return false;
            }
        }
        return true;
    }

    public void serializeBF(Path BF_file) throws Exception {//序列化
        m_bits.serializeBitAr(BF_file);
    }

    public void deserializeBF(Path BF_file) throws Exception {//反序列化
        m_bits.deserializeBitAr(BF_file);
    }

    public static List<Integer> myhash(String key, int len, int k, int range, int seed) {
        List<Integer> hashvals = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            // Calculate MurmurHash3 hash value
            int hash = Hashing.murmur3_32(seed * k + i)
                    .hashString(key, StandardCharsets.UTF_8)
                    .asInt() % range;
            hashvals.add(hash);
        }
        return hashvals;
    }
}
