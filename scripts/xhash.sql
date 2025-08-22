-- Spark Compatible Hash implementation

create or replace function xhash_int(value int, seed int default 42) 
returns int
language java
runtime_version = 17
handler='SparkHashPureJava.hashInt'
as $$
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class SparkHashPureJava {
    private static final int SEED = 42;

    // Spark Murmur3 constants
    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;

    private static int mixK1(int k1) {
        k1 *= C1;
        k1 = Integer.rotateLeft(k1, 15);
        k1 *= C2;
        return k1;
    }

    private static int mixH1(int h1, int k1) {
        h1 ^= k1;
        h1 = Integer.rotateLeft(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;
        return h1;
    }

    private static int fmix(int h1, int length) {
        h1 ^= length;
        h1 ^= (h1 >>> 16);
        h1 *= 0x85ebca6b;
        h1 ^= (h1 >>> 13);
        h1 *= 0xc2b2ae35;
        h1 ^= (h1 >>> 16);
        return h1;
    }

    // Hash int
    public static int hashInt(int value, int seed) {
        int k1 = mixK1(value);
        int h1 = mixH1(seed, k1);
        return fmix(h1, 4);
    }

    // Hash long
    public static int hashLong(long value, int seed) {
        int low = (int) value;
        int high = (int) (value >>> 32);
        int h1 = mixH1(seed, mixK1(low));
        h1 = mixH1(h1, mixK1(high));
        return fmix(h1, 8);
    }

    // Hash float
    public static int hashFloat(float value, int seed) {
        return hashInt(Float.floatToIntBits(value), seed);
    }

    // Hash double
    public static int hashDouble(double value, int seed) {
        return hashLong(Double.doubleToLongBits(value), seed);
    }

    // Hash string like Spark
    public static int hashString(String s, int seed) {
        if (s == null) return seed;
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        int h1 = seed;

        int nblocks = bytes.length / 4;
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        for (int i = 0; i < nblocks; i++) {
            int k1 = bb.getInt(i * 4);
            h1 = mixH1(h1, mixK1(k1));
        }

        // tail
        int tailStart = nblocks * 4;
        int k1 = 0;
        int remaining = bytes.length & 3;
        if (remaining == 3) k1 ^= (bytes[tailStart + 2] & 0xff) << 16;
        if (remaining >= 2) k1 ^= (bytes[tailStart + 1] & 0xff) << 8;
        if (remaining >= 1) k1 ^= (bytes[tailStart] & 0xff);
        if (k1 != 0) h1 = mixH1(h1, mixK1(k1));

        return fmix(h1, bytes.length);
    }
}
$$;


create or replace function xhash_long(value int, seed int default 42) 
returns int
language java
runtime_version = 17
handler='SparkHashPureJava.hashLong'
as $$
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class SparkHashPureJava {
    private static final int SEED = 42;

    // Spark Murmur3 constants
    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;

    private static int mixK1(int k1) {
        k1 *= C1;
        k1 = Integer.rotateLeft(k1, 15);
        k1 *= C2;
        return k1;
    }

    private static int mixH1(int h1, int k1) {
        h1 ^= k1;
        h1 = Integer.rotateLeft(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;
        return h1;
    }

    private static int fmix(int h1, int length) {
        h1 ^= length;
        h1 ^= (h1 >>> 16);
        h1 *= 0x85ebca6b;
        h1 ^= (h1 >>> 13);
        h1 *= 0xc2b2ae35;
        h1 ^= (h1 >>> 16);
        return h1;
    }

    // Hash int
    public static int hashInt(int value, int seed) {
        int k1 = mixK1(value);
        int h1 = mixH1(seed, k1);
        return fmix(h1, 4);
    }

    // Hash long
    public static int hashLong(long value, int seed) {
        int low = (int) value;
        int high = (int) (value >>> 32);
        int h1 = mixH1(seed, mixK1(low));
        h1 = mixH1(h1, mixK1(high));
        return fmix(h1, 8);
    }

    // Hash float
    public static int hashFloat(float value, int seed) {
        return hashInt(Float.floatToIntBits(value), seed);
    }

    // Hash double
    public static int hashDouble(double value, int seed) {
        return hashLong(Double.doubleToLongBits(value), seed);
    }

    // Hash string like Spark
    public static int hashString(String s, int seed) {
        if (s == null) return seed;
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        int h1 = seed;

        int nblocks = bytes.length / 4;
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        for (int i = 0; i < nblocks; i++) {
            int k1 = bb.getInt(i * 4);
            h1 = mixH1(h1, mixK1(k1));
        }

        // tail
        int tailStart = nblocks * 4;
        int k1 = 0;
        int remaining = bytes.length & 3;
        if (remaining == 3) k1 ^= (bytes[tailStart + 2] & 0xff) << 16;
        if (remaining >= 2) k1 ^= (bytes[tailStart + 1] & 0xff) << 8;
        if (remaining >= 1) k1 ^= (bytes[tailStart] & 0xff);
        if (k1 != 0) h1 = mixH1(h1, mixK1(k1));

        return fmix(h1, bytes.length);
    }
}
$$;





create or replace function xhash_float(value float, seed int default 42) 
returns int
language java
runtime_version = 17
handler='SparkHashPureJava.hashFloat'
as $$
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class SparkHashPureJava {
    private static final int SEED = 42;

    // Spark Murmur3 constants
    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;

    private static int mixK1(int k1) {
        k1 *= C1;
        k1 = Integer.rotateLeft(k1, 15);
        k1 *= C2;
        return k1;
    }

    private static int mixH1(int h1, int k1) {
        h1 ^= k1;
        h1 = Integer.rotateLeft(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;
        return h1;
    }

    private static int fmix(int h1, int length) {
        h1 ^= length;
        h1 ^= (h1 >>> 16);
        h1 *= 0x85ebca6b;
        h1 ^= (h1 >>> 13);
        h1 *= 0xc2b2ae35;
        h1 ^= (h1 >>> 16);
        return h1;
    }

    // Hash int
    public static int hashInt(int value, int seed) {
        int k1 = mixK1(value);
        int h1 = mixH1(seed, k1);
        return fmix(h1, 4);
    }

    // Hash long
    public static int hashLong(long value, int seed) {
        int low = (int) value;
        int high = (int) (value >>> 32);
        int h1 = mixH1(seed, mixK1(low));
        h1 = mixH1(h1, mixK1(high));
        return fmix(h1, 8);
    }

    // Hash float
    public static int hashFloat(float value, int seed) {
        return hashInt(Float.floatToIntBits(value), seed);
    }

    // Hash double
    public static int hashDouble(double value, int seed) {
        return hashLong(Double.doubleToLongBits(value), seed);
    }

    // Hash string like Spark
    public static int hashString(String s, int seed) {
        if (s == null) return seed;
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        int h1 = seed;

        int nblocks = bytes.length / 4;
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        for (int i = 0; i < nblocks; i++) {
            int k1 = bb.getInt(i * 4);
            h1 = mixH1(h1, mixK1(k1));
        }

        // tail
        int tailStart = nblocks * 4;
        int k1 = 0;
        int remaining = bytes.length & 3;
        if (remaining == 3) k1 ^= (bytes[tailStart + 2] & 0xff) << 16;
        if (remaining >= 2) k1 ^= (bytes[tailStart + 1] & 0xff) << 8;
        if (remaining >= 1) k1 ^= (bytes[tailStart] & 0xff);
        if (k1 != 0) h1 = mixH1(h1, mixK1(k1));

        return fmix(h1, bytes.length);
    }
}
$$;


create or replace function xhash_double(value double, seed int default 42) 
returns int
language java
runtime_version = 17
handler='SparkHashPureJava.hashDouble'
as $$
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class SparkHashPureJava {
    private static final int SEED = 42;

    // Spark Murmur3 constants
    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;

    private static int mixK1(int k1) {
        k1 *= C1;
        k1 = Integer.rotateLeft(k1, 15);
        k1 *= C2;
        return k1;
    }

    private static int mixH1(int h1, int k1) {
        h1 ^= k1;
        h1 = Integer.rotateLeft(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;
        return h1;
    }

    private static int fmix(int h1, int length) {
        h1 ^= length;
        h1 ^= (h1 >>> 16);
        h1 *= 0x85ebca6b;
        h1 ^= (h1 >>> 13);
        h1 *= 0xc2b2ae35;
        h1 ^= (h1 >>> 16);
        return h1;
    }

    // Hash int
    public static int hashInt(int value, int seed) {
        int k1 = mixK1(value);
        int h1 = mixH1(seed, k1);
        return fmix(h1, 4);
    }

    // Hash long
    public static int hashLong(long value, int seed) {
        int low = (int) value;
        int high = (int) (value >>> 32);
        int h1 = mixH1(seed, mixK1(low));
        h1 = mixH1(h1, mixK1(high));
        return fmix(h1, 8);
    }

    // Hash float
    public static int hashFloat(float value, int seed) {
        return hashInt(Float.floatToIntBits(value), seed);
    }

    // Hash double
    public static int hashDouble(double value, int seed) {
        return hashLong(Double.doubleToLongBits(value), seed);
    }

    // Hash string like Spark
    public static int hashString(String s, int seed) {
        if (s == null) return seed;
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        int h1 = seed;

        int nblocks = bytes.length / 4;
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        for (int i = 0; i < nblocks; i++) {
            int k1 = bb.getInt(i * 4);
            h1 = mixH1(h1, mixK1(k1));
        }

        // tail
        int tailStart = nblocks * 4;
        int k1 = 0;
        int remaining = bytes.length & 3;
        if (remaining == 3) k1 ^= (bytes[tailStart + 2] & 0xff) << 16;
        if (remaining >= 2) k1 ^= (bytes[tailStart + 1] & 0xff) << 8;
        if (remaining >= 1) k1 ^= (bytes[tailStart] & 0xff);
        if (k1 != 0) h1 = mixH1(h1, mixK1(k1));

        return fmix(h1, bytes.length);
    }
}
$$;

create or replace function xhash_string(value string, seed int default 42) 
returns int
language java
runtime_version = 17
handler='SparkHashPureJava.hashString'
as $$
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class SparkHashPureJava {
    private static final int SEED = 42;

    // Spark Murmur3 constants
    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;

    private static int mixK1(int k1) {
        k1 *= C1;
        k1 = Integer.rotateLeft(k1, 15);
        k1 *= C2;
        return k1;
    }

    private static int mixH1(int h1, int k1) {
        h1 ^= k1;
        h1 = Integer.rotateLeft(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;
        return h1;
    }

    private static int fmix(int h1, int length) {
        h1 ^= length;
        h1 ^= (h1 >>> 16);
        h1 *= 0x85ebca6b;
        h1 ^= (h1 >>> 13);
        h1 *= 0xc2b2ae35;
        h1 ^= (h1 >>> 16);
        return h1;
    }

    // Hash int
    public static int hashInt(int value, int seed) {
        int k1 = mixK1(value);
        int h1 = mixH1(seed, k1);
        return fmix(h1, 4);
    }

    // Hash long
    public static int hashLong(long value, int seed) {
        int low = (int) value;
        int high = (int) (value >>> 32);
        int h1 = mixH1(seed, mixK1(low));
        h1 = mixH1(h1, mixK1(high));
        return fmix(h1, 8);
    }

    // Hash float
    public static int hashFloat(float value, int seed) {
        return hashInt(Float.floatToIntBits(value), seed);
    }

    // Hash double
    public static int hashDouble(double value, int seed) {
        return hashLong(Double.doubleToLongBits(value), seed);
    }

    // Hash string like Spark
    public static int hashString(String s, int seed) {
        if (s == null) return seed;
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        int h1 = seed;

        int nblocks = bytes.length / 4;
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        for (int i = 0; i < nblocks; i++) {
            int k1 = bb.getInt(i * 4);
            h1 = mixH1(h1, mixK1(k1));
        }

        // tail
        int tailStart = nblocks * 4;
        int k1 = 0;
        int remaining = bytes.length & 3;
        if (remaining == 3) k1 ^= (bytes[tailStart + 2] & 0xff) << 16;
        if (remaining >= 2) k1 ^= (bytes[tailStart + 1] & 0xff) << 8;
        if (remaining >= 1) k1 ^= (bytes[tailStart] & 0xff);
        if (k1 != 0) h1 = mixH1(h1, mixK1(k1));

        return fmix(h1, bytes.length);
    }
}
$$;


select xhash_int(123), xhash_long(123), xhash_float(123.0), xhash_double(123.0), xhash_string('123');
