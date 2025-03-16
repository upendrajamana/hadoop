import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.bloom.BloomFilter;

import java.io.IOException;

public class BloomFilterReducer extends Reducer<Text, BloomFilter, Text, BloomFilter> {
    private BloomFilter combinedBloomFilter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Initialize the combined Bloom filter with the same parameters as the mapper
        int vectorSize = 1000;  // Size of the bit vector
        int nbHash = 5;         // Number of hash functions
        combinedBloomFilter = new BloomFilter(vectorSize, nbHash, 0);
    }

    public void reduce(Text key, Iterable<BloomFilter> values, Context context)
            throws IOException, InterruptedException {
        for (BloomFilter bloomFilter : values) {
            // Combine all Bloom filters into one
            combinedBloomFilter.or(bloomFilter);
        }
        // Emit the combined Bloom filter
        context.write(key, combinedBloomFilter);
    }
}
