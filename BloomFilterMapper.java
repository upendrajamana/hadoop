import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import java.io.IOException;

public class BloomFilterMapper extends Mapper<Object, Text, Text, BloomFilter> {
    private Text year = new Text();
    private BloomFilter bloomFilter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Initialize Bloom filter with desired parameters
        int vectorSize = 1000;  // Size of the bit vector
        int nbHash = 5;         // Number of hash functions
        bloomFilter = new BloomFilter(vectorSize, nbHash, 0);
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.length() > 0) {
            try {
                String yearString = line.substring(15, 19);  // Extracting Year (Positions 15-19)
                year.set(yearString);

                // Add the year to the Bloom filter
                bloomFilter.add(new Key(yearString.getBytes()));

                // Emit the year and the Bloom filter
                context.write(year, bloomFilter);
            } catch (StringIndexOutOfBoundsException e) {
                // Skip lines with incorrect formatting
            }
        }
    }
}
