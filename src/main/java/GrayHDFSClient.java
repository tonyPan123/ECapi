
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import java.io.IOException;

    public final class GrayHDFSClient implements AutoCloseable {
        private final FileSystem fs;

        private static final byte[] BLOCK;
        static {
            BLOCK = new byte[10000];
            for (int i = 0; i < BLOCK.length; i++) {
                BLOCK[i] = (byte)'a';
            }
        }

        public GrayHDFSClient(final String confDir) throws IOException {
            final Configuration conf = new Configuration();
            conf.addResource(new Path(confDir + "/core-site.xml"));
            conf.addResource(new Path(confDir + "/hdfs-site.xml"));
            // because of Maven
            conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", LocalFileSystem.class.getName());
            // set HADOOP user
            System.setProperty("HADOOP_USER_NAME", "gray");
            System.setProperty("hadoop.home.dir", "/");
            // get the filesystem - HDFS
            fs = FileSystem.get(conf);
        }

        public void readFile(final String filePath) throws IOException {
            final Path path = new Path(filePath);
            if (!fs.exists(path)) {
                throw new IOException("file not exists");
            }
            final byte[] bytes = new byte[16];
            int numBytes = 0;
            final FSDataInputStream in = fs.open(path);
            while ((numBytes = in.read(bytes)) > 0) {
                // do nothing
            }
            in.close();
        }

        public void writeFile(final String filePath, final int len) throws IOException {
            final Path path = new Path(filePath);
            final FSDataOutputStream out = fs.create(path, true);
            for (int i = 0; i < len; i += BLOCK.length) {
                out.write(BLOCK, 0, Math.min(len - i, BLOCK.length));
            }
            out.close();
        }

        @Override
        public void close() throws IOException {
            fs.close();
        }
    }


