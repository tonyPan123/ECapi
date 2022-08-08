
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;


import java.io.IOException;

    public final class GrayHDFSClient implements AutoCloseable {
        private final DFSClient c;

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
            c = new DFSClient(conf);
        }

        public void readFile(final String filePath) throws IOException {
            if (!c.exists(filePath)) {
                throw new IOException("file not exists");
            }
            final byte[] bytes = new byte[16];
            int numBytes = 0;
            final DFSInputStream in = c.open(filePath);
            while ((numBytes = in.read(bytes)) > 0) {
                // do nothing
            }
            in.close();
        }

        public void writeFile(final String filePath, final int len) throws IOException {
            final DFSOutputStream out = (DFSOutputStream) c.create(filePath, true);
            for (int i = 0; i < len; i += BLOCK.length) {
                out.write(BLOCK, 0, Math.min(len - i, BLOCK.length));
            }
            out.close();
        }

        public int getLastBlockDatanode(final String filePath) throws IOException {
            final LocatedBlocks locatedBlocks =
                    c.getLocatedBlocks(filePath, 0);
            final LocatedStripedBlock lastBlock =
                    (LocatedStripedBlock)locatedBlocks.getLastLocatedBlock();
            return lastBlock.getLocations()[0].getIpcPort();
        }


        @Override
        public void close() throws IOException {
            c.close();
        }
    }


