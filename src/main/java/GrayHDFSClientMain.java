import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrayHDFSClientMain {

    private static final Logger LOG = LoggerFactory.getLogger(GrayHDFSClientMain.class);
    private static long t0 = System.nanoTime();

    public static void run(final String [] args) {
        LOG.info("running command " + Arrays.toString(args));
        final String confDir = args[0];
        final boolean isRead = args[1].equals("read");
        final String filename = args[2];
        final int num = Integer.parseInt(args[3]);
        int progress = 0;
        int failure = 0;
        while (progress < num) {
            try (final GrayHDFSClient client = new GrayHDFSClient(confDir)) {
                while (progress < num) {
                    final long nano = System.nanoTime();
                    if (isRead) {
                        client.readFile(filename);
                    } else {
                        client.writeFile(filename, 3_000_000);
                    }
                    progress++;
                    failure = 0;
                    LOG.info("progress = {}, time = {}", progress, System.nanoTime() - t0);
                }
            } catch (final IOException e) {
                LOG.warn("Client encounter exception", e);
                failure++;
                if (failure >= 3) {
                    break;
                }
            }
        }
    }

    public static void reproduction(final String [] args) {
        final String confDir = args[0];
        final String filename = args[1];
        try (final GrayHDFSClient client = new GrayHDFSClient(confDir)) {
            client.writeFile(filename, 3_000_000);
            Thread.sleep(10000);
            LOG.info("Corruption Target is " + client.triggerReconstruction(filename));
        } catch (Exception e) {
            LOG.warn("Client(or sleep) encounter exception", e);
        }




    }

    public static void main(final String[] args) throws IOException {
        //run(args);
        reproduction(args);
    }
}
