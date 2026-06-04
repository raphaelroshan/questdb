/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cairo;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * The WAL apply stamps each native partition's last-modifying seqTxn into the _txn parquet-file-size
 * word (bit 63 UPLOADED masked off), so every instance holds a deterministic per-partition version.
 * Converting a partition to parquet overwrites the slot with the file size.
 */
public class NativePartitionSeqTxnTest extends AbstractCairoTest {

    @Test
    public void testChangeColumnTypeOnUploadedNativePartitionClearsUploaded() throws Exception {
        // A native partition with UPLOADED set has its bytes rewritten under a new writer index by
        // ALTER COLUMN TYPE. The changeColumnType pre-pass must clear UPLOADED (reset the slot to -1)
        // so the partition re-uploads, rather than a cold read mapping the new index to a missing
        // parquet field id and decoding the column as NULL.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10), ('2024-01-01T01:00:00', 20)");
            drainWalQueue();
            // day2 becomes the active partition, leaving day1 (index 0) non-active and stamped.
            execute("INSERT INTO t VALUES ('2024-01-02T00:00:00', 30)");
            drainWalQueue();

            // Stage day1 into the uploaded-while-native state on the physical writer.
            try (TableWriter writer = getWriter("t")) {
                TxWriter tx = writer.getTxWriter();
                Assert.assertFalse(tx.isPartitionParquet(0));
                Assert.assertTrue("day1 must be stamped before staging UPLOADED", tx.getNativePartitionSeqTxn(0) > 0);
                tx.setPartitionUploaded(0, true);
                Assert.assertTrue(tx.isPartitionUploaded(0));
                writer.bumpPartitionTableVersion();
                writer.commit();
            }

            execute("ALTER TABLE t ALTER COLUMN x TYPE LONG");
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                Assert.assertFalse("ALTER COLUMN TYPE must clear UPLOADED on the rewritten native partition",
                        tx.isPartitionUploaded(0));
                Assert.assertEquals("the slot resets to the unknown-version sentinel",
                        -1L, tx.getNativePartitionSeqTxn(0));
                Assert.assertFalse(tx.isPartitionParquet(0));
            }

            // The rewritten column reads back its values (cast to LONG), not NULL.
            assertSql(
                    "ts\tx\n" +
                            "2024-01-01T00:00:00.000000Z\t10\n" +
                            "2024-01-01T01:00:00.000000Z\t20\n" +
                            "2024-01-02T00:00:00.000000Z\t30\n",
                    "SELECT * FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testConvertToParquetOverwritesSeqTxnWithFileSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1), ('2024-01-01T01:00:00', 2)");
            drainWalQueue();
            // day2 active so day1 is a convertible, non-active partition.
            execute("INSERT INTO t VALUES ('2024-01-02T00:00:00', 3)");
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                Assert.assertTrue("day1 native partition is stamped",
                        reader.getTxFile().getNativePartitionSeqTxn(0) > 0);
            }

            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                Assert.assertTrue("day1 converted to parquet", tx.isPartitionParquet(0));
                // The slot now holds the parquet file size (KBs), not the small seqTxn.
                Assert.assertTrue("slot now holds the parquet file size", tx.getPartitionParquetFileSize(0) > 0);
            }
        });
    }

    @Test
    public void testShowPartitionsDoesNotLeakSeqTxnAsFileSize() throws Exception {
        // Reader regression: every native partition now carries a non-(-1) seqTxn in offset 3, but
        // table_partitions gates the parquet-file-size read on the format bit, so it must still show
        // -1 for native partitions, never the seqTxn.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1)");
            drainWalQueue();
            execute("INSERT INTO t VALUES ('2024-01-02T00:00:00', 2)");
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                Assert.assertTrue(tx.getNativePartitionSeqTxn(0) > 0);
                Assert.assertTrue(tx.getNativePartitionSeqTxn(1) > 0);
            }

            assertSql(
                    "index\tisParquet\tparquetFileSize\n" +
                            "0\tfalse\t-1\n" +
                            "1\tfalse\t-1\n",
                    "SELECT index, isParquet, parquetFileSize FROM table_partitions('t')"
            );

            assertSql(
                    "ts\tx\n" +
                            "2024-01-01T00:00:00.000000Z\t1\n" +
                            "2024-01-02T00:00:00.000000Z\t2\n",
                    "SELECT * FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testWalAppendStampsActivePartitionSeqTxn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1)");
            drainWalQueue();
            execute("INSERT INTO t VALUES ('2024-01-01T01:00:00', 2)");
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                Assert.assertEquals(1, tx.getPartitionCount());
                Assert.assertFalse(tx.isPartitionParquet(0));
                Assert.assertFalse(tx.isPartitionUploaded(0));
                // every commit appends to the active partition and stamps it with the committed seqTxn
                Assert.assertTrue(tx.getSeqTxn() > 0);
                Assert.assertEquals(tx.getSeqTxn(), tx.getNativePartitionSeqTxn(0));
            }
        });
    }

    @Test
    public void testWalO3MutateAdvancesPartitionSeqTxn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // separate commits so each day is stamped as the active partition at its own commit
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1)");
            drainWalQueue();
            execute("INSERT INTO t VALUES ('2024-01-02T00:00:00', 2)");
            drainWalQueue();
            execute("INSERT INTO t VALUES ('2024-01-03T00:00:00', 3)");
            drainWalQueue();

            long day1SeqTxn;
            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                Assert.assertEquals(3, tx.getPartitionCount());
                day1SeqTxn = tx.getNativePartitionSeqTxn(0);
                Assert.assertTrue(day1SeqTxn > 0);
                Assert.assertFalse(tx.isPartitionUploaded(0));
            }

            // O3 write into day1 (now far behind the active day3) rewrites it and advances its seqTxn
            execute("INSERT INTO t VALUES ('2024-01-01T05:00:00', 99)");
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                long newSeqTxn = tx.getSeqTxn();
                Assert.assertTrue("O3 commit advanced the global seqTxn", newSeqTxn > day1SeqTxn);
                Assert.assertEquals("day1 restamped to the O3 commit seqTxn", newSeqTxn, tx.getNativePartitionSeqTxn(0));
                Assert.assertFalse("a write clears UPLOADED", tx.isPartitionUploaded(0));
                Assert.assertFalse(tx.isPartitionParquet(0));
            }
        });
    }
}
