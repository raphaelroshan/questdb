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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.cairo.wal.MetadataService;
import io.questdb.tasks.TableWriterTask;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * The async command-queue is a per-writer, in-memory ring buffer that lives and dies with the
 * writer; a writer closed without draining it (e.g. a distressed teardown skips tick()) drops
 * whatever is queued. {@link AsyncWriterCommand#abandon()} gives a fire-and-forget producer a
 * cleanup signal so it can release external state (e.g. a single-flight slot) rather than leak it.
 * These tests pin that {@code doClose} notifies queued commands via {@code abandon()} and that a
 * command applied by {@code tick()} is never abandoned.
 */
public class AsyncWriterCommandAbandonTest extends AbstractCairoTest {

    @Test
    public void testCloseWithoutTickAbandonsQueuedCommand() throws Exception {
        // An off-pool writer's close() runs doClose directly (no pool-return tick), so a command
        // published but never drained is orphaned -- exactly the distressed-teardown shape.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY");
            final TableToken token = engine.verifyTableName("tbl");
            final AtomicInteger abandoned = new AtomicInteger();
            final AtomicInteger applied = new AtomicInteger();
            try (TableWriter writer = newOffPoolWriter("tbl")) {
                writer.publishAsyncWriterCommand(new ProbeCommand(token, abandoned, applied));
                // Queued, not yet drained.
                Assert.assertEquals(0, abandoned.get());
                Assert.assertEquals(0, applied.get());
            }
            Assert.assertEquals("abandon() must fire once for the orphaned queued command", 1, abandoned.get());
            Assert.assertEquals("a closed writer must never apply the orphaned command", 0, applied.get());
        });
    }

    @Test
    public void testTickAppliesQueuedCommandWithoutAbandon() throws Exception {
        // The counterpart: a command drained by tick() is applied, never abandoned, so the
        // close-time cancel cannot double-handle a command the writer already processed.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY");
            final TableToken token = engine.verifyTableName("tbl");
            final AtomicInteger abandoned = new AtomicInteger();
            final AtomicInteger applied = new AtomicInteger();
            try (TableWriter writer = newOffPoolWriter("tbl")) {
                writer.publishAsyncWriterCommand(new ProbeCommand(token, abandoned, applied));
                writer.tick();
                Assert.assertEquals("tick() applies the queued command", 1, applied.get());
                Assert.assertEquals("an applied command is not abandoned", 0, abandoned.get());
            }
            // Already drained, so close() finds nothing to abandon.
            Assert.assertEquals(0, abandoned.get());
        });
    }

    /**
     * Minimal {@link AsyncWriterCommand} that round-trips through the task's stored producer
     * reference (newInstance() defaults to null) and records whether it was applied or abandoned.
     */
    private static final class ProbeCommand implements AsyncWriterCommand {
        private static final int PROBE_CMD_TYPE = 10_001;
        private final AtomicInteger abandoned;
        private final AtomicInteger applied;
        private final TableToken tableToken;
        private long correlationId;

        private ProbeCommand(TableToken tableToken, AtomicInteger abandoned, AtomicInteger applied) {
            this.tableToken = tableToken;
            this.abandoned = abandoned;
            this.applied = applied;
        }

        @Override
        public void abandon() {
            abandoned.incrementAndGet();
        }

        @Override
        public long apply(MetadataService svc, boolean contextAllowsAnyStructureChanges) {
            applied.incrementAndGet();
            return 0;
        }

        @Override
        public void close() {
        }

        @Override
        public AsyncWriterCommand deserialize(TableWriterTask task) {
            return this;
        }

        @Override
        public int getCmdType() {
            return PROBE_CMD_TYPE;
        }

        @Override
        public String getCommandName() {
            return "PROBE";
        }

        @Override
        public long getCorrelationId() {
            return correlationId;
        }

        @Override
        public int getTableId() {
            return tableToken.getTableId();
        }

        @Override
        public int getTableNamePosition() {
            return 0;
        }

        @Override
        public TableToken getTableToken() {
            return tableToken;
        }

        @Override
        public long getTableVersion() {
            return 0;
        }

        @Override
        public boolean isStructural() {
            return false;
        }

        @Override
        public void serialize(TableWriterTask task) {
            task.of(PROBE_CMD_TYPE, tableToken.getTableId(), tableToken);
            task.setInstance(correlationId);
            task.setAsyncWriterCommand(this);
        }

        @Override
        public void setCommandCorrelationId(long correlationId) {
            this.correlationId = correlationId;
        }

        @Override
        public void startAsync() {
        }
    }
}
