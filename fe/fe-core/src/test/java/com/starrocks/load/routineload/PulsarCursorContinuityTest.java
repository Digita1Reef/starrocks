// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.load.routineload;

import com.starrocks.common.Pair;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.thrift.TPulsarRLTaskProgress;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PulsarCursorContinuityTest {
    private static PulsarProgress replay(PulsarProgress progress) {
        String serialized = GsonUtils.GSON.toJson(progress);
        assertTrue(!serialized.contains("partitionToInitialPosition"), "initial markers remain transient");
        return GsonUtils.GSON.fromJson(serialized, PulsarProgress.class);
    }

    @Test
    public void retainsCommittedCursorAcrossReplay() {
        for (long initialPosition : List.of(0L, 1L)) {
            for (long backlog : List.of(0L, 1L, 1000000L)) {
                long expected = initialPosition == PulsarRoutineLoadJob.POSITION_LATEST_VAL ? -1L : initialPosition;
                PulsarProgress progress = new PulsarProgress();
                progress.unprotectUpdate(List.of("existing"), initialPosition);
                assertTrue(progress.getInitialPosition("existing") == expected, "new partition honors default");
                progress.update(new PulsarProgress(new TPulsarRLTaskProgress(Map.of("existing", backlog))));
                assertTrue(progress.getInitialPosition("existing") == -1L, "commit removes initial marker");
                for (int replayCount = 0; replayCount < 4; replayCount++) {
                    progress = replay(progress);
                    progress.unprotectUpdate(List.of("existing", "new"), initialPosition);
                    assertTrue(progress.getInitialPosition("existing") == -1L,
                            "committed partition must not seek after leader replay");
                    assertTrue(progress.getInitialPosition("new") == expected,
                            "new partition honors default after replay");
                    assertTrue(progress.getPartitionToInitialPosition(List.of("existing")).isEmpty(),
                            "renewed task carries no seek marker for committed partition");
                    assertTrue(progress.getBacklogNums().equals(List.of(backlog)), "replay preserves committed progress");
                }
                progress.unprotectUpdate(List.of("existing"), initialPosition);
                assertTrue(progress.getInitialPosition("new") == -1L, "removed partition drops transient marker");
                progress.modifyInitialPositions(List.of(Pair.create("existing", 1L)));
                progress.unprotectUpdate(List.of("existing"), initialPosition);
                assertTrue(progress.getInitialPosition("existing") == 1L, "explicit operator ALTER remains explicit");
            }
        }
        PulsarProgress fresh = new PulsarProgress();
        fresh.unprotectUpdate(List.of("new"), null);
        assertTrue(fresh.getInitialPosition("new") == -1L, "null default remains unset");
    }

    @Test
    public void latestBootstrapResumesSubscriptionWithoutSeekingBetweenEmptyTasks() {
        List<String> partitions = List.of("default", "explicit");
        PulsarProgress progress = new PulsarProgress();
        progress.addPartitionToInitialPosition(Pair.create("explicit", 0L));
        progress.unprotectUpdate(partitions, 0L);
        Map<String, Long> subscribeWithoutSeek = Map.of("default", -1L, "explicit", -1L);
        assertEquals(subscribeWithoutSeek, progress.getPartitionToInitialPosition(partitions));
        progress.update(new PulsarProgress(new TPulsarRLTaskProgress(Map.of())));
        assertEquals(subscribeWithoutSeek, progress.getPartitionToInitialPosition(partitions));
        progress = replay(progress);
        progress.unprotectUpdate(partitions, 0L);
        assertEquals(subscribeWithoutSeek, progress.getPartitionToInitialPosition(partitions));
        progress.modifyInitialPositions(List.of(Pair.create("explicit", 0L)));
        assertEquals(Map.of("default", -1L, "explicit", 0L), progress.getPartitionToInitialPosition(partitions));
    }
}
