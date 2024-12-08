#pragma once

#include <vector>
#include <set>

#include "common/constants.h"
#include "common/types/types.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

namespace kuzu {

namespace common {
class Serializer;
class Deserializer;
} // namespace common

namespace storage {

/* Change this macro if we want to disable FCM feature */
#define ENABLE_FREE_CHUNK_MAP true
// #define ENABLE_FREE_CHUNK_MAP false

/*
 * FreeChunkLevel indicates how many pages are free to use in a corresponding FreeChunkEntry
 * Note that: these pages are consecutive in disk space and we are indicating the lower limit
 * Therefore, it is possible to waste some fragmented spaces (in FREE_CHUNK_LEVEL_0)
 */
enum FreeChunkLevel : int {
    INVALID_FREE_CHUNK_LEVEL = -1,
    FREE_CHUNK_LEVEL_0 = 0,
    FREE_CHUNK_LEVEL_2 = 1,
    FREE_CHUNK_LEVEL_4 = 2,
    FREE_CHUNK_LEVEL_8 = 3,
    FREE_CHUNK_LEVEL_16 = 4,
    FREE_CHUNK_LEVEL_32 = 5,
    FREE_CHUNK_LEVEL_64 = 6,
    FREE_CHUNK_LEVEL_128 = 7,
    MAX_FREE_CHUNK_LEVEL = 8
};

/* This const array indicates the upper limit of each level (same as the lower limit of the next level) */
const common::page_idx_t FreeChunkLevelPageNumLimit[MAX_FREE_CHUNK_LEVEL] = {
    2, 4, 8, 16, 32, 64, 128, UINT32_MAX
};

/*
 * FreeChunkEntry is the main structure to maintain free space information of each chunk:
 *   pageIdx indicates the start page of a given data chunk
 *   numPages indicates how many consecutive free pages this data chunk owns
 *   reuseTS is the latest TS when this entry is created. This is to make sure the data of corresponding
 *     data chunk is not recycled until no one keeps TS that is old enough to see it.
 * Note: reuseTS is removed in the 2nd version of implementation since flushing only happens when checkpoint and
 *       checkpoint will wait all other transactions to finish before proceeding and writing data to disk;
 *       with that saying, we are safe to reuse any recycled column chunk here without version control.
 */
typedef struct FreeChunkEntry {
    common::page_idx_t pageIdx;
    common::page_idx_t numPages;
    std::unique_ptr<FreeChunkEntry> nextEntry = nullptr;

    void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<FreeChunkEntry> deserialize(common::Deserializer &deserializer);

} FreeChunkEntry;

/*
 * FreeChunkMap is our main data structure here. It maintains a list of linked list of FreeChunkEntry
 * where the index of each L.L. indicates its FreeChunkLevel and offer necessary interface to its user.
 */
class FreeChunkMap {
public:
    FreeChunkMap();
    ~FreeChunkMap();

    /* Get a free chunk to write new data */
    std::unique_ptr<FreeChunkEntry> getFreeChunk(common::page_idx_t numPages);
    /* Add info of a recycled chunk into FreeChunkMap */
    void addFreeChunk(common::page_idx_t pageIdx, common::page_idx_t numPages);

    /* Functions used for persistency of FreeChunkMap data */
    void serialize(common::Serializer& serializer) const;
    void deserialize(common::Deserializer& deserializer);

private:
    /* Helper functions */
    FreeChunkLevel getChunkLevel(common::page_idx_t numPages);
    void updateMaxAvailLevel();

    /*
     * No need for locks here since only checkpoint will need free chunks
     * when all other transactions are blocked
     */
    std::vector<std::unique_ptr<FreeChunkEntry>> freeChunkList;
    std::unordered_set<common::page_idx_t> existingFreeChunks;
    FreeChunkLevel maxAvailLevel;
};

} // namespace storage
} // namespace kuzu
