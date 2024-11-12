#include "storage/store/free_chunk_map.h"


#include "common/assert.h"
#include "common/constants.h"
#include "common/types/types.h"
#include "main/client_context.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
using namespace kuzu::common;

namespace kuzu {
namespace storage {

FreeChunkMap::FreeChunkMap()
    :maxAvailLevel(INVALID_FREE_CHUNK_LEVEL) {
    /* we initialize our vector as nullptr vector by default */
    for (int i = 0; i < MAX_FREE_CHUNK_LEVEL; i++) {
        freeChunkList.push_back(nullptr);
    }
};

FreeChunkMap::~FreeChunkMap() {
    for (int i = 0; i < MAX_FREE_CHUNK_LEVEL; i++) {
        std::unique_ptr<FreeChunkEntry> curr = std::move(freeChunkList[i]);
        while (curr != nullptr) {
            // std::move will set the original variable to nullptr.
            // automatic garbage collection will deallocate the memory.
            curr = std::move(curr->nextEntry);
        }
    }

    freeChunkList.clear();
    existingFreeChunks.clear();
    maxAvailLevel = INVALID_FREE_CHUNK_LEVEL;
}

/*
 * This function returns which FREE_CHUNK_LEVEL a given numPages belongs to.
 * For example,
 *     numPages <= 256  -> FREE_CHUNK_LEVEL_0
 *     numPages <= 512  -> FREE_CHUNK_LEVEL_256
 *     numPages <= 1024 -> FREE_CHUNK_LEVEL_512
 *     numPages <= 2048 -> FREE_CHUNK_LEVEL_1024
 *     ...
 */
FreeChunkLevel FreeChunkMap::GetChunkLevel(page_idx_t numPages)
{
    /* if numPage <= ith FreeChunkLevelPageNumLimit, we put it at FREE_CHUNK_LEVEL_i */
    for (int i = FREE_CHUNK_LEVEL_0; i < MAX_FREE_CHUNK_LEVEL; i++) {
        if (numPages <= FreeChunkLevelPageNumLimit[i]) {
            return static_cast<FreeChunkLevel>(i);
        }
    }

    /* You should never reach here since FreeChunkLevelPageNumLimit has last entry as UINT32_MAX */
    KU_ASSERT(false);
    return MAX_FREE_CHUNK_LEVEL;
}

void FreeChunkMap::UpdateMaxAvailLevel()
{
    FreeChunkLevel nextAvailLevel = INVALID_FREE_CHUNK_LEVEL;
    for (int i = maxAvailLevel; i >= FREE_CHUNK_LEVEL_0; i--) {
        if (freeChunkList[i] != nullptr) {
            nextAvailLevel = static_cast<FreeChunkLevel>(i);
            break;
        }
    }
    maxAvailLevel = nextAvailLevel;
}

/*
 * Note: Any caller of this function need to add the entry back to FreeChunkMap after use so that the rest
 * of its unused space will be reused
 */
std::unique_ptr<FreeChunkEntry> FreeChunkMap::GetFreeChunk(page_idx_t numPages)
{
    /* 0. return immediately if it does not want any pages */
    if (numPages == 0) {
        return nullptr;
    }

    /* 1. Get the corresponding ChunkLevel numPages belongs to */
    FreeChunkLevel curLevel = GetChunkLevel(numPages);
    KU_ASSERT(curLevel < MAX_FREE_CHUNK_LEVEL);

    /* 2. return nullptr if have no entry for the given level and above */
    if (maxAvailLevel < curLevel) {
        return nullptr;
    }

    /* 3. Now, search an usable entry for given numPages and timestamp unitl exceeding the max level */
    while (curLevel <= maxAvailLevel) {
        /* if the level we are searching has no space to reuse, continue search the next level */
        if (freeChunkList[curLevel] == nullptr) {
            curLevel = static_cast<FreeChunkLevel>((curLevel + 1 < maxAvailLevel)? curLevel + 1:maxAvailLevel);
            continue;
        }

        /* Search the current level for an entry with valid reuse timestamp */
        KU_ASSERT(freeChunkList[curLevel] != nullptr);
        /* curEntry and lastSearchEntry are raw pointers to traverse the linked list */
        FreeChunkEntry* curEntry = freeChunkList[curLevel].get();
        FreeChunkEntry *lastSearchEntry = nullptr;
        /* entryToReturn is the unique pointer instance to return from the function */
        std::unique_ptr<FreeChunkEntry> entryToReturn = nullptr;
        while (curEntry != nullptr) {
            if (curEntry->numPages >= numPages) {
                /* found a valid entry to reuse. Remove it from current linked list */
                if (lastSearchEntry == nullptr) {
                    /* the valid entry is the first entry in the L.L. */
                    entryToReturn = std::move(freeChunkList[curLevel]);
                    freeChunkList[curLevel] = std::move(entryToReturn->nextEntry);

                    /* update maxAvailLevel if we have removed the last entry of the max available Level */
                    if (curLevel == maxAvailLevel && freeChunkList[curLevel] == nullptr) {
                        UpdateMaxAvailLevel();
                    }
                } else {
                    /* need to unlink it from its parent */
                    entryToReturn = std::move(lastSearchEntry->nextEntry);
                    lastSearchEntry->nextEntry = std::move(entryToReturn->nextEntry);
                }
                entryToReturn->nextEntry = nullptr;
                existingFreeChunks.erase(entryToReturn->pageIdx);

                return entryToReturn;
            }

            /* Move to the next entry */
            lastSearchEntry = curEntry;
            curEntry = curEntry->nextEntry.get();
        }
    }
    
    /* No reusable chunk. Just return nullptr here */
    return nullptr;
}

void FreeChunkMap::AddFreeChunk(page_idx_t pageIdx, page_idx_t numPages)
{
    KU_ASSERT(pageIdx != INVALID_PAGE_IDX && numPages != 0);

    /* 0. Make sure we do not have duplicate entry here */
    if (existingFreeChunks.find(pageIdx) != existingFreeChunks.end()) {
        KU_ASSERT(0);
        return;
    }

    /* 1. Get the corresponding ChunkLevel numPages belongs to */
    FreeChunkLevel curLevel = GetChunkLevel(numPages);
    KU_ASSERT(curLevel < MAX_FREE_CHUNK_LEVEL);

    /* 2. Create a new FreeChunkEntry */
    FreeChunkEntry *entry = new FreeChunkEntry;
    entry->pageIdx = pageIdx;
    entry->numPages = numPages;

    /* 3. Insert it into the L.L. */
    if (freeChunkList[curLevel] == nullptr) {
        freeChunkList[curLevel] = entry;
        if (maxAvailLevel < curLevel) {
            maxAvailLevel = curLevel;
        }
    } else {
        FreeChunkEntry *curEntryInList = freeChunkList[curLevel];
        while (curEntryInList->nextEntry != nullptr) {
            curEntryInList = curEntryInList->nextEntry;
        }

        KU_ASSERT(curEntryInList != nullptr);
        curEntryInList->nextEntry = entry;
    }
    existingFreeChunks.insert(pageIdx);
}

/*
 * Serializes free chunk entry for persistence. Called from serializeVector within serialize of
 * free chunk map.
 */
void FreeChunkEntry::serialize(Serializer& serializer) {
    serializer.writeDebuggingInfo("pageIdx");
    serializer.write<page_idx_t>(pageIdx);
    serializer.writeDebuggingInfo("numPages");
    serializer.write<page_idx_t>(numPages);
    serializer.writeDebuggingInfo("nextEntry");
    serializer.serializeOptionalValue(nextEntry);
}

/*
 * Deserializes free chunk entry when restoring from checkpoint
 */
std::unique_ptr<FreeChunkEntry> FreeChunkEntry::deserialize(Deserializer& deserializer) {
    std::string str;
    page_idx_t pageIdx = INVALID_PAGE_IDX;
    page_idx_t numPages = INVALID_PAGE_IDX;
    std::unique_ptr<FreeChunkEntry> nextEntry = nullptr;
    deserializer.validateDebuggingInfo(str, "freeChunkLevel");
    deserializer.deserializeValue<page_idx_t>(pageIdx);
    deserializer.validateDebuggingInfo(str, "numPages");
    deserializer.deserializeValue<page_idx_t>(numPages);
    deserializer.validateDebuggingInfo(str, "nextEntry");
    deserializer.deserializeOptionalValue(nextEntry);
    std::unique_ptr<FreeChunkEntry> currentEntry = std::make_unique<FreeChunkEntry>();
    currentEntry->pageIdx = std::move(pageIdx);
    currentEntry->numPages = std::move(numPages);
    currentEntry->nextEntry = std::move(nextEntry);
    return currentEntry;
}


/*
 * Serializes free chunk map for persistence.
 */
void FreeChunkMap::serialize(Serializer& serializer) const
{
    serializer.writeDebuggingInfo("freeChunkLevel");
    serializer.write<FreeChunkLevel>(maxAvailLevel);
    serializer.writeDebuggingInfo("freeChunkList");
    serializer.serializeVectorOfPtrs(freeChunkList);
    serializer.writeDebuggingInfo("existingFreeChunks");
    serializer.serializeUnorderedSet(existingFreeChunks);
}

/*
 * Deserializes free chunk map when restoring from checkpoint.
 */
std::unique_ptr<FreeChunkMap> FreeChunkMap::deserialize(Deserializer& deserializer)
{
    std::string str;
    std::vector<std::unique_ptr<FreeChunkEntry>> freeChunkList;
    std::unordered_set<page_idx_t> existingFreeChunks;
    FreeChunkLevel maxAvailLevel = INVALID_FREE_CHUNK_LEVEL;
    deserializer.validateDebuggingInfo(str, "maxAvailLevel");
    deserializer.deserializeValue<FreeChunkLevel>(maxAvailLevel);
    deserializer.validateDebuggingInfo(str, "freeChunkList");
    deserializer.deserializeVectorOfPtrs(freeChunkList);
    deserializer.validateDebuggingInfo(str, "existingFreeChunks");
    deserializer.deserializeUnorderedSet(existingFreeChunks);
    std::unique_ptr<FreeChunkMap> freeChunkMap = std::make_unique<FreeChunkMap>();
    freeChunkMap->maxAvailLevel = std::move(maxAvailLevel);
    freeChunkMap->freeChunkList = std::move(freeChunkList);
    freeChunkMap->existingFreeChunks = std::move(existingFreeChunks);
    return freeChunkMap;
}

} // namespace storage
} // namespace kuzu
