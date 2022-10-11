#include "src/storage/storage_structure/include/lists/lists.h"

#include "src/storage/storage_structure/include/lists/lists_update_iterator.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

// Note: The given nodeOffset and largeListHandle may not be connected. For example if we
// are about to read a new nodeOffset, say v5, after having read a previous nodeOffset, say v7, with
// a largeList, then the input to this function can be nodeOffset: 5 and largeListHandle containing
// information about the last portion of v7's large list. Similarly, if nodeOffset is v3 and v3
// has a small list then largeListHandle does not contain anything specific to v3 (it would likely
// be containing information about the last portion of the last large list that was read).
void Lists::readValues(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    auto& listSyncState = listHandle.listSyncState;
    if (listSyncState.getListSourceStore() == ListSourceStore::RelUpdateStore) {
        listUpdateStore->readValues(listSyncState, valueVector,
            listUpdateStore->getColIdxInFT(storageStructureIDAndFName.storageStructureID.listFileID
                                               .relPropertyListID.propertyID));
    } else {
        // If the startElementOffset is 0, it means that this is the first time that we read from
        // the list. As a result, we need to reset the cursor and mapper.
        if (listHandle.listSyncState.getStartElemOffset() == 0) {
            listHandle.resetCursorMapper(metadata, numElementsPerPage);
        }
        readFromList(valueVector, listHandle);
    }
}

void Lists::readSmallList(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    auto tmpTransaction = make_unique<Transaction>(READ_ONLY, UINT64_MAX);
    readBySequentialCopy(tmpTransaction.get(), valueVector, listHandle.cursorAndMapper.cursor,
        listHandle.cursorAndMapper.mapper);
}

void Lists::prepareCommitOrRollbackIfNecessary(bool isCommit) {
    if (isUpdateStoreEmpty()) {
        return;
    }
    // Note: We need to add this list to WAL's set of updatedLists here instead
    // of for example during WALReplayer when modifying pages for the following reason: Note that
    // until this function is called, no updates to the files of Lists has
    // been made. That is, so far there are no log records in WAL to indicate a change to this
    // Lists. Therefore suppose a transaction makes changes, which results in changes to this Lists
    // but then rolls back. Then since there are no log records, we cannot rely on the log for the
    // WALReplayer to know that we need to rollback this Lists in memory. Therefore, we need to
    // manually add this Lists to the set of Lists to rollback when the Database class calls
    // storageManager->prepareListsToCommitOrRollbackIfNecessary, which blindly calls each Lists to
    // check if they have something to commit or rollback.
    wal->addToUpdatedLists(storageStructureIDAndFName.storageStructureID.listFileID);
    auto updateItr = ListsUpdateIteratorFactory::getListsUpdateIterator(this);
    if (isCommit) {
        prepareCommit(*updateItr);
    }
    updateItr->doneUpdating();
}

void Lists::initListReadingState(
    node_offset_t nodeOffset, ListHandle& listHandle, TransactionType transactionType) {
    auto& listSyncState = listHandle.listSyncState;
    listSyncState.reset();
    listSyncState.setBoundNodeOffset(nodeOffset);
    listSyncState.setListHeader(headers->getHeader(nodeOffset));
    uint64_t numElementsInPersistentStore = getNumElementsInPersistentStore(nodeOffset);
    auto numElementsInUpdateStore =
        transactionType == WRITE ? listUpdateStore->getNumInsertedRelsForNodeOffset(nodeOffset) : 0;
    listSyncState.setNumValuesInList(numElementsInPersistentStore == 0 ?
                                         numElementsInUpdateStore :
                                         numElementsInPersistentStore);
    listSyncState.setDataToReadFromUpdateStore(numElementsInUpdateStore != 0);
    // If there's no element is persistentStore and the relUpdateStore is non-empty, we can
    // skip reading from persistentStore and start reading from relUpdateStore directly.
    listSyncState.setSourceStore(numElementsInPersistentStore == 0 && numElementsInUpdateStore > 0 ?
                                     ListSourceStore::RelUpdateStore :
                                     ListSourceStore::PersistentStore);
}

void Lists::fillInMemListsFromPersistentStore(
    CursorAndMapper& cursorAndMapper, uint64_t numElementsInPersistentStore, InMemList& inMemList) {
    uint64_t numElementsRead = 0;
    auto numElementsToRead = numElementsInPersistentStore;
    auto listData = inMemList.getListData();
    while (numElementsRead < numElementsToRead) {
        auto numElementsToReadInCurPage = min(numElementsToRead - numElementsRead,
            (uint64_t)(numElementsPerPage - cursorAndMapper.cursor.elemPosInPage));
        auto physicalPageIdx = cursorAndMapper.mapper(cursorAndMapper.cursor.pageIdx);
        auto frame = bufferManager.pin(fileHandle, physicalPageIdx);
        memcpy(listData, frame + cursorAndMapper.cursor.elemPosInPage * elementSize,
            numElementsToReadInCurPage * elementSize);
        if (inMemList.hasNullBuffer()) {
            NullMask::copyNullMask((uint64_t*)(frame + numElementsPerPage * elementSize),
                cursorAndMapper.cursor.elemPosInPage, inMemList.getNullMask(), numElementsRead,
                numElementsToReadInCurPage);
        }
        bufferManager.unpin(fileHandle, physicalPageIdx);
        numElementsRead += numElementsToReadInCurPage;
        listData += numElementsToReadInCurPage * elementSize;
        cursorAndMapper.cursor.nextPage();
    }
}

void Lists::prepareCommit(ListsUpdateIterator& listsUpdateIterator) {
    // See comments in UnstructuredPropertyLists::prepareCommit.
    for (auto updatedChunkItr = listUpdateStore->getInsertedEdgeTupleIdxes().begin();
         updatedChunkItr != listUpdateStore->getInsertedEdgeTupleIdxes().end(); ++updatedChunkItr) {
        for (auto updatedNodeOffsetItr = updatedChunkItr->second.begin();
             updatedNodeOffsetItr != updatedChunkItr->second.end(); updatedNodeOffsetItr++) {
            auto nodeOffset = updatedNodeOffsetItr->first;
            auto totalNumElements = getTotalNumElementsInList(TransactionType::WRITE, nodeOffset);
            InMemList inMemList{totalNumElements, elementSize, mayContainNulls()};
            CursorAndMapper cursorAndMapper;
            cursorAndMapper.reset(
                metadata, numElementsPerPage, headers->getHeader(nodeOffset), nodeOffset);
            auto numElementsInPersistentStore = getNumElementsInPersistentStore(nodeOffset);
            fillInMemListsFromPersistentStore(
                cursorAndMapper, numElementsInPersistentStore, inMemList);
            listUpdateStore->readToListAndUpdateOverflowIfNecessary(
                storageStructureIDAndFName.storageStructureID.listFileID,
                updatedNodeOffsetItr->second, inMemList, numElementsInPersistentStore,
                getDiskOverflowFileIfExists(), dataType, getNodeIDCompressionIfExists());
            listsUpdateIterator.updateList(nodeOffset, inMemList);
        }
    }
}

/**
 * Note: This function is called for property Lists other than STRINGS. This is called by
 * readValues, which is the main function for reading all Lists except UNSTRUCTURED
 * and NODE_ID.
 */
void Lists::readFromLargeList(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    // assumes that the associated adjList has already updated the syncState.
    auto pageCursor = PageUtils::getPageElementCursorForPos(
        listHandle.listSyncState.getStartElemOffset(), numElementsPerPage);
    auto tmpTransaction = make_unique<Transaction>(READ_ONLY, UINT64_MAX);
    readBySequentialCopy(
        tmpTransaction.get(), valueVector, pageCursor, listHandle.cursorAndMapper.mapper);
}

void Lists::readFromList(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    if (ListHeaders::isALargeList(listHandle.listSyncState.getListHeader())) {
        readFromLargeList(valueVector, listHandle);
    } else {
        readSmallList(valueVector, listHandle);
    }
}

void StringPropertyLists::readFromLargeList(
    const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    Lists::readFromLargeList(valueVector, listHandle);
    diskOverflowFile.readStringsToVector(*valueVector);
}

void StringPropertyLists::readSmallList(
    const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    Lists::readSmallList(valueVector, listHandle);
    diskOverflowFile.readStringsToVector(*valueVector);
}

void ListPropertyLists::readFromLargeList(
    const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    Lists::readFromLargeList(valueVector, listHandle);
    diskOverflowFile.readListsToVector(*valueVector);
}

void ListPropertyLists::readSmallList(
    const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    Lists::readSmallList(valueVector, listHandle);
    diskOverflowFile.readListsToVector(*valueVector);
}

void AdjLists::readValues(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    auto& listSyncState = listHandle.listSyncState;
    if (listSyncState.getListSourceStore() == ListSourceStore::PersistentStore &&
        listSyncState.getStartElemOffset() + listSyncState.getNumValuesToRead() ==
            listSyncState.getNumValuesInList()) {
        listSyncState.setSourceStore(ListSourceStore::RelUpdateStore);
    }
    if (listSyncState.getListSourceStore() == ListSourceStore::RelUpdateStore) {
        readFromRelUpdateStore(listSyncState, valueVector);
    } else {
        // If the startElemOffset is invalid, it means that we never read from the list. As a
        // result, we need to reset the cursor and mapper.
        if (listHandle.listSyncState.getStartElemOffset() == -1) {
            listHandle.resetCursorMapper(metadata, numElementsPerPage);
        }
        readFromList(valueVector, listHandle);
    }
}

unique_ptr<vector<nodeID_t>> AdjLists::readAdjacencyListOfNode(
    // We read the adjacency list of a node in 2 steps: i) we read all the bytes from the pages
    // that hold the list into a buffer; and (ii) we interpret the bytes in the buffer based on the
    // nodeIDCompressionScheme into a vector of nodeID_t.
    node_offset_t nodeOffset) {
    auto header = headers->getHeader(nodeOffset);
    CursorAndMapper cursorAndMapper;
    cursorAndMapper.reset(getListsMetadata(), numElementsPerPage, header, nodeOffset);
    // Step 1
    auto numElementsInList = getNumElementsInPersistentStore(nodeOffset);
    auto listLenInBytes = numElementsInList * elementSize;
    auto buffer = make_unique<uint8_t[]>(listLenInBytes);
    auto sizeLeftToCopy = listLenInBytes;
    auto bufferPtr = buffer.get();
    while (sizeLeftToCopy) {
        auto physicalPageIdx = cursorAndMapper.mapper(cursorAndMapper.cursor.pageIdx);
        auto sizeToCopyInPage = min(
            ((uint64_t)(numElementsPerPage - cursorAndMapper.cursor.elemPosInPage) * elementSize),
            sizeLeftToCopy);
        auto frame = bufferManager.pin(fileHandle, physicalPageIdx);
        memcpy(bufferPtr, frame + mapElementPosToByteOffset(cursorAndMapper.cursor.elemPosInPage),
            sizeToCopyInPage);
        bufferManager.unpin(fileHandle, physicalPageIdx);
        bufferPtr += sizeToCopyInPage;
        sizeLeftToCopy -= sizeToCopyInPage;
        cursorAndMapper.cursor.elemPosInPage = 0;
        cursorAndMapper.cursor.pageIdx++;
    }

    // Step 2
    unique_ptr<vector<nodeID_t>> retVal = make_unique<vector<nodeID_t>>();
    auto sizeLeftToDecompress = listLenInBytes;
    bufferPtr = buffer.get();
    while (sizeLeftToDecompress) {
        nodeID_t nodeID(0, 0);
        nodeIDCompressionScheme.readNodeID(bufferPtr, &nodeID);
        bufferPtr += nodeIDCompressionScheme.getNumBytesForNodeIDAfterCompression();
        retVal->emplace_back(nodeID);
        sizeLeftToDecompress -= nodeIDCompressionScheme.getNumBytesForNodeIDAfterCompression();
    }
    return retVal;
}

void AdjLists::readFromLargeList(
    const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    uint64_t nextPartBeginElemOffset;
    auto& listSyncState = listHandle.listSyncState;
    if (!listSyncState.hasValidRangeToRead()) {
        nextPartBeginElemOffset = 0;
    } else {
        nextPartBeginElemOffset = listSyncState.getEndElemOffset();
        listHandle.cursorAndMapper.cursor =
            PageUtils::getPageElementCursorForPos(nextPartBeginElemOffset, numElementsPerPage);
    }
    // The number of edges to read is the minimum of: (i) how may edges are left to read
    // (info.listLen - nextPartBeginElemOffset); and (ii) how many elements are left in the current
    // page that's being read (nextPartBeginElemOffset above should be set to the beginning of the
    // next page. Note that because of case (ii), this computation guarantees that what we read fits
    // into a single page. That's why we can call copyFromAPage.
    auto numValuesToCopy =
        min((uint32_t)(listSyncState.getNumValuesInList() - nextPartBeginElemOffset),
            numElementsPerPage - (uint32_t)(nextPartBeginElemOffset % numElementsPerPage));
    valueVector->state->initOriginalAndSelectedSize(numValuesToCopy);
    listSyncState.setRangeToRead(
        nextPartBeginElemOffset, valueVector->state->selVector->selectedSize);
    // map logical pageIdx to physical pageIdx
    auto physicalPageId =
        listHandle.cursorAndMapper.mapper(listHandle.cursorAndMapper.cursor.pageIdx);
    readNodeIDsFromAPageBySequentialCopy(valueVector, 0, physicalPageId,
        listHandle.cursorAndMapper.cursor.elemPosInPage, numValuesToCopy, nodeIDCompressionScheme,
        true /*isAdjLists*/);
}

// Note: This function sets the original and selected size of the DataChunk into which it will
// read a list of nodes and edges.
void AdjLists::readSmallList(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    valueVector->state->initOriginalAndSelectedSize(listHandle.listSyncState.getNumValuesInList());
    readNodeIDsBySequentialCopy(valueVector, listHandle.cursorAndMapper.cursor,
        listHandle.cursorAndMapper.mapper, nodeIDCompressionScheme, true /*isAdjLists*/);
    // We set the startIdx + numValuesToRead == numValuesInList in listSyncState to indicate to the
    // callers (e.g., the adj_list_extend or var_len_extend) that we have read the small list
    // already. This allows the callers to know when to switch to reading from the update store if
    // there is any updates.
    listHandle.listSyncState.setRangeToRead(0, listHandle.listSyncState.getNumValuesInList());
}

void AdjLists::readFromRelUpdateStore(
    ListSyncState& listSyncState, shared_ptr<ValueVector> valueVector) const {
    if (listSyncState.getStartElemOffset() + listSyncState.getNumValuesToRead() ==
            listSyncState.getNumValuesInList() ||
        !listSyncState.hasValidRangeToRead()) {
        // We have read all values from persistent store or the persistent store is empty, we should
        // reset listSyncState to indicate ranges in relUpdateStore and start reading from
        // relUpdateStore.
        listSyncState.setNumValuesInList(
            listUpdateStore->getNumInsertedRelsForNodeOffset(listSyncState.getBoundNodeOffset()));
        listSyncState.setRangeToRead(
            0, min(DEFAULT_VECTOR_CAPACITY, listSyncState.getNumValuesInList()));
    } else {
        listSyncState.setRangeToRead(listSyncState.getEndElemOffset(),
            min(DEFAULT_VECTOR_CAPACITY,
                listSyncState.getNumValuesInList() - listSyncState.getEndElemOffset()));
    }
    // Note that: we always store nbr node in the second column of factorizedTable.
    listUpdateStore->readValues(listSyncState, valueVector, 1 /* colIdx */);
}

} // namespace storage
} // namespace graphflow
