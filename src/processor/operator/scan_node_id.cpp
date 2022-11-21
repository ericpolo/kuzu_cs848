#include "processor/operator/scan_node_id.h"

namespace kuzu {
namespace processor {

void ScanNodeIDSemiMask::setMask(uint64_t nodeOffset, uint8_t maskerIdx) {
    nodeMask->setMask(nodeOffset, maskerIdx, maskerIdx + 1);
    morselMask->setMask(nodeOffset >> DEFAULT_VECTOR_CAPACITY_LOG_2, maskerIdx, maskerIdx + 1);
}

pair<node_offset_t, node_offset_t> ScanTableNodeIDSharedState::getNextRangeToRead() {
    unique_lock lck{mtx};
    // Note: we use maxNodeOffset=UINT64_MAX to represent an empty table.
    if (currentNodeOffset > maxNodeOffset || maxNodeOffset == UINT64_MAX) {
        return make_pair(currentNodeOffset, currentNodeOffset);
    }
    if (semiMask) {
        auto currentMorselIdx = currentNodeOffset >> DEFAULT_VECTOR_CAPACITY_LOG_2;
        assert(currentNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
        while (currentMorselIdx <= maxMorselIdx && !semiMask->isMorselMasked(currentMorselIdx)) {
            currentMorselIdx++;
        }
        currentNodeOffset = min(currentMorselIdx * DEFAULT_VECTOR_CAPACITY, maxNodeOffset);
    }
    auto startOffset = currentNodeOffset;
    auto range = min(DEFAULT_VECTOR_CAPACITY, maxNodeOffset + 1 - currentNodeOffset);
    currentNodeOffset += range;
    return make_pair(startOffset, startOffset + range);
}

void ScanNodeIDSharedState::initialize(Transaction* transaction) {
    unique_lock lck{mtx};
    if (initialized) {
        return;
    }
    for (auto& tableState : tableStates) {
        tableState->initialize(transaction);
    }
    initialized = true;
}

tuple<ScanTableNodeIDSharedState*, node_offset_t, node_offset_t>
ScanNodeIDSharedState::getNextRangeToRead() {
    unique_lock lck{mtx};
    if (currentStateIdx == tableStates.size()) {
        return make_tuple(nullptr, INVALID_NODE_OFFSET, INVALID_NODE_OFFSET);
    }
    auto [startOffset, endOffset] = tableStates[currentStateIdx]->getNextRangeToRead();
    while (startOffset >= endOffset) {
        currentStateIdx++;
        if (currentStateIdx == tableStates.size()) {
            return make_tuple(nullptr, INVALID_NODE_OFFSET, INVALID_NODE_OFFSET);
        }
        auto [_startOffset, _endOffset] = tableStates[currentStateIdx]->getNextRangeToRead();
        startOffset = _startOffset;
        endOffset = _endOffset;
    }
    assert(currentStateIdx < tableStates.size());
    return make_tuple(tableStates[currentStateIdx].get(), startOffset, endOffset);
}

shared_ptr<ResultSet> ScanNodeID::init(ExecutionContext* context) {
    PhysicalOperator::init(context);
    resultSet = populateResultSet();
    auto outDataChunk = resultSet->dataChunks[outDataPos.dataChunkPos];
    outValueVector = make_shared<ValueVector>(NODE_ID, context->memoryManager);
    outValueVector->setSequential();
    outDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
    sharedState->initialize(transaction);
    return resultSet;
}

bool ScanNodeID::getNextTuples() {
    metrics->executionTime.start();
    do {
        auto [state, startOffset, endOffset] = sharedState->getNextRangeToRead();
        if (state == nullptr) {
            metrics->executionTime.stop();
            return false;
        }
        auto nodeIDValues = (nodeID_t*)(outValueVector->getData());
        auto size = endOffset - startOffset;
        for (auto i = 0u; i < size; ++i) {
            nodeIDValues[i].offset = startOffset + i;
            nodeIDValues[i].tableID = state->getTable()->getTableID();
        }
        outValueVector->state->initOriginalAndSelectedSize(size);
        setSelVector(state, startOffset, endOffset);
    } while (outValueVector->state->selVector->selectedSize == 0);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(outValueVector->state->selVector->selectedSize);
    return true;
}

void ScanNodeID::setSelVector(
    ScanTableNodeIDSharedState* tableState, node_offset_t startOffset, node_offset_t endOffset) {
    if (tableState->isSemiMaskEnabled()) {
        outValueVector->state->selVector->resetSelectorToValuePosBuffer();
        // Fill selected positions based on node mask for nodes between the given startOffset and
        // endOffset. If the node is masked (i.e., valid for read), then it is set to the selected
        // positions. Finally, we update the selectedSize for selVector.
        sel_t numSelectedValues = 0;
        for (auto i = 0u; i < (endOffset - startOffset); i++) {
            outValueVector->state->selVector->selectedPositions[numSelectedValues] = i;
            numSelectedValues += tableState->getSemiMask()->isNodeMasked(i + startOffset);
        }
        outValueVector->state->selVector->selectedSize = numSelectedValues;
    } else {
        // By default, the selected positions is set to the const incremental pos array.
        outValueVector->state->selVector->resetSelectorToUnselected();
    }
    // Apply changes to the selVector from nodes metadata.
    tableState->getTable()->setSelVectorForDeletedOffsets(transaction, outValueVector);
}

} // namespace processor
} // namespace kuzu
