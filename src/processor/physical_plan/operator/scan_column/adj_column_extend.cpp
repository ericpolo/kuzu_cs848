#include "src/processor/include/physical_plan/operator/scan_column/adj_column_extend.h"

namespace graphflow {
namespace processor {

AdjColumnExtend::AdjColumnExtend(uint64_t dataChunkPos, uint64_t valueVectorPos, BaseColumn* column,
    unique_ptr<PhysicalOperator> prevOperator)
    : ScanColumn{dataChunkPos, valueVectorPos, column, move(prevOperator)} {
    auto outNodeIDVector = make_shared<NodeIDVector>(((AdjColumn*)column)->getCompressionScheme());
    outNodeIDVector->setIsSequence(inNodeIDVector->getIsSequence());
    outValueVector = static_pointer_cast<ValueVector>(outNodeIDVector);
    inDataChunk->append(outValueVector);
    outValueVector->setDataChunkOwner(inDataChunk);
}

void AdjColumnExtend::getNextTuples() {
    ScanColumn::getNextTuples();
    static_pointer_cast<NodeIDVector>(outValueVector)->discardNulls();
}

} // namespace processor
} // namespace graphflow
