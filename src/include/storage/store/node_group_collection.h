#pragma once

#include "storage/store/group_collection.h"
#include "storage/store/node_group.h"

namespace kuzu {
namespace transaction {
class Transaction;
}
namespace storage {
class MemoryManager;

class NodeGroupCollection {
public:
    explicit NodeGroupCollection(MemoryManager& memoryManager,
        const std::vector<common::LogicalType>& types, bool enableCompression,
        FileHandle* dataFH = nullptr, common::Deserializer* deSer = nullptr);

    void append(const transaction::Transaction* transaction,
        const std::vector<common::ValueVector*>& vectors);
    void append(const transaction::Transaction* transaction, NodeGroupCollection& other);
    void appned(const transaction::Transaction* transaction, NodeGroup& nodeGroup);

    // This function only tries to append data into the last node group, and if the last node group
    // is not enough to hold all the data, it will append partially and return the number of rows
    // appended.
    // The returned values are the startOffset and numValuesAppended.
    // NOTE: This is specially coded to only be used by NodeBatchInsert for now.
    std::pair<common::offset_t, common::offset_t> appendToLastNodeGroupAndFlushWhenFull(
        transaction::Transaction* transaction, ChunkedNodeGroup& chunkedGroup);

    common::row_idx_t getNumRows();
    common::node_group_idx_t getNumNodeGroups() {
        const auto lock = nodeGroups.lock();
        return nodeGroups.getNumGroups(lock);
    }
    NodeGroup* getNodeGroupNoLock(const common::node_group_idx_t groupIdx) {
        return nodeGroups.getGroupNoLock(groupIdx);
    }
    NodeGroup* getNodeGroup(const common::node_group_idx_t groupIdx) {
        const auto lock = nodeGroups.lock();
        return nodeGroups.getGroup(lock, groupIdx);
    }
    NodeGroup* getOrCreateNodeGroup(common::node_group_idx_t groupIdx, NodeGroupDataFormat format);

    void setNodeGroup(const common::node_group_idx_t nodeGroupIdx,
        std::unique_ptr<NodeGroup> group) {
        const auto lock = nodeGroups.lock();
        nodeGroups.replaceGroup(lock, nodeGroupIdx, std::move(group));
    }

    void clear() {
        const auto lock = nodeGroups.lock();
        nodeGroups.clear(lock);
    }

    /* This function retrieve the physical info of all column chunk datas of the given column */
    std::vector<std::pair<common::page_idx_t, common::page_idx_t>>
        getAllChunkPhysicInfoForColumn(common::column_id_t columnID);
    /* This function retrieve the physical info of all column chunk datas of ALL columns */
    std::vector<std::pair<common::page_idx_t, common::page_idx_t>> getAllChunkPhysicInfo();

    common::column_id_t getNumColumns() const { return types.size(); }

    void addColumn(transaction::Transaction* transaction, TableAddColumnState& addColumnState);

    uint64_t getEstimatedMemoryUsage();

    void checkpoint(MemoryManager& memoryManager, NodeGroupCheckpointState& state);

    void serialize(common::Serializer& ser);

private:
    bool enableCompression;
    common::row_idx_t numRows;
    std::vector<common::LogicalType> types;
    GroupCollection<NodeGroup> nodeGroups;
    FileHandle* dataFH;
};

} // namespace storage
} // namespace kuzu
