#pragma once

#include "copy_csv.h"

#include "src/storage/store/include/nodes_store.h"

namespace graphflow {
namespace processor {

class CopyNodeCSV : public CopyCSV {

public:
    CopyNodeCSV(Catalog* catalog, CSVDescription csvDescription, Label label, WAL* wal, uint32_t id,
        const string& paramsString, NodesStore* nodesStore)
        : CopyCSV(catalog, move(csvDescription), move(label), wal, id, paramsString),
          nodesStore{nodesStore} {}

    void execute(TaskScheduler& taskScheduler, ExecutionContext* executionContext) override;

    PhysicalOperatorType getOperatorType() override { return COPY_NODE_CSV; }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CopyNodeCSV>(
            catalog, csvDescription, label, wal, id, paramsString, nodesStore);
    }

private:
    NodesStore* nodesStore;
};

} // namespace processor
} // namespace graphflow
