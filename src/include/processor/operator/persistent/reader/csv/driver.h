#pragma once

#include <cstdint>

#include "common/data_chunk/data_chunk.h"
#include "function/table/bind_input.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace processor {

// TODO(Keenan): Split up this file.
class BaseCSVReader;
class ParsingDriver {
public:
    explicit ParsingDriver(common::DataChunk& chunk);
    virtual ~ParsingDriver() = default;

    bool done(uint64_t rowNum);
    bool addValue(uint64_t rowNum, common::column_id_t columnIdx, std::string_view value);
    bool addRow(uint64_t rowNum, common::column_id_t columnCount);

private:
    virtual bool doneEarly() = 0;
    virtual BaseCSVReader* getReader() = 0;

private:
    common::DataChunk& chunk;

protected:
    bool rowEmpty;
};

class ParallelCSVReader;

class ParallelParsingDriver : public ParsingDriver {
public:
    ParallelParsingDriver(common::DataChunk& chunk, ParallelCSVReader* reader);
    bool doneEarly() override;

private:
    BaseCSVReader* getReader() override;

private:
    ParallelCSVReader* reader;
};

class SerialCSVReader;

class SerialParsingDriver : public ParsingDriver {
public:
    SerialParsingDriver(common::DataChunk& chunk, SerialCSVReader* reader);
    bool doneEarly() override;

private:
    BaseCSVReader* getReader() override;

protected:
    SerialCSVReader* reader;
};

class SniffCSVNameAndTypeDriver : public SerialParsingDriver {
public:
    SniffCSVNameAndTypeDriver(SerialCSVReader* reader,
        const function::ScanTableFuncBindInput* bindInput);

    bool done(uint64_t rowNum) const;
    bool addValue(uint64_t rowNum, common::column_id_t columnIdx, std::string_view value);

public:
    std::vector<std::pair<std::string, common::LogicalType>> columns;
    std::vector<bool> sniffType;
    // if the type isn't declared in the header, sniff it
};

} // namespace processor
} // namespace kuzu
