#include "src/common/include/vector/operations/vector_cast_operations.h"

#include <cassert>

#include "src/common/include/value.h"

namespace graphflow {
namespace common {

void VectorCastOperations::castStructuredToUnknownValue(ValueVector& operand, ValueVector& result) {
    assert(operand.dataType != UNKNOWN && result.dataType == UNKNOWN);
    auto outValues = (Value*)result.values;
    switch (operand.dataType) {
    case BOOL: {
        for (auto i = 0u; i < operand.state->numSelectedValues; i++) {
            auto pos = operand.state->selectedValuesPos[i];
            outValues[pos].primitive.boolean_ = operand.values[pos];
        }
    } break;
    case INT32: {
        auto intValues = (int32_t*)operand.values;
        for (auto i = 0u; i < operand.state->numSelectedValues; i++) {
            auto pos = operand.state->selectedValuesPos[i];
            outValues[pos].primitive.integer_ = intValues[pos];
        }
    } break;
    case DOUBLE: {
        auto doubleValues = (double_t*)operand.values;
        for (auto i = 0u; i < operand.state->numSelectedValues; i++) {
            auto pos = operand.state->selectedValuesPos[i];
            outValues[pos].primitive.double_ = doubleValues[pos];
        }
    } break;
    default:
        assert(false); // should never happen.
    }
}

void VectorCastOperations::castUnknownToBoolValue(ValueVector& operand, ValueVector& result) {
    assert(operand.dataType == UNKNOWN && result.dataType == BOOL);
    auto inValues = (Value*)result.values;
    for (auto i = 0u; i < operand.state->numSelectedValues; i++) {
        auto pos = operand.state->selectedValuesPos[i];
        if (inValues[pos].dataType != BOOL) {
            throw std::invalid_argument("don’t know how to treat that as a predicate: “" +
                                        inValues[pos].toValueAndType() + "”.");
        }
        result.values[pos] = inValues[pos].primitive.boolean_;
    }
}

} // namespace common
} // namespace graphflow
