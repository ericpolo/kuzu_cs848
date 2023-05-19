#include "binder/binder.h"
#include "binder/expression/function_expression.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression_binder.h"
#include "common/string_utils.h"
#include "function/schema/vector_label_operations.h"
#include "parser/expression/parsed_function_expression.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindFunctionExpression(
    const ParsedExpression& parsedExpression) {
    auto& parsedFunctionExpression = (ParsedFunctionExpression&)parsedExpression;
    auto functionName = parsedFunctionExpression.getFunctionName();
    StringUtils::toUpper(functionName);
    // check for special function binding
    if (functionName == ID_FUNC_NAME) {
        return bindInternalIDExpression(parsedExpression);
    } else if (functionName == LABEL_FUNC_NAME) {
        return bindLabelFunction(parsedExpression);
    }
    auto functionType = binder->catalog.getFunctionType(functionName);
    if (functionType == FUNCTION) {
        return bindScalarFunctionExpression(parsedExpression, functionName);
    } else {
        assert(functionType == AGGREGATE_FUNCTION);
        return bindAggregateFunctionExpression(
            parsedExpression, functionName, parsedFunctionExpression.getIsDistinct());
    }
}

std::shared_ptr<Expression> ExpressionBinder::bindScalarFunctionExpression(
    const ParsedExpression& parsedExpression, const std::string& functionName) {
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = bindExpression(*parsedExpression.getChild(i));
        children.push_back(std::move(child));
    }
    return bindScalarFunctionExpression(children, functionName);
}

std::shared_ptr<Expression> ExpressionBinder::bindScalarFunctionExpression(
    const expression_vector& children, const std::string& functionName) {
    auto builtInFunctions = binder->catalog.getBuiltInScalarFunctions();
    std::vector<LogicalType> childrenTypes;
    for (auto& child : children) {
        childrenTypes.push_back(child->dataType);
    }
    auto function = builtInFunctions->matchFunction(functionName, childrenTypes);
    if (builtInFunctions->canApplyStaticEvaluation(functionName, children)) {
        return staticEvaluate(functionName, children);
    }
    expression_vector childrenAfterCast;
    for (auto i = 0u; i < children.size(); ++i) {
        auto targetType =
            function->isVarLength ? function->parameterTypeIDs[0] : function->parameterTypeIDs[i];
        childrenAfterCast.push_back(implicitCastIfNecessary(children[i], targetType));
    }
    std::unique_ptr<function::FunctionBindData> bindData;
    if (function->bindFunc) {
        bindData = function->bindFunc(childrenAfterCast, function);
    } else {
        bindData =
            std::make_unique<function::FunctionBindData>(LogicalType(function->returnTypeID));
    }
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(function->name, childrenAfterCast);
    return make_shared<ScalarFunctionExpression>(functionName, FUNCTION, std::move(bindData),
        std::move(childrenAfterCast), function->execFunc, function->selectFunc,
        uniqueExpressionName);
}

std::shared_ptr<Expression> ExpressionBinder::bindAggregateFunctionExpression(
    const ParsedExpression& parsedExpression, const std::string& functionName, bool isDistinct) {
    auto builtInFunctions = binder->catalog.getBuiltInAggregateFunction();
    std::vector<LogicalType> childrenTypes;
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = bindExpression(*parsedExpression.getChild(i));
        // rewrite aggregate on node or rel as aggregate on their internal IDs.
        // e.g. COUNT(a) -> COUNT(a._id)
        if (child->dataType.getLogicalTypeID() == LogicalTypeID::NODE ||
            child->dataType.getLogicalTypeID() == LogicalTypeID::REL) {
            child = bindInternalIDExpression(*child);
        }
        childrenTypes.push_back(child->dataType);
        children.push_back(std::move(child));
    }
    auto function = builtInFunctions->matchFunction(functionName, childrenTypes, isDistinct);
    auto uniqueExpressionName =
        AggregateFunctionExpression::getUniqueName(function->name, children, function->isDistinct);
    if (children.empty()) {
        uniqueExpressionName = binder->getUniqueExpressionName(uniqueExpressionName);
    }
    std::unique_ptr<function::FunctionBindData> bindData;
    if (function->bindFunc) {
        bindData = function->bindFunc(children, function);
    } else {
        bindData =
            std::make_unique<function::FunctionBindData>(LogicalType(function->returnTypeID));
    }
    return make_shared<AggregateFunctionExpression>(functionName, std::move(bindData),
        std::move(children), function->aggregateFunction->clone(), uniqueExpressionName);
}

std::shared_ptr<Expression> ExpressionBinder::staticEvaluate(
    const std::string& functionName, const expression_vector& children) {
    assert(children[0]->expressionType == common::LITERAL);
    auto strVal = ((LiteralExpression*)children[0].get())->getValue()->getValue<std::string>();
    std::unique_ptr<Value> value;
    if (functionName == CAST_TO_DATE_FUNC_NAME) {
        value = std::make_unique<Value>(Date::FromCString(strVal.c_str(), strVal.length()));
    } else if (functionName == CAST_TO_TIMESTAMP_FUNC_NAME) {
        value = std::make_unique<Value>(Timestamp::FromCString(strVal.c_str(), strVal.length()));
    } else {
        assert(functionName == CAST_TO_INTERVAL_FUNC_NAME);
        value = std::make_unique<Value>(Interval::FromCString(strVal.c_str(), strVal.length()));
    }
    return createLiteralExpression(std::move(value));
}

std::shared_ptr<Expression> ExpressionBinder::bindInternalIDExpression(
    const ParsedExpression& parsedExpression) {
    auto child = bindExpression(*parsedExpression.getChild(0));
    validateExpectedDataType(
        *child, std::unordered_set<LogicalTypeID>{LogicalTypeID::NODE, LogicalTypeID::REL});
    return bindInternalIDExpression(*child);
}

std::shared_ptr<Expression> ExpressionBinder::bindInternalIDExpression(
    const Expression& expression) {
    if (expression.dataType.getLogicalTypeID() == LogicalTypeID::NODE) {
        auto& node = (NodeExpression&)expression;
        return node.getInternalIDProperty();
    } else {
        assert(expression.dataType.getLogicalTypeID() == LogicalTypeID::REL);
        return bindRelPropertyExpression(expression, INTERNAL_ID_SUFFIX);
    }
}

std::unique_ptr<Expression> ExpressionBinder::createInternalNodeIDExpression(
    const Expression& expression) {
    auto& node = (NodeExpression&)expression;
    std::unordered_map<table_id_t, property_id_t> propertyIDPerTable;
    for (auto tableID : node.getTableIDs()) {
        propertyIDPerTable.insert({tableID, INVALID_PROPERTY_ID});
    }
    auto result = std::make_unique<PropertyExpression>(LogicalType(LogicalTypeID::INTERNAL_ID),
        INTERNAL_ID_SUFFIX, node, std::move(propertyIDPerTable), false /* isPrimaryKey */);
    return result;
}

std::shared_ptr<Expression> ExpressionBinder::bindLabelFunction(
    const ParsedExpression& parsedExpression) {
    // bind child node
    auto child = bindExpression(*parsedExpression.getChild(0));
    if (child->dataType.getLogicalTypeID() == common::LogicalTypeID::NODE) {
        return bindNodeLabelFunction(*child);
    } else {
        assert(child->dataType.getLogicalTypeID() == common::LogicalTypeID::REL);
        return bindRelLabelFunction(*child);
    }
}

static std::vector<std::unique_ptr<Value>> populateLabelValues(
    std::vector<table_id_t> tableIDs, const catalog::CatalogContent& catalogContent) {
    auto tableIDsSet = std::unordered_set<table_id_t>(tableIDs.begin(), tableIDs.end());
    table_id_t maxTableID = *std::max_element(tableIDsSet.begin(), tableIDsSet.end());
    std::vector<std::unique_ptr<Value>> labels;
    labels.resize(maxTableID + 1);
    for (auto i = 0; i < labels.size(); ++i) {
        if (tableIDsSet.contains(i)) {
            labels[i] = std::make_unique<Value>(catalogContent.getTableName(i));
        } else {
            // TODO(Xiyang/Guodong): change to null literal once we support null in LIST type.
            labels[i] = std::make_unique<Value>(std::string(""));
        }
    }
    return labels;
}

std::shared_ptr<Expression> ExpressionBinder::bindNodeLabelFunction(const Expression& expression) {
    auto catalogContent = binder->catalog.getReadOnlyVersion();
    auto& node = (NodeExpression&)expression;
    if (!node.isMultiLabeled()) {
        auto labelName = catalogContent->getTableName(node.getSingleTableID());
        return createLiteralExpression(std::make_unique<Value>(labelName));
    }
    auto nodeTableIDs = catalogContent->getNodeTableIDs();
    expression_vector children;
    children.push_back(node.getInternalIDProperty());
    auto labelsValue =
        std::make_unique<Value>(LogicalType(LogicalTypeID::VAR_LIST,
                                    std::make_unique<VarListTypeInfo>(
                                        std::make_unique<LogicalType>(LogicalTypeID::STRING))),
            populateLabelValues(nodeTableIDs, *catalogContent));
    children.push_back(createLiteralExpression(std::move(labelsValue)));
    auto execFunc = function::LabelVectorOperation::execFunction;
    auto bindData =
        std::make_unique<function::FunctionBindData>(LogicalType(LogicalTypeID::STRING));
    auto uniqueExpressionName = ScalarFunctionExpression::getUniqueName(LABEL_FUNC_NAME, children);
    return make_shared<ScalarFunctionExpression>(LABEL_FUNC_NAME, FUNCTION, std::move(bindData),
        std::move(children), execFunc, nullptr, uniqueExpressionName);
}

std::shared_ptr<Expression> ExpressionBinder::bindRelLabelFunction(const Expression& expression) {
    auto catalogContent = binder->catalog.getReadOnlyVersion();
    auto& rel = (RelExpression&)expression;
    if (!rel.isMultiLabeled()) {
        auto labelName = catalogContent->getTableName(rel.getSingleTableID());
        return createLiteralExpression(std::make_unique<Value>(labelName));
    }
    auto relTableIDs = catalogContent->getRelTableIDs();
    expression_vector children;
    children.push_back(rel.getInternalIDProperty());
    auto labelsValue =
        std::make_unique<Value>(LogicalType(LogicalTypeID::VAR_LIST,
                                    std::make_unique<VarListTypeInfo>(
                                        std::make_unique<LogicalType>(LogicalTypeID::STRING))),
            populateLabelValues(relTableIDs, *catalogContent));
    children.push_back(createLiteralExpression(std::move(labelsValue)));
    auto execFunc = function::LabelVectorOperation::execFunction;
    auto bindData =
        std::make_unique<function::FunctionBindData>(LogicalType(LogicalTypeID::STRING));
    auto uniqueExpressionName = ScalarFunctionExpression::getUniqueName(LABEL_FUNC_NAME, children);
    return make_shared<ScalarFunctionExpression>(LABEL_FUNC_NAME, FUNCTION, std::move(bindData),
        std::move(children), execFunc, nullptr, uniqueExpressionName);
}

} // namespace binder
} // namespace kuzu
