#pragma once

#include <unordered_map>

#include "binder/expression/expression.h"

namespace kuzu {
namespace planner {

typedef uint32_t f_group_pos;
constexpr f_group_pos INVALID_F_GROUP_POS = UINT32_MAX;

class FactorizationGroup {
    friend class Schema;

public:
    FactorizationGroup() : flat{false}, singleState{false}, cardinalityMultiplier{1} {}
    FactorizationGroup(const FactorizationGroup& other)
        : flat{other.flat}, singleState{other.singleState},
          cardinalityMultiplier{other.cardinalityMultiplier}, expressions{other.expressions},
          expressionNameToPos{other.expressionNameToPos} {}

    inline void setFlat() {
        assert(!flat);
        flat = true;
    }
    inline bool isFlat() const { return flat; }
    inline void setSingleState() {
        assert(!singleState);
        singleState = true;
        setFlat();
    }
    inline bool isSingleState() const { return singleState; }

    inline void setMultiplier(uint64_t multiplier) { cardinalityMultiplier = multiplier; }
    inline uint64_t getMultiplier() const { return cardinalityMultiplier; }

    inline void insertExpression(const std::shared_ptr<binder::Expression>& expression) {
        assert(!expressionNameToPos.contains(expression->getUniqueName()));
        expressionNameToPos.insert({expression->getUniqueName(), expressions.size()});
        expressions.push_back(expression);
    }
    inline binder::expression_vector getExpressions() const { return expressions; }
    inline uint32_t getExpressionPos(const binder::Expression& expression) {
        assert(expressionNameToPos.contains(expression.getUniqueName()));
        return expressionNameToPos.at(expression.getUniqueName());
    }

private:
    bool flat;
    bool singleState;
    uint64_t cardinalityMultiplier;
    binder::expression_vector expressions;
    std::unordered_map<std::string, uint32_t> expressionNameToPos;
};

class Schema {
public:
    inline f_group_pos getNumGroups() const { return groups.size(); }

    inline FactorizationGroup* getGroup(std::shared_ptr<binder::Expression> expression) const {
        return getGroup(getGroupPos(expression->getUniqueName()));
    }

    inline FactorizationGroup* getGroup(const std::string& expressionName) const {
        return getGroup(getGroupPos(expressionName));
    }

    inline FactorizationGroup* getGroup(uint32_t pos) const { return groups[pos].get(); }

    f_group_pos createGroup();

    void insertToScope(const std::shared_ptr<binder::Expression>& expression, uint32_t groupPos);

    void insertToGroupAndScope(
        const std::shared_ptr<binder::Expression>& expression, uint32_t groupPos);

    void insertToGroupAndScope(const binder::expression_vector& expressions, uint32_t groupPos);

    inline f_group_pos getGroupPos(const binder::Expression& expression) const {
        return getGroupPos(expression.getUniqueName());
    }

    inline f_group_pos getGroupPos(const std::string& expressionName) const {
        assert(expressionNameToGroupPos.contains(expressionName));
        return expressionNameToGroupPos.at(expressionName);
    }

    inline std::pair<f_group_pos, uint32_t> getExpressionPos(
        const binder::Expression& expression) const {
        auto groupPos = getGroupPos(expression);
        return std::make_pair(groupPos, groups[groupPos]->getExpressionPos(expression));
    }

    inline void flattenGroup(f_group_pos pos) { groups[pos]->setFlat(); }
    inline void setGroupAsSingleState(f_group_pos pos) { groups[pos]->setSingleState(); }

    bool isExpressionInScope(const binder::Expression& expression) const;

    inline binder::expression_vector getExpressionsInScope() const { return expressionsInScope; }

    binder::expression_vector getExpressionsInScope(f_group_pos pos) const;

    binder::expression_vector getSubExpressionsInScope(
        const std::shared_ptr<binder::Expression>& expression);

    std::unordered_set<f_group_pos> getDependentGroupsPos(
        const std::shared_ptr<binder::Expression>& expression);

    inline void clearExpressionsInScope() {
        expressionNameToGroupPos.clear();
        expressionsInScope.clear();
    }

    // Get the group positions containing at least one expression in scope.
    std::unordered_set<f_group_pos> getGroupsPosInScope() const;

    std::unique_ptr<Schema> copy() const;

    void clear();

private:
    std::vector<std::unique_ptr<FactorizationGroup>> groups;
    std::unordered_map<std::string, uint32_t> expressionNameToGroupPos;
    // Our projection doesn't explicitly remove expressions. Instead, we keep track of what
    // expressions are in scope (i.e. being projected).
    binder::expression_vector expressionsInScope;
};

class SchemaUtils {
public:
    static std::vector<binder::expression_vector> getExpressionsPerGroup(
        const binder::expression_vector& expressions, const Schema& schema);
    // Given a set of factorization group, a leading group is selected as the unFlat group (caller
    // should ensure at most one unFlat group which is our general assumption of factorization). If
    // all groups are flat, we select any (the first) group as leading group.
    static f_group_pos getLeadingGroupPos(
        const std::unordered_set<f_group_pos>& groupPositions, const Schema& schema);

    static void validateAtMostOneUnFlatGroup(
        const std::unordered_set<f_group_pos>& groupPositions, const Schema& schema);
    static void validateNoUnFlatGroup(
        const std::unordered_set<f_group_pos>& groupPositions, const Schema& schema);
};

} // namespace planner
} // namespace kuzu
