#ifndef operator_hpp
#define operator_hpp

template<typename T>
struct ExpressionNode {
public:
    virtual T evaluate() const = 0;
    virtual void clear() = 0;
    virtual void set_value(std::string key, void* val) = 0;
    virtual bool has_value() = 0;
};

template<typename TResult, typename TOperands>
class BinaryExpression: public ExpressionNode<TResult> {

protected:
    ExpressionNode<TOperands>* left;
    ExpressionNode<TOperands>* right;

public:
    BinaryExpression(ExpressionNode<TOperands>* left, ExpressionNode<TOperands>* right): left(left), right(right) {}

    virtual TResult evaluate() const = 0;
    virtual void clear() {
        left->clear();
        right->clear();
    }

    virtual void set_value(std::string key, void* value) {
        left->set_value(key, value);
        right->set_value(key, value);
    }

    virtual bool has_value() {
        return left->has_value() && right->has_value();
    }
};

class AndOperator: public BinaryExpression<bool, bool> {
    public:
    AndOperator(ExpressionNode<bool>* left, ExpressionNode<bool>* right): BinaryExpression<bool, bool>(left, right) {}

    virtual bool evaluate() const {
        return left->evaluate() && right->evaluate();
    }
};

template<typename T>
class GteOperator: public BinaryExpression<bool, T> {
public:
    GteOperator(ExpressionNode<T>* left, ExpressionNode<T>* right): BinaryExpression<bool, T>(left, right) {}

    virtual bool evaluate() const {
        return this->left->evaluate() >= this->right->evaluate();
    }
};

template<typename T>
class LteOperator: public BinaryExpression<bool, T> {
public:
    LteOperator(ExpressionNode<T>* left, ExpressionNode<T>* right): BinaryExpression<bool, T>(left, right) {}

    virtual bool evaluate() const {
        return this->left->evaluate() <= this->right->evaluate();
    }
};

template<typename T>
class EqOperator: public BinaryExpression<bool, T> {
public:
    EqOperator(ExpressionNode<T>* left, ExpressionNode<T>* right): BinaryExpression<bool, T>(left, right) {}

    virtual bool evaluate() const {
        // Both are null
        if (!(this->left->has_value()) && !(this->right->has_value())) {
            return true;
        }

        return this->left->evaluate() == this->right->evaluate();
    }

};


template<typename T>
struct ConstantValue: public ExpressionNode<T> {
    const T value;

    ConstantValue(T val): value(val) {
    }

    inline virtual bool has_value() {
        return true;
    }

    inline virtual T evaluate() const {
        return value;
    }

    inline virtual void clear() {
        // noop for constant
    }

    inline virtual void set_value(std::string key, void* val) {
        // noop for constant
    }
};

template<typename T>
struct FieldValue: public ExpressionNode<T> {
    const std::string field;
    bool has_val;
    T value;

 FieldValue(const std::string fieldName): field(fieldName), has_val(false) {
    }

    inline virtual bool has_value() {
        return has_val;
    }

    inline virtual T evaluate() const {
        return value;
    }

    inline virtual void clear() {
        has_val = false;
        value = (T)NULL;
    }

    inline virtual void set_value(std::string key, void* val) {
        if (key == field) {
            has_val = true;
            value = reinterpret_cast<T>(val);
        }
    }
};

#endif
