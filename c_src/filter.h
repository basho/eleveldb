#ifndef operator_hpp
#define operator_hpp

template<typename T>
struct ExpressionNode {
public:
    virtual T evaluate() const = 0;
    virtual void clear() = 0;
    virtual void set_value(std::string key, void* val) = 0;
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
};

template<typename T>
class GteOperator: public BinaryExpression<bool, T> {
public:
    GteOperator(ExpressionNode<T>* left, ExpressionNode<T>* right): BinaryExpression<bool, T>(left, right) {}
    
    virtual bool evaluate() const {
        return BinaryExpression<bool, T>::left->evaluate() >= BinaryExpression<bool, T>::right->evaluate();
    }
};

template<typename T>
class LteOperator: public BinaryExpression<bool, T> {
public:
    LteOperator(ExpressionNode<T>* left, ExpressionNode<T>* right): BinaryExpression<bool, T>(left, right) {}
    
    virtual bool evaluate() const {
        return BinaryExpression<bool, T>::left->evaluate() <= BinaryExpression<bool, T>::right->evaluate();
    }
};

template<typename T>
class EqOperator: public BinaryExpression<bool, T> {
public:
    EqOperator(ExpressionNode<T>* left, ExpressionNode<T>* right): BinaryExpression<bool, T>(left, right) {}
    
    virtual bool evaluate() const {
        return BinaryExpression<bool, T>::left->evaluate() == BinaryExpression<bool, T>::right->evaluate();
    }
};


template<typename T>
struct ConstantValue: public ExpressionNode<T> {
    const T value;
    
    ConstantValue(T val): value(val) {
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
    T* value;

    FieldValue(const std::string fieldName): field(fieldName) {
    }

    inline virtual T evaluate() const {
        return *value;
    }

    inline virtual void clear() {
        value = nullptr;
    }

    inline virtual void set_value(std::string key, void* val) {
        if (key == field) {
            value = reinterpret_cast<T*>(val);
        }
    }
};

#endif
