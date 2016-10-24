#ifndef operator_hpp
#define operator_hpp

#include <iostream>
#include <vector>

#include <cstring>

#include "exceptionutils.h"
#include "DataType.h"

using namespace eleveldb;

//=======================================================================
// A template base-class for all expression nodes
//=======================================================================

template<typename TResult>
struct ExpressionNode {
public:

    //------------------------------------------------------------
    // Constructor with datatype, to allow type checking in
    // set_value()
    //------------------------------------------------------------

    ExpressionNode(DataType::Type type=DataType::UNKNOWN) {
        type_ = type;
    };

    virtual ~ExpressionNode() {};

    //------------------------------------------------------------
    // Inherited interface
    //------------------------------------------------------------

    virtual TResult evaluate() const = 0;
    virtual void    clear()     = 0;
    virtual bool    has_value() const {return false;};

    virtual void    set_value(std::string key,     void* val, 
                              DataType::Type type, size_t size=0) = 0;
  
    //------------------------------------------------------------
    // Check a type against the operand data type for this expression
    //------------------------------------------------------------

    virtual void checkType(DataType::Type type) 
        {
            // If the type is unknown, we can't check it

            if(type_ == DataType::UNKNOWN)
                ThrowRuntimeError("Unable to check the type of this expression");

            // If the type is a string, check that the passed type is
            // either a string or a uchar_ptr

            if(type_ == DataType::STRING) {
                if(!(type == DataType::STRING || type == DataType::UCHAR_PTR)) {
                    std::ostringstream os;
                    os << "Attempt to set the wrong type of value (" << type 
                       << ") for this expression, which is of type " << type_;
                    ThrowRuntimeError(os.str());
                }

                // Else just check if the types match

            } else if(type_ != type) {
                std::ostringstream os;
                os << "Attempt to set the wrong type of value (" << type 
                   << ") for this expression, which is of type " << type_;
                ThrowRuntimeError(os.str() << " this  = " << this);
            }
        }

    //------------------------------------------------------------
    // Where, applicable. return the size of the value managed by this
    // node
    //------------------------------------------------------------

    virtual size_t size() {
        return 0;
    }

    virtual void throwIfNoValue() const {
        if(!has_value())
            ThrowRuntimeError("Expression has no value");
    }

protected:

    DataType::Type type_;
};

//=======================================================================
// Base-class for expressions involving two operators (binary operators)
//=======================================================================

template<typename TResult, typename TOperands>
class BinaryExpression : public ExpressionNode<TResult> {
    
protected:
    
    ExpressionNode<TOperands>* left_;
    ExpressionNode<TOperands>* right_;
    
public:
    
    BinaryExpression(ExpressionNode<TOperands>* left  = 0, 
                     ExpressionNode<TOperands>* right = 0, 
                     DataType::Type type = DataType::UNKNOWN) : 
        ExpressionNode<TResult>(type), left_(left), right_(right) {};
    
    virtual ~BinaryExpression() {
        if(left_) {
            delete left_;
            left_ = 0;
        }
        
        if(right_) {
            delete right_;
            right_ = 0;
        }
    }

    virtual TResult evaluate() const = 0;

    virtual void clear() {
        left_->clear();
        right_->clear();
    }
    
    virtual void set_value(std::string key, void* value, 
                           DataType::Type type, size_t size=0) {
        left_->set_value(key, value, type, size);
        right_->set_value(key, value, type, size);
    }

    virtual bool has_value() const {
        return left_->has_value() && right_->has_value();
    }

    virtual void throwIfNoValue() const {
        left_->throwIfNoValue();
        right_->throwIfNoValue();
    }

};

//=======================================================================
// Specializations of binary operators
//=======================================================================

//------------------------------------------------------------
// AND
//------------------------------------------------------------

class AndOperator: public BinaryExpression<bool, bool> {
public:

    AndOperator(ExpressionNode<bool>* left, ExpressionNode<bool>* right) : 
        BinaryExpression<bool, bool>(left, right, DataType::UNKNOWN) {}

    virtual ~AndOperator() {};

    virtual bool evaluate() const {

        // Note: now that we are allowing NULLs, we can no longer
        // check binary has_value() in AndOperator.
        //
        // What if one of our conditions is a NULL comparison, e.g.,
        // 'f1 == []'?  Then left->has_value()==false should _not_
        // cause us to return false here!
        //
        // We must instead evaluate both clauses, and allow the
        // evaluate functions to determine whether or not
        // has_value()==false is an error condition
        
        return left_->evaluate() && right_->evaluate();
    }
};

//------------------------------------------------------------
// OR
//------------------------------------------------------------

class OrOperator: public BinaryExpression<bool, bool> {
public:

    OrOperator(ExpressionNode<bool>* left, ExpressionNode<bool>* right) : 
        BinaryExpression<bool, bool>(left, right, DataType::UNKNOWN) {}

    virtual ~OrOperator() {};

    virtual bool evaluate() const {

        // Note: now that we are allowing NULLs, we can no longer
        // check binary has_value() in OrOperator.
        //
        // What if one of our conditions is a NULL comparison, e.g.,
        // 'f1 == []'?  Then left->has_value()==false should _not_
        // cause us to return false here!
        //
        // A different example: what if our condition is 'f1 > 3 OR f3
        // <= 5.0'?  If f3 is NULL (has_value()==false) but f1 == 4,
        // then this operator should logically return true
        //
        // We must instead evaluate both clauses, and allow the
        // evaluate functions to determine whether or not
        // has_value()==false is an error condition

        return left_->evaluate() || right_->evaluate();
    }
};

//------------------------------------------------------------
// > operator
//------------------------------------------------------------

template<typename T>
class GtOperator: public BinaryExpression<bool, T> {
public:

    GtOperator(ExpressionNode<T>* left, 
               ExpressionNode<T>* right, 
               DataType::Type type) : 
        BinaryExpression<bool, T>(left, right, type) {}

    virtual ~GtOperator() {};

    virtual bool evaluate() const {
        if(BinaryExpression<bool, T>::has_value()) {
            return this->left_->evaluate() > this->right_->evaluate();
        } else {
            return false;
        }
    }
};

//------------------------------------------------------------
// >= operator
//------------------------------------------------------------

template<typename T>
class GteOperator : public BinaryExpression<bool, T> {
public:

    GteOperator(ExpressionNode<T>* left, 
                ExpressionNode<T>* right, 
                DataType::Type type) : 
        BinaryExpression<bool, T>(left, right, type) {}

    virtual ~GteOperator() {};

    virtual bool evaluate() const {
        if(BinaryExpression<bool, T>::has_value()) {
            return this->left_->evaluate() >= this->right_->evaluate();
        } else {
            return false;
        }
    }
};

//------------------------------------------------------------
// < operator
//------------------------------------------------------------

template<typename T>
class LtOperator : public BinaryExpression<bool, T> {
public:

    LtOperator(ExpressionNode<T>* left, 
               ExpressionNode<T>* right, 
               DataType::Type type) : 
        BinaryExpression<bool, T>(left, right, type) {}

    virtual ~LtOperator() {};

    virtual bool evaluate() const {
        if(BinaryExpression<bool, T>::has_value()) {
            return this->left_->evaluate() < this->right_->evaluate();
        } else {
            return false;
        }
    }
};

//------------------------------------------------------------
// <= operator
//------------------------------------------------------------

template<typename T>
class LteOperator : public BinaryExpression<bool, T> {
public:

    LteOperator(ExpressionNode<T>* left, 
                ExpressionNode<T>* right, 
                DataType::Type type) : 
        BinaryExpression<bool, T>(left, right, type) {}

    virtual ~LteOperator() {};

    virtual bool evaluate() const {
        if(BinaryExpression<bool, T>::has_value()) {
            return this->left_->evaluate() <= this->right_->evaluate();
        } else {
            return false;
        }
    }
};

//------------------------------------------------------------
// == operator
//------------------------------------------------------------

template<typename T>
class EqOperator: public BinaryExpression<bool, T> {
public:

    EqOperator(ExpressionNode<T>* left, 
               ExpressionNode<T>* right, 
               DataType::Type type) : 
        BinaryExpression<bool, T>(left, right, type) {};

    virtual ~EqOperator() {};

    virtual bool evaluate() const {
        if(BinaryExpression<bool, T>::has_value()) {
            return this->left_->evaluate() == this->right_->evaluate();
        } else {
            return false;
        }
    }
};

// If we are managing a uchar ptr, do a bytewise comparison

template<>
class EqOperator<unsigned char*>: public BinaryExpression<bool, unsigned char*> {
public:
    
    EqOperator(ExpressionNode<unsigned char*>* left, 
               ExpressionNode<unsigned char*>* right, 
               DataType::Type type) : 
        BinaryExpression<bool, unsigned char*>(left, right, type) {}

    virtual ~EqOperator() {};

    virtual bool evaluate() const {
        // [] to represent NULL w/i Riak TS, which does NOT conflict w/ <<"">>.
        // NOTE: comparison to NULL is always rewritten with NULL on the
        // right-hand-side of the equality comparison
        if(right_->size() == 0) {
            return !left_->has_value();
        }

        if(!BinaryExpression<bool,unsigned char*>::has_value())
            return false;

        // If the sizes are equal, compare memory blocks
        if(left_->size() == right_->size()) {
            bool eq = (memcmp(left_->evaluate(), right_->evaluate(), left_->size()) == 0);
            return eq;
        }

        // Else not equal
        return false;
    }
};


//------------------------------------------------------------
// != operator
//------------------------------------------------------------

template<typename T>
class NeqOperator: public BinaryExpression<bool, T> {
public:

    NeqOperator(ExpressionNode<T>* left, 
                ExpressionNode<T>* right, 
                DataType::Type type) : 
        BinaryExpression<bool, T>(left, right, type) {}

    virtual ~NeqOperator() {};

    virtual bool evaluate() const {
        if(BinaryExpression<bool, T>::has_value()) {
            return this->left_->evaluate() != this->right_->evaluate();
        } else {
            return false;
        }
    }

};

// If managing a uchar ptr, do a bytewise comparison

template<>
class NeqOperator<unsigned char*>: public BinaryExpression<bool, unsigned char*> {
public:

    NeqOperator(ExpressionNode<unsigned char*>* left, 
                ExpressionNode<unsigned char*>* right, 
                DataType::Type type) : 
        BinaryExpression<bool, unsigned char*>(left, right, type) {}

    virtual ~NeqOperator() {};

    virtual bool evaluate() const {
        // [] to represent NULL w/i Riak TS, which does NOT conflict w/ <<"">>.
        // NOTE: comparison to NULL is always rewritten with NULL on the
        // right-hand-side of the equality comparison
        if(right_->size() == 0) {
            return left_->has_value();
        }

        if(!BinaryExpression<bool,unsigned char*>::has_value())
            return false;

        // If the sizes are equal, compare memory blocks

        if(left_->size() == right_->size()) {
            bool neq = memcmp(left_->evaluate(), right_->evaluate(), left_->size()) != 0;
            return neq;
        }

        // Else not equal

        return true;
    }
};

//=======================================================================
// Unary operators
//=======================================================================

//------------------------------------------------------------
// Constant value
//------------------------------------------------------------

template<typename T>
struct ConstantValue : public ExpressionNode<T> {
    const T value_;

    ConstantValue(T val) : ExpressionNode<T>(DataType::UNKNOWN), value_(val) {};

    inline virtual bool has_value() const {
        return true;
    }

    inline virtual T evaluate() const {
        return value_;
    }

    inline virtual void clear() {
        // noop for constant
    }

    inline virtual void set_value(std::string key, void* val, DataType::Type type, size_t size=0) {
        // noop for constant
    }
};

template<>
struct ConstantValue<unsigned char*> : public ExpressionNode<unsigned char*> {

    std::vector<unsigned char> buf_;
    const unsigned char* value_;
    size_t size_;
    
    ConstantValue(std::vector<unsigned char>& buf) : 
        ExpressionNode<unsigned char*>(DataType::UNKNOWN) {
        initialize(&buf[0], buf.size());
    }
        
    ConstantValue(unsigned char* val, size_t size) : 
        ExpressionNode<unsigned char*>(DataType::UNKNOWN) {
        initialize(val, size);
    }

    void initialize(unsigned char* val, size_t size) {

        // Constructor for const copies the value into an internal
        // buffer, since we are not guaranteed that the passed pointer
        // is persistent
            
        buf_.resize(size);
        memcpy((void*)&buf_[0], (void*)val, size);
        value_ = &buf_[0];
        size_  = size;
    }

    inline virtual bool has_value() const {
        return true;
    }

    inline virtual unsigned char* evaluate() const {
        return (unsigned char*)value_;
    }

    inline virtual void clear() {
        // noop for constant
    }

    inline virtual void set_value(std::string key, void* val, DataType::Type type, size_t size=0) {
        // noop for constant
    }
    
    inline size_t size() {
        return size_;
    }
};

//------------------------------------------------------------
// Field value
//------------------------------------------------------------

template<typename T>
struct FieldValue: public ExpressionNode<T> {
    const std::string field_;
    bool has_val_;
    T value_;

    FieldValue(const std::string fieldName, DataType::Type type) : 
        ExpressionNode<T>(type), field_(fieldName), has_val_(false) {};

    inline virtual bool has_value() const {
        return has_val_;
    }

    virtual void throwIfNoValue() const {
        if(!has_value())
            ThrowRuntimeError("Expression for field '" << field_ << "' has no value");
    }

    inline virtual T evaluate() const {
        FieldValue<T>::throwIfNoValue();
        return value_;
    }

    inline virtual void clear() {
        has_val_ = false;
    }

    //------------------------------------------------------------
    // Check a type against the type of this field
    //------------------------------------------------------------

    inline void checkType(DataType::Type type) {
        try {
            ExpressionNode<T>::checkType(type);
        } catch(std::runtime_error& err) {
            ThrowRuntimeError(err.what() << ", while processing field " << field_);
        }
    }

    //------------------------------------------------------------
    // Set a value for this field
    //------------------------------------------------------------

    inline virtual void set_value(std::string key, void* val, 
                                  DataType::Type type, size_t size=0) {

        // If called from a BinaryOperator parent, only set/check the
        // value for the matching field

        if(key == field_) {
            checkType(type);
            value_   = *((T*)val);
            has_val_ = true;
        }
    }
};

//------------------------------------------------------------
// String specialization of FieldValue class
//------------------------------------------------------------

template<>
struct FieldValue<std::string>: public ExpressionNode<std::string> {
    const std::string field_;
    bool has_val_;
    std::string value_;

    FieldValue(const std::string fieldName, DataType::Type type) : 
        ExpressionNode<std::string>(type), field_(fieldName), has_val_(false) {}

    inline virtual bool has_value() const {
        return has_val_;
    }

    inline virtual std::string evaluate() const {
        FieldValue<std::string>::throwIfNoValue();
        return value_;
    }

    inline virtual void clear() {
        has_val_ = false;
    }

    //------------------------------------------------------------
    // Check a type against the type of this field
    //------------------------------------------------------------

    inline void checkType(DataType::Type type) {
        try {
            ExpressionNode<std::string>::checkType(type);
        } catch(std::runtime_error& err) {
            ThrowRuntimeError(err.what() << ", while processing field " << field_);
        }
    }

    //------------------------------------------------------------
    // Set a value for this field
    //------------------------------------------------------------

    inline virtual void set_value(std::string key, void* val, 
                                  DataType::Type type, size_t size=0) {

        // If called from a BinaryOperator parent, only set/check the
        // value for the matching field. (This is because set_value
        // from a binary will be called on both the left and right
        // operands)

        if(key == field_) {
            checkType(type);

            if(type == DataType::STRING)
                value_ = *((std::string*)val);
            else if(type == DataType::UCHAR_PTR)
                value_ = (char*)val;

            has_val_ = true;
        }
    }

};

//------------------------------------------------------------
// Binary (unsigned char*) specialization of FieldValue class
//------------------------------------------------------------

template<>
struct FieldValue<unsigned char*>: public ExpressionNode<unsigned char*> {
    const std::string field_;
    bool has_val_;
    unsigned char* value_;
    size_t size_;

    FieldValue(const std::string fieldName, DataType::Type type) : 
    ExpressionNode<unsigned char*>(type), field_(fieldName), has_val_(false), value_(0), size_(0) {}

    inline virtual bool has_value() const {
        return has_val_;
    }

    inline virtual unsigned char* evaluate() const {
        FieldValue<unsigned char*>::throwIfNoValue();
        return value_;
    }

    inline virtual void clear() {
        has_val_ = false;
    }

    //------------------------------------------------------------
    // Check a type against the type of this field
    //------------------------------------------------------------

    inline void checkType(DataType::Type type) {
        try {
            ExpressionNode<unsigned char*>::checkType(type);
        } catch(std::runtime_error& err) {
            ThrowRuntimeError(err.what() << ", while processing field " << field_);
        }
    }

    //------------------------------------------------------------
    // Set a value for this field
    //------------------------------------------------------------

    inline virtual void set_value(std::string key, void* val, 
                                  DataType::Type type, size_t size=0) {

        // If called from a BinaryOperator parent, only set/check the
        // value for the matching field (This is because set_value
        // from a binary will be called on both the left and right
        // operands)

        if(key == field_) {
            checkType(type);

            // set_value() for FieldValue nodes stores the external
            // pointer, rather than copy, since these point to memory
            // that is persistent in the leveldb key for the duration
            // of the expression evaluation

            value_   = *((unsigned char**)val);
            size_    = size;
            has_val_ = true;
        }
    }

    inline size_t size() {
        return size_;
    }
};

#endif
