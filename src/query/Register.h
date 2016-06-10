#pragma once
#ifndef REGISTER_H
#define REGISTER_H

#include "sql/SchemaTypes.h"

class Register
{
	friend class TableScanOperator;
	friend class HashJoinOperator;
public:
	std::string GetAttributeName();
	Integer GetInteger();
	std::string GetString();

	SchemaTypes::Tag GetType();

	bool operator==( const Register& other ) const;
	bool operator!=( const Register& other ) const;

	// No need for hash since using unordered map later
private:
	Integer mIntVar;
	std::string mStringVar;
	SchemaTypes::Tag mType;
	std::string mAttrName;
};
#endif