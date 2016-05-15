#pragma once
#ifndef SCHEMA_PARSER_H
#define SCHEMA_PARSER_H

#include <exception>
#include <string>
#include <memory>
#include <fstream>
#include <sstream>
#include "Schema.h"

class SchemaParserError : std::exception
{
	std::string msg;
	unsigned line;
public:
	SchemaParserError( unsigned line, const std::string& m ) : msg( m ), line( line )
	{
	}
	~SchemaParserError() throw()
	{
	}
	const char* what() const throw()
	{
		return msg.c_str();
	}
};

struct SchemaParser
{
	bool fromSqlString;
	std::string sqlOrFile;
	enum class State : unsigned
	{
		Init, Create, Table, CreateTableBegin, CreateTableEnd, TableName, Primary, Key, KeyListBegin, KeyName, KeyListEnd, AttributeName, AttributeTypeInt, AttributeTypeChar, CharBegin, CharValue, CharEnd, AttributeTypeNumeric, NumericBegin, NumericValue1, NumericSeparator, NumericValue2, NumericEnd, Not, Null, Separator, Semicolon
	};
	State state;
	SchemaParser( const std::string& sqlOrFile, bool fromSqlString ) : 
		fromSqlString(fromSqlString), sqlOrFile(sqlOrFile), state( State::Init )
	{
	}
	~SchemaParser()
	{
	};
	std::unique_ptr<Schema> parse();

private:
	void nextToken( unsigned line, const std::string& token, Schema& s );
};

#endif
