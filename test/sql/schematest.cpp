#include "sql/Schema.h"
#include "sql/SchemaParser.h"
#include "DBCore.h"
#include "utility/macros.h"
#include "utility/helpers.h"

#include "gtest/gtest.h"

// Test that our schemas are the same after parsing/serialization etc etc.
class SchemaTest : public ::testing::Test
{
public:
	virtual void SetUp() override
	{
		core = new DBCore();
		core->WipeDatabase();
	}
	virtual void TearDown() override
	{
		SDELETE( core );
	}
	DBCore* core;
};

TEST_F( SchemaTest, SchemaEquality )
{
	// Tests if the same two schemas are equal and different schemas are not
	std::string sql = std::string( "create table employee (id integer, country_id char( 2 ), mgr_id integer, " ) +
		"salary integer, first_name char( 20 ), middle char( 1 ), last_name char( 20 )," +
		"primary key( id, country_id ));\n" +
		"create table country(country_id char( 2 ), short_name char( 20 ), long_name char( 50 ), primary key( country_id ));\n" +
		"create table department(id integer, primary key( id ), name char( 25 ), country_id char( 2 ));";
	SchemaParser parserSql( sql, true );
	std::unique_ptr<Schema> sSql = parserSql.parse();

	std::string sql2 = std::string( "create table employee (id integer, country_id char( 2 ), mgr_id integer, " ) +
		"salary integer, first_name char( 20 ), middle char( 1 ), last_name char( 20 )," +
		"primary key( id, country_id ));\n" +
		"create table country(country_id char( 2 ), short_name char( 20 ), long_name char( 50 ), primary key( country_id ));\n" +
		"create table department(id integer, primary key( id ), name char( 25 ), country_id char( 2 ));";
	SchemaParser parserSql2( sql2, true );
	std::unique_ptr<Schema> sSql2 = parserSql2.parse();

	EXPECT_EQ( *sSql, *sSql2 );

	// changed the typo in salary back as a change xD
	std::string sql3 = std::string( "create table employee (id integer, country_id char( 2 ), mgr_id integer, " ) +
		"salery integer, first_name char( 20 ), middle char( 1 ), last_name char( 20 )," +
		"primary key( id, country_id ));\n" +
		"create table country(country_id char( 2 ), short_name char( 20 ), long_name char( 50 ), primary key( country_id ));\n" +
		"create table department(id integer, primary key( id ), name char( 25 ), country_id char( 2 ));";
	SchemaParser parserSql3( sql3, true );
	std::unique_ptr<Schema> sSql3 = parserSql3.parse();
	EXPECT_NE( *sSql, *sSql3 );
}

TEST_F( SchemaTest, ParseFileStringEq )
{
	std::string sql = std::string( "create table employee (id integer, country_id char( 2 ), mgr_id integer, " ) +
		"salary integer, first_name char( 20 ), middle char( 1 ), last_name char( 20 )," +
		"primary key( id, country_id ));\n" +
		"create table country(country_id char( 2 ), short_name char( 20 ), long_name char( 50 ), primary key( country_id ));\n" +
		"create table department(id integer, primary key( id ), name char( 25 ), country_id char( 2 ));";
	SchemaParser parserSql( sql, true );
	std::unique_ptr<Schema> sSql = parserSql.parse();

	std::string filepath = "testSchema.sql";
	SchemaParser parserFile( filepath, false );
	std::unique_ptr<Schema> sFile = parserFile.parse();

	EXPECT_EQ( *sSql, *sFile );
}

TEST_F( SchemaTest, AddRelationFileString )
{
	// Test if the masterschema from file is the same as from string
	std::string filepath = "testSchema.sql";
	core->AddRelationsFromFile( filepath );
	Schema s = *core->GetSchema(); // Make copy
	core->WipeDatabase();
	std::string sql = std::string( "create table employee (id integer, country_id char( 2 ), mgr_id integer, " ) +
		"salary integer, first_name char( 20 ), middle char( 1 ), last_name char( 20 )," +
		"primary key( id, country_id ));\n" +
		"create table country(country_id char( 2 ), short_name char( 20 ), long_name char( 50 ), primary key( country_id ));\n" +
		"create table department(id integer, primary key( id ), name char( 25 ), country_id char( 2 ));";
	core->AddRelationsFromString( sql );
	EXPECT_EQ( s, *core->GetSchema() );
}

TEST_F( SchemaTest, SerializationDeserialization )
{
	std::string filepath = "testSchema.sql";
	core->AddRelationsFromFile( filepath );
	// Create a serialized data
	std::vector<uint8_t> data;
	const_cast<Schema*>(core->GetSchema())->Serialize( data );
	// Unserialized data
	Schema deserialized;
	deserialized.Deserialize( &data[0] );
	EXPECT_EQ( *core->GetSchema(), deserialized );	
}

TEST_F( SchemaTest, SerializationDeserializationMultipages )
{
	// Serialization/Deserialization and dbreloading test of schema with multiple pages of relations (>16KB)
	std::string sql = std::string( "create table employee (id integer, country_id char( 2 ), mgr_id integer, " ) +
		"salary integer, first_name char( 20 ), middle char( 1 ), last_name char( 20 ), ";
	for ( uint32_t i = 0; i < 10000; ++i)
	{
		sql += "derpattr_" + std::to_string( i ) + " integer, ";
	}
	sql += "primary key( id, country_id ));";
	core->AddRelationsFromString( sql );

	// Create a serialized data
	const Schema* olds = core->GetSchema();
	SDELETE( core );
	core = new DBCore();
	EXPECT_EQ( *olds, *core->GetSchema() );
}