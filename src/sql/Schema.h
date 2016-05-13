#ifndef SCHEMA_H
#define SCHEMA_H

#include <vector>
#include <string>
#include "SchemaTypes.h"

/// <summary>
/// Schema class. Database will always first try to load a currently existing schema as masterschema.
/// If there is no existing schema, the masterschema will be created as empty.
/// New schemas can then be added by merging them to this masterschema, on shutdown they will be serialized.
/// </summary>
struct Schema {
   struct Relation {
      struct Attribute {
         std::string name;
         SchemaTypes::Tag type;
		 uint32_t len;
         bool notNull;
         Attribute() : len(~0), notNull(true) {}
		 bool operator==( const Schema::Relation::Attribute& other ) const;
		 bool operator!=( const Schema::Relation::Attribute& other ) const;
      };
	  uint64_t segmentId;
	  uint64_t pagecount = 0;
      std::string name;
      std::vector<Schema::Relation::Attribute> attributes;
      std::vector<uint32_t> primaryKey;
      Relation(const std::string& name) : name(name) {
	  }
	  bool operator==( const Schema::Relation& other ) const;
	  bool operator!=( const Schema::Relation& other ) const;
   };
   std::vector<Schema::Relation> relations;
   std::string toString() const;

   void Serialize( std::vector<uint8_t>& data );
   void Deserialize( const uint8_t* data );
   void MergeSchema( Schema& other );

   bool operator==( const Schema& other ) const;
   bool operator!=( const Schema& other ) const;

private:
	// Serialization
	void SerializeRelation( Relation& r, std::vector<uint8_t>& data );
	void SerializeAttribute( Relation::Attribute& a, std::vector<uint8_t>& data );

	void AppendToData( bool toappend, std::vector<uint8_t>& data );
	void AppendToData( uint16_t toappend, std::vector<uint8_t>& data );
	void AppendToData( uint32_t toappend, std::vector<uint8_t>& data );
	void AppendToData( uint64_t toappend, std::vector<uint8_t>& data );
	void AppendToData( const std::string& toappend, std::vector<uint8_t>& data );

	// Deserialization
	void DeserializeRelation( const uint8_t*& data );
	void DeserializeAttribute( Relation& r, const uint8_t*& data );

	void ReadFromData( bool& toread, const uint8_t*& data );
	void ReadFromData( uint16_t& toread, const uint8_t*& data );
	void ReadFromData( uint32_t& toread, const uint8_t*& data );
	void ReadFromData( uint64_t& toread, const uint8_t*& data );
	void ReadFromData( std::string& toread, const uint8_t*& data );
};
#endif
