#ifndef SCHEMA_H
#define SCHEMA_H

#include <vector>
#include <string>
#include "SchemaTypes.h"

struct Schema {
   struct Relation {
      struct Attribute {
         std::string name;
         SchemaTypes::Tag type;
         unsigned len;
         bool notNull;
         Attribute() : len(~0), notNull(true) {}
      };
      std::string name;
      std::vector<Schema::Relation::Attribute> attributes;
      std::vector<unsigned> primaryKey;
      Relation(const std::string& name) : name(name) {}
   };
   std::vector<Schema::Relation> relations;
   std::string toString() const;
};
#endif
