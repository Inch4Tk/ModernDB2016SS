#pragma once
#ifndef MACROS_H
#define MACROS_H

#define SDELETE(p) {if(p){delete p; p = nullptr;}}
#define ADELETE(p) {if(p){delete[] p; p = nullptr;}}

#endif