# Introduction
Project for TUM course: Datenbanksysteme und moderne CPU-Architekturen 
The submission commits are tagged. (Github release) If the current submission tag does not exist, I may have forgot, check out the last commit.

# Build Dependencies
* Cmake 3.1 (Why? http://stackoverflow.com/questions/10984442/how-to-detect-c11-support-of-a-compiler-with-cmake/20165220#20165220 Installation Suggestion: See VagrantBootstrap.sh)

# Build project on UNIX
Go into project root folder (terminal). Execute the following commands.
(For validating assignments, check out the validating subitem of executing)
```
mkdir -p build && cd build
cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release ..
make all
```
(Optional: Substitute Debug as CMAKE_BUILD_TYPE)

# Build project on Windows
Download Cmake GUI. Point Source directory to project root folder (the one containing this Readme).
Point Build directory to project_root/buildwin
Configure -> Generate

# (Optional) Vagrant
Project contains vagrant file for quick Ubuntu VM creation.
Download and install Vagrant + VirtualBox.
Execute in project root.
```
vagrant up
```
Creates and Provisions an Ubuntu LTE 14.04 VM. Install necessary buildtools, syncs project root folder to
/vagrant
SSH connection can be started from the console with
```
vagrant ssh
```

# Tested on
* Windows 7, with Visual Studio 2015 (v140) (Cmake version 3.5.1)
* Ubuntu LTE 14.04 VM, with gcc 4.8.4 (Cmake version 3.5.2)

# Executing on Unix
All commands are executed from target build folder.

## Validating Assignments
Since some assignment validations files use asserts, use Debug build.
To validate assignments configure the cmake project with an additional flag like this:
```
mkdir -p build && cd build
cmake -G "Unix Makefiles" -DASSIGN2_VALID=ON -DASSIGN3_VALID=ON -DASSIGN4_VALID=ON -DCMAKE_BUILD_TYPE=Debug ..
make all
./assignment2 <pagesOnDisk> <pagesInRAM> <threads>
./assignment3
./assignment4 <optional: n>
```
Can also skip single assignments by not adding the -DASSIGNX_VALID=ON flag.
Note: Corresponding Assignment 3 unit-test is actually stricter/more challenging than the modified provided validation file.

## Normal Execution
This starts up the database (currently nothing happens, database starts and shuts down)
```
./moderndb
```

## Tests
Runs all tests with googletest. Beware, as running tests will wipe the database (including metadata), 
provided tests were run from the same folder as the database files.
(IMPORTANT: This way you can also check a correct Assignment 2, 3 and 4 output. 
However, tests appear to be slower on unix than direct console calls of the validation file (may just be the VM though))
```
./moderndb_test
```
Test compilation can be disabled by passing "-DCOMPILE_TESTS=OFF" as an
additional flag to cmake
