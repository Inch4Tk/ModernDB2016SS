# Introduction
Project for TUM course: Datenbanksysteme und moderne CPU-Architekturen 

# Dependencies
* Cmake

# Build project on UNIX
Go into project root folder (terminal). Execute the following commands.
```
mkdir -p build && cd build
cmake -G "Unix Makefiles" -D CMAKE_BUILD_TYPE=Debug ..
make all
```
(Optional: Substitute Release as CMAKE_BUILD_TYPE)

# Build project on Windows
Download Cmake GUI. Point Source directory to project root folder (the one containing this Readme).
Point Build directory to project_root/build or project_root/buildwin if you also plan to build for unix
Configure -> Generate

# Vagrant
Project contains vagrant file for quick Ubuntu VM creation.
Download and install Vagrant + VirtualBox.
Execute in project root.
```
vagrant up
```
Creates and Provisions an Ubuntu LTE 14.04 VM. Install necessary buildtools, syncs project root folder to
/vagrant

# Executing
From build folder
```
./sort inputFile outputFile memoryBufferInMB
```