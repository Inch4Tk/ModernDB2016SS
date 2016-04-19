# Introduction
Project for TUM course: Datenbanksysteme und moderne CPU-Architekturen 
The submission commits are tagged. (Github release) If the current submission tag does not exist, I may have forgot, check out the last commit.

# Build Dependencies
* Cmake 3.1 (Why? http://stackoverflow.com/questions/10984442/how-to-detect-c11-support-of-a-compiler-with-cmake/20165220#20165220 Installation Suggestion: See VagrantBootstrap.sh)

# Build project on UNIX
Go into project root folder (terminal). Execute the following commands.
```
mkdir -p build && cd build
cmake -G "Unix Makefiles" -D CMAKE_BUILD_TYPE=Release ..
make all
```
(Optional: Substitute Debug as CMAKE_BUILD_TYPE)

# Build project on Windows
Download Cmake GUI. Point Source directory to project root folder (the one containing this Readme).
Point Build directory to project_root/build or project_root/buildwin if you also plan to build for unix
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

# Executing
From build folder
```
./sort inputFile outputFile memoryBufferInMB
```