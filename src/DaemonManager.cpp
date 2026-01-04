#include "DaemonManager.h"
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <signal.h>
#include <fstream>
#include <iostream>
#include <cstdlib>
#include <cerrno>
#include <cstring>

namespace ModbusLogger {

bool DaemonManager::signalHandlersSetup = false;
DaemonManager* DaemonManager::instance = nullptr;

DaemonManager::DaemonManager(const std::string& pidFilePath)
    : pidFilePath(pidFilePath), isDaemon(false) {
}

DaemonManager::~DaemonManager() {
    if (isDaemon) {
        removePidFile();
    }
}

bool DaemonManager::daemonize() {
    // Check if already running
    if (isAlreadyRunning()) {
        std::cerr << "Error: Process is already running (PID file exists: " << pidFilePath << ")" << std::endl;
        return false;
    }

    // Fork the process
    pid_t pid = fork();
    if (pid < 0) {
        std::cerr << "Error: Failed to fork: " << std::strerror(errno) << std::endl;
        return false;
    }

    // If we're the parent process, exit
    if (pid > 0) {
        exit(0);
    }

    // Child process continues here
    // Create a new session and detach from terminal
    if (setsid() < 0) {
        std::cerr << "Error: Failed to create new session: " << std::strerror(errno) << std::endl;
        return false;
    }

    // Fork again to ensure we're not a session leader
    pid = fork();
    if (pid < 0) {
        std::cerr << "Error: Failed to fork second time: " << std::strerror(errno) << std::endl;
        return false;
    }

    // If we're the parent process, exit
    if (pid > 0) {
        exit(0);
    }

    // Change working directory to root
    if (chdir("/") < 0) {
        std::cerr << "Warning: Failed to change directory to /: " << std::strerror(errno) << std::endl;
    }

    // Close standard file descriptors
    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    // Keep stderr open for error logging

    // Write PID file
    if (!writePidFile()) {
        std::cerr << "Error: Failed to write PID file" << std::endl;
        return false;
    }

    isDaemon = true;
    return true;
}

void DaemonManager::setupSignalHandlers(std::function<void()> callback) {
    shutdownCallback = callback;
    instance = this;
    signalHandlersSetup = true;

    struct sigaction sa;
    sa.sa_handler = signalHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    if (sigaction(SIGTERM, &sa, nullptr) < 0) {
        std::cerr << "Error: Failed to setup SIGTERM handler: " << std::strerror(errno) << std::endl;
    }

    if (sigaction(SIGHUP, &sa, nullptr) < 0) {
        std::cerr << "Error: Failed to setup SIGHUP handler: " << std::strerror(errno) << std::endl;
    }
}

void DaemonManager::signalHandler(int signal) {
    if (instance && instance->shutdownCallback) {
        if (signal == SIGTERM || signal == SIGHUP) {
            instance->shutdownCallback();
        }
    }
}

bool DaemonManager::writePidFile() const {
    std::ofstream pidFile(pidFilePath);
    if (!pidFile.is_open()) {
        std::cerr << "Error: Cannot open PID file for writing: " << pidFilePath << std::endl;
        return false;
    }
    pidFile << getpid() << std::endl;
    pidFile.close();
    return true;
}

void DaemonManager::removePidFile() const {
    if (unlink(pidFilePath.c_str()) < 0 && errno != ENOENT) {
        std::cerr << "Warning: Failed to remove PID file: " << std::strerror(errno) << std::endl;
    }
}

bool DaemonManager::isAlreadyRunning() const {
    std::ifstream pidFile(pidFilePath);
    if (!pidFile.is_open()) {
        return false;
    }

    int pid;
    pidFile >> pid;
    pidFile.close();

    // Check if process with this PID exists
    // Send signal 0 to check if process exists
    if (kill(pid, 0) == 0) {
        return true;
    }

    // Process doesn't exist, but PID file does - remove stale file
    if (errno == ESRCH) {
        removePidFile();
    }

    return false;
}

int DaemonManager::getPidFromFile() const {
    std::ifstream pidFile(pidFilePath);
    if (!pidFile.is_open()) {
        return -1;
    }

    int pid;
    pidFile >> pid;
    pidFile.close();
    return pid;
}

} // namespace ModbusLogger

