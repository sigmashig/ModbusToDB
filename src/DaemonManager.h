#ifndef DAEMONMANAGER_H
#define DAEMONMANAGER_H

#include <functional>
#include <string>

namespace ModbusLogger {

class DaemonManager {
public:
  explicit DaemonManager(const std::string &pidFilePath);
  ~DaemonManager();

  DaemonManager(const DaemonManager &) = delete;
  DaemonManager &operator=(const DaemonManager &) = delete;

  // Daemonize the process (fork, detach, write PID file)
  // Returns true if daemonization succeeded, false on error
  // In the parent process, returns true and exits
  // In the child process, returns true and continues
  bool daemonize();

  // Setup signal handlers (SIGTERM, SIGHUP)
  // shutdownCallback is called when SIGTERM is received
  void setupSignalHandlers(std::function<void()> shutdownCallback);

  // Remove PID file
  void removePidFile() const;

  // Check if process is already running (PID file exists and process is alive)
  bool isAlreadyRunning() const;

  // Get PID from file
  int getPidFromFile() const;

private:
  std::string pidFilePath;
  bool isDaemon;
  std::function<void()> shutdownCallback;
  static bool signalHandlersSetup;
  static DaemonManager *instance;

  static void signalHandler(int signal);
  bool writePidFile() const;
};

} // namespace ModbusLogger

#endif // DAEMONMANAGER_H
