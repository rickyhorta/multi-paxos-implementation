# Multi-Paxos Implementation

This project contains the work done on the implementation of the Multi-Paxos Consensus Algorithm. I developed this solution along with 2 university colleagues.

# Requirements

The project requires the following packages:

- Java 22
- Maven 3.8.4
- Protoc 3.12

## Environment

The project includes a template `setup_env.sh` which students may use as basis to download all necessary packages for
the project execution.
The current script was designed for Linux/Ubuntu distributions on an Intel x86 environments.
Students may extend this script for their target distribution/CPU architecture.

If so, it will keep all required packages in `INSTALL_DIR`, which can be activated by running
`source INSTALL_DIR/env.sh`.

# Compiling

To compile the project, students must run the command
`mvn clean install` in the root directory

## ⚠️ ️**WARNING - Compilation Environment** ⚠️

The project requires different `pom.xml` for the *contract* module depending on the CPU architecture/OS distribution.
The project currently has two pre-pepared poms:

- One for **ARM/M4 Mac-OS** based systems, named `arm-pom.xml`;
- One for **Intel/Linux** based systems, named `intel-pom.xml`;

Before the first compilation, copy your required contract pom and rename it to `pom.xml`.

# Deployment

Current implementation assumes that all modules run on the same physical machine and requires 5 active servers.

The project is composed of three main components:

- Servers
- App (Clients)
- Console

## Servers

The servers run the base implementation. They are executed running the following command in the *server* directory:

`mvn exec:java -Dexec.args="{port} {id} {scheduler} {max}"`

Where you must fill in the following arguments:

- **{port}**: Base port of all servers. **All servers should use the same port**. The Server binded port will be  *
  *{port} + {id}**.
- **{id}**: Sequential id of the server. Current implementation requires servers to be ID'ed starting from *0* to *N-1*
  servers.
- **{scheduler}**: The scheduler used for reconfiguration (use 'A' to start)
- **{max}**: The maximum number of participants in a meeting

## Client

A client that executes transactions. It is executed by running the following command in the *app* directory:

`mvn exec:java -Dexec.args="{id} {host} {port} {scheduler}"`

Where you must fill in the following arguments:

- **{id}**: Sequential id of the client. Current implementation requires clients to be ID'ed starting from *1*.
- **{host}**: The host for all servers. (just use "localhost")
- **{port}**: Base port of all servers.
- **{scheduler}**: The scheduler used (use 'A' to start)

The client module opens a terminal from where students may issue commands. The following commands are available:

- `help` - Shows the full command list;
- `exit` - Gracefully finishes the client.

## Console

The console client servers as a front-end to issue configuration settings to servers. It is executed by running the
following command in the *consoleclient* directory:

`mvn exec:java -Dexec.args="{host} {port} {scheduler}"`

here you must fill in the following arguments:

- **{host}**: The host for all servers. (just use "localhost")
- **{port}**: Base port of all servers.
- **{scheduler}**: The scheduler used (use 'A' to start)

The console client opens a terminal from where students may issue configuration changes to servers. The following
commands are available:

- `help` - Shows the full command list;
- `ballot ballot_number server` - Instructs a server to start ballot with number 'ballot_number';
- `debug mode replica_id` - Activates debug on a given replica;
- `exit` - Gracefully finishes the console.

## Protobuffs and Utils

To support these modules, the project has additional directories:

- *contract*, holding the required `.proto` files;
- *util*, holding the general classes to collect RPC responses
- *core*, holding the general classes that manage meetings
- *configs*, holding the general classes that manage configurations
  
