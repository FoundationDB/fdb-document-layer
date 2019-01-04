# Overview

This directory provides a Docker image for running FoundationDB Document Layer.
It takes most of the scripts from FoundationDB Dockerfile.

The image in this directory is based on Ubuntu 18.04, but the commands and
scripts used to build it should be suitable for most other distros with small
tweaks to the installation of dependencies.

The image relies on the following dependencies:

*	bash
*	wget
*	glibc

# Build Configuration

This image supports several build arguments for build-time configuration.

### FDB_DOC_VERSION

The version of FoundationDB Documnet Layer to install in the container. This is required.

### FDB_WEBSITE

The base URL for the FoundationDB website. The default is
`https://www.foundationdb.org`.

# Runtime Configuration

This image supports several environment variables for run-time configuration.

### FDB_DOC_PORT

The port that FoundationDB Document Layer should bind to. The default is 27016. 

### FDB_NETWORKING_MODE

A networking mode that controls what address Document Layer listens on. If this
is `container` (the default), then the server will listen on its public IP
within the docker network, and will only be accessible from other containers.

If this is `host`, then the server will listen on `127.0.0.1`, and will not be
accessible from other containers. You should use `host` networking mode if you
want to access your container from your host machine, and you should also
map the port to the same port on your host machine when you run the container.

### FDB_CLUSTER_FILE_CONTENTS

This is a mandatory argument that contains the cluster file contents of the
FoundationDB cluster, Document Layer needs to connect to.
