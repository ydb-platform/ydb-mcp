import os
import socket
import time
import logging
import pytest
import platform
import docker

logger = logging.getLogger(__name__)

def get_docker_client():
    """Connect to Docker daemon via multiple methods or fail."""
    connection_errors = []
    # Method 1: default environment
    try:
        client = docker.from_env()
        client.ping()
        return client
    except Exception as e:
        connection_errors.append(f"default: {e}")
    # Method 2: DOCKER_HOST
    docker_host = os.getenv("DOCKER_HOST")
    if docker_host:
        try:
            client = docker.DockerClient(base_url=docker_host)
            client.ping()
            return client
        except Exception as e:
            connection_errors.append(f"DOCKER_HOST: {e}")
    # Method 3: common Unix sockets
    socket_paths = [
        "unix:///var/run/docker.sock",
        "unix://" + os.path.expanduser("~/.docker/run/docker.sock"),
        "unix://" + os.path.expanduser("~/.colima/default/docker.sock"),
    ]
    for sp in socket_paths:
        try:
            client = docker.DockerClient(base_url=sp)
            client.ping()
            return client
        except Exception as e:
            connection_errors.append(f"{sp}: {e}")
    # All methods failed
    logger.error("Docker connection errors:\n%s", "\n".join(connection_errors))
    pytest.fail("Could not connect to Docker. Make sure Docker daemon is running.")

def start_container(image: str, **kwargs):
    """Pull and run a Docker container with given parameters."""
    client = get_docker_client()
    # Pull image
    client.images.pull(image)
    # Run container
    container = client.containers.run(image=image, **kwargs)
    return container

def stop_container(container):
    """Stop the given Docker container."""
    try:
        container.stop(timeout=1)
    except Exception:
        logger.warning("Error stopping container %s", container)

def wait_for_port(host: str, port: int, timeout: int = 30):
    """Wait until given host:port is accepting connections or fail."""
    for _ in range(timeout):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1)
            try:
                if sock.connect_ex((host, port)) == 0:
                    return
            except Exception:
                pass
        time.sleep(1)
    pytest.fail(f"Port {host}:{port} not ready after {timeout}s")

def start_ydb_container():
    """Start a YDB Docker container for integration tests."""
    # YDB image and ports configuration
    image = "ydbplatform/local-ydb:latest"
    env = {
        "GRPC_TLS_PORT": "2135",
        "GRPC_PORT": "2136",
        "MON_PORT": "8765",
        "YDB_KAFKA_PROXY_PORT": "9092",
        "YDB_USE_IN_MEMORY_PDISKS": "1",
    }
    ports = {"2135/tcp": 2135, "2136/tcp": 2136, "8765/tcp": 8765, "9092/tcp": 9092}
    container = start_container(
        image=image,
        detach=True,
        remove=True,
        hostname="localhost",
        platform="linux/amd64",
        environment=env,
        ports=ports,
    )
    return container

def start_ollama_container():
    """Start an Ollama Docker container for integration tests by pulling `llama2` and serving it."""
    image = "ollama/ollama:latest"
    client = get_docker_client()
    # Pull the Ollama image for linux/amd64
    client.images.pull(image, platform="linux/amd64")
    # Combine model pull and serve in one container to ensure the model is available
    shell_cmd = "ollama pull llama2 && exec ollama serve --http-port 11434 --http-address 0.0.0.0"
    container = client.containers.run(
        image=image,
        command=["sh", "-c", shell_cmd],
        detach=True,
        remove=True,
        ports={"11434/tcp": 11434},
        platform="linux/amd64",
    )
    return container