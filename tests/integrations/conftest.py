import time

import pytest
from testcontainers.compose import DockerCompose

from stantic.server import Server

COMPOSE_PATH = "."  # the folder containing docker-compose.yml


# "http://172.0.0.1:8080/FROST-Server/v1.1"


@pytest.fixture(scope="session")
def server():
    frost = DockerCompose(
        COMPOSE_PATH, compose_file_name="docker-compose.yaml", pull=False
    )
    frost.start()

    service_host = frost.get_service_host("web", 8080)
    service_port = frost.get_service_port("web", 8080)
    print(f"RETURN: {service_host}:{service_port}")

    url = f"http://{service_host}:{service_port}/FROST-Server/v1.1"

    # hack
    retries = 10
    for i in range(retries):
        print(f"Waiting for system to come alive. Try: {i+1}")
        time.sleep(3)
        stdout, _ = frost.get_logs()
        if "database system is ready to accept connections" in stdout.decode(
            "unicode_escape"
        ):
            print("Finished")
            break

    server = Server(url)
    yield server
    frost.stop()
