import time

import pytest

from stantic.server import Server

# from testcontainers.compose import DockerCompose

# COMPOSE_PATH = "." #the folder containing docker-compose.yml


@pytest.mark.usefixtures("server")
def test_server_is_alive(server: Server):
    print(f"Server status: {server.is_alive}")
    assert server.is_alive is True

    # with DockerCompose(
    #     COMPOSE_PATH,
    #     compose_file_name=["docker-compose.yaml"],
    #     pull=False) as compose:

    #     cmd = compose.docker_compose_command()
    #     print(cmd)

    #     compose.wait_for("http://172.0.0.1:8080/FROST-Server/v1.1")
    #     print('mhm')
    #     time.sleep(10)

    #     print(compose.get_logs())


@pytest.mark.usefixtures("server")
def test_server_stuff(server: Server):
    print("Wasting time...")
    for i in range(4):
        print(i)
        time.sleep(2)

    a = 1
    assert a == 1
