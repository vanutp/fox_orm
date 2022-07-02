import socket
import subprocess

import psycopg2
from sqlalchemy import Table


def schema_to_set(table: Table):
    return {(x.name, x.type.__class__) for x in table.columns.values()}


def port_occupied(port: int):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    res = s.connect_ex(('localhost', port)) == 0
    s.close()
    return res


def try_connect_postgres(db_uri: str):
    try:
        psycopg2.connect(db_uri)
        return True
    except psycopg2.OperationalError:
        return False


def start_postgres(port: int):
    res = subprocess.run(
        [
            'docker',
            'run',
            '--rm',
            '-p',
            f'127.0.0.1:{port}:{port}',
            '-e',
            'POSTGRES_PASSWORD=postgres',
            '-d',
            'postgres',
            f'-p {port}',
        ],
        capture_output=True,
    )
    if res.returncode != 0:
        raise Exception('Failed to start postgres')
    return res.stdout.strip().rsplit(b'\n', 1)[-1].decode()


def stop_container(container_id: str):
    res = subprocess.run(
        ['docker', 'stop', container_id],
        capture_output=True,
    )
    if res.returncode != 0:
        raise Exception('Failed to stop container')
