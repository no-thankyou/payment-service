"""Shared fixtures.

Integration tests get real PostgreSQL and RabbitMQ from testcontainers (started lazily, once per
session). Set TEST_DATABASE_URL / TEST_RABBIT_URL to reuse already running services instead.
"""

import os
from collections.abc import Iterator

import pytest

POSTGRES_IMAGE = "postgres:16-alpine"
RABBIT_IMAGE = "rabbitmq:4-management-alpine"


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    """SQLAlchemy async URL (postgresql+asyncpg://...) of an empty database."""
    if url := os.environ.get("TEST_DATABASE_URL"):
        yield url
        return
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(POSTGRES_IMAGE, driver="asyncpg") as pg:
        yield pg.get_connection_url()


@pytest.fixture(scope="session")
def rabbit_url() -> Iterator[str]:
    """AMQP URL of a fresh RabbitMQ broker."""
    if url := os.environ.get("TEST_RABBIT_URL"):
        yield url
        return
    from testcontainers.community.rabbitmq import RabbitMqContainer

    with RabbitMqContainer(RABBIT_IMAGE) as rmq:
        host = rmq.get_container_host_ip()
        port = rmq.get_exposed_port(rmq.port)
        yield f"amqp://{rmq.username}:{rmq.password}@{host}:{port}/"
