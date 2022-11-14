from prometheus_client import Gauge, Counter, Histogram, Summary, generate_latest, CollectorRegistry
from fastapi import FastAPI
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP
from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
import datetime
import asyncio
import logging

app = FastAPI()
registry = CollectorRegistry()
logger = logging.getLogger('mlmetrics')
logger.setLevel(logging.INFO)


@app.get('/metrics')
def expose_metrics():
    global registry
    return generate_latest(registry)


async def get_rsync_connection(proxy_host, proxy_port):
    logger.info(f"Getting rsync connection - proxy host {proxy_host}, proxy_port {proxy_port}...")
    connection = await asyncio.open_connection(proxy_host, proxy_port)
    return connection


async def expose_metrics_rsocket(connection):
    global registry

    logger.info("Starting rsocket request-response client...")
    async with RSocketClient(single_transport_provider(TransportTCP(*connection))) as client:

        async def run_request_response():
            try:
                while True:
                    sent = generate_latest(registry)
                    if sent is not None:
                        logger.info(f"Data to send: {sent}")
                        payload = Payload(sent)
                        result = await client.request_response(payload)
                        received = result.data

                        time_received = datetime.datetime.strptime(received.decode(), b'%Y-%m-%d %H:%M:%S'.decode())
                        logger.info(f'Response: {time_received}')

                    # Use SCDF default scrape interval of 10s
                    await asyncio.sleep(10)
            except Exception as e:
                logger.error('Error occurred: ', exc_info=True)
                pass

        asyncio.run(run_request_response())


def prepare_counter(name, description, tags, value):
    global registry
    c = Counter(name, description, tags, registry=registry)
    c.set(value)


def prepare_gauge(name, description, tags, value):
    global registry
    g = Gauge(name, description, tags, registry=registry)
    g.set(value)


def prepare_histogram(name, description, tags, value):
    global registry
    h = Histogram(name, description, tags, registry=registry)
    h.observe(value)


def prepare_summary(name, description, tags, value):
    global registry
    g = Summary(name, description, tags, registry=registry)
    g.observe(value)
