from prometheus_client import Gauge, Counter, Histogram, Summary, generate_latest, CollectorRegistry
from fastapi import FastAPI
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP
from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.helpers import create_future
import datetime
import asyncio
import logging

app = FastAPI()
registry = CollectorRegistry()
logger = logging.getLogger('mlmetrics')
logger.setLevel(logging.INFO)
assigned_metrics = {}


@app.get('/metrics')
def expose_metrics():
    global registry
    return generate_latest(registry)


async def get_rsync_connection(proxy_host, proxy_port):
    logger.info(f"Getting rsocket connection - proxy host {proxy_host}, proxy_port {proxy_port}...")
    connection = await asyncio.open_connection(proxy_host, proxy_port)
    return connection


async def expose_metrics_rsocket(connection):
    global registry

    class ClientHandler(BaseRequestHandler):
        async def request_response(self, p: Payload):
            return create_future(Payload(b'(client ' + p.data + b')',
                                         b'(client ' + p.metadata + b')'))

    logger.info("Starting rsocket request-response client...")

    async with RSocketClient(single_transport_provider(TransportTCP(*connection)), ClientHandler) as client:

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

        await run_request_response()


def prepare_counter(name, description, tags, value):
    global registry, assigned_metrics
    if not assigned_metrics.get(name):
        assigned_metrics[name] = Counter(name, description, tags, registry=registry)
    assigned_metrics[name].set(value)


def prepare_gauge(name, description, tags, value):
    global registry, assigned_metrics
    if not assigned_metrics.get(name):
        assigned_metrics[name] = Gauge(name, description, tags, registry=registry)
    assigned_metrics[name].set(value)


def prepare_histogram(name, description, tags, value):
    global registry, assigned_metrics
    if not assigned_metrics.get(name):
        assigned_metrics[name] = Histogram(name, description, tags, registry=registry)
    assigned_metrics[name].observe(value)


def prepare_summary(name, description, tags, value):
    global registry, assigned_metrics
    if not assigned_metrics.get(name):
        assigned_metrics[name] = Summary(name, description, tags, registry=registry)
    assigned_metrics[name].observe(value)
