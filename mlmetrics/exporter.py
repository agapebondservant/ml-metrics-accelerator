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
from typing import Optional
import nest_asyncio
nest_asyncio.apply()

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

        def __init__(self):
            self.log = logging.getLogger('ClientHandler')
            self.log.setLevel(logging.INFO)
            # self.received = asyncio.Event()
            # self.received_payload: Optional[Payload] = None

        async def request_response(self, payload: Payload):
            try:
                key = payload.data if payload else None
                self.log.info(f"In request_response method...{key}")
                sent = generate_latest(registry)
                self.log.info(f"Metrics data...{sent}")
                return create_future(Payload(sent))
            except Exception as e:
                logger.error('Error occurred: ', exc_info=True)

        # async def request_fire_and_forget(self, payload: Payload):
        #    self.log.info("In request_fire_and_forget method...")
        #    self.log.info(f"{payload} {payload.data}") if payload else True
        #    self.received_payload = payload
        #    self.received.set()

    logger.info("Starting rsocket request-response client...")

    async with RSocketClient(single_transport_provider(TransportTCP(*connection)), handler_factory=ClientHandler) as client:

        client.set_handler_using_factory(ClientHandler)

        async def run_request_response():

            while True:
                try:
                    """sent = generate_latest(registry)
                    if sent:
                        logger.info(f"Data to send: {sent}")
                        payload = Payload(sent)
                        # client.fire_and_forget(payload)
                        # result = await client.request_response(payload)
                        # key = result.data
                        logger.info(f'Data sent.')"""
                    pass
                except Exception as e:
                    logger.error('Error occurred: ', exc_info=True)
                finally:
                    # Use SCDF default scrape interval of 10s
                    await asyncio.sleep(10)

        asyncio.run(run_request_response())


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
