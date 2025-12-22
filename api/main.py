import logging
import asyncio
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter, Histogram, Gauge

from config import settings
from routes import auth, requests, admin, profile, services, sites, payments, revisions, domains, manager, reviews
from db import init_pool, close_pool
import cron_jobs

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
http_requests_total = Counter(
    'autosites_http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status_code']
)

http_request_duration_seconds = Histogram(
    'autosites_http_request_duration_seconds',
    'HTTP request duration in seconds',
    ['method', 'endpoint']
)

requests_created = Counter(
    'autosites_requests_created_total',
    'Total site requests created'
)

sites_deployed = Counter(
    'autosites_sites_deployed_total',
    'Total sites deployed',
    ['status']
)

active_requests = Gauge(
    'autosites_active_requests_count',
    'Number of active requests'
)

users_registered = Counter(
    'autosites_users_registered_total',
    'Total users registered'
)

payments_processed = Counter(
    'autosites_payments_processed_total',
    'Total payments processed',
    ['status']
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting API server...")
    await init_pool()

    # Start cron jobs scheduler
    cron_task = asyncio.create_task(cron_jobs.start_cron_scheduler())
    logger.info("Cron jobs scheduler started")

    yield

    # Shutdown
    logger.info("Shutting down...")
    cron_task.cancel()
    try:
        await cron_task
    except asyncio.CancelledError:
        pass
    await close_pool()


app = FastAPI(
    title="AutoSites API",
    version="1.0.0",
    lifespan=lifespan,
)

# Prometheus instrumentation
Instrumentator().instrument(app).expose(app)

# Custom metrics middleware
@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start_time = time.time()
    active_requests.inc()

    try:
        response = await call_next(request)
        duration = time.time() - start_time

        # Record metrics
        http_requests_total.labels(
            method=request.method,
            endpoint=request.url.path,
            status_code=response.status_code
        ).inc()

        http_request_duration_seconds.labels(
            method=request.method,
            endpoint=request.url.path
        ).observe(duration)

        return response
    finally:
        active_requests.dec()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routes
app.include_router(auth.router, prefix="/api/auth", tags=["auth"])
app.include_router(requests.router, prefix="/api/requests", tags=["requests"])
app.include_router(admin.router, prefix="/api/admin", tags=["admin"])
app.include_router(profile.router, prefix="/api/profile", tags=["profile"])
app.include_router(services.router, prefix="/api", tags=["services"])
app.include_router(sites.router, prefix="/api/sites", tags=["sites"])
app.include_router(payments.router, prefix="/api/payments", tags=["payments"])
app.include_router(revisions.router, prefix="/api/revisions", tags=["revisions"])
app.include_router(domains.router, prefix="/api/requests", tags=["domains"])
app.include_router(manager.router, prefix="/api/manager", tags=["manager"])
app.include_router(reviews.router, prefix="/api/reviews", tags=["reviews"])


@app.get("/api/health")
async def health_check():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.API_PORT,
        reload=settings.DEBUG,
    )
