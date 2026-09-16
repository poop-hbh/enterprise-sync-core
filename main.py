import asyncio
import hashlib
import json
import logging
import os
import re
import uuid
from contextlib import asynccontextmanager
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from time import time as _time
from typing import Optional, Set

import httpx
from fastapi import FastAPI, Header, HTTPException, Request, Response
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import DateTime, Numeric, String, delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# ---------------- Config ----------------
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite+aiosqlite:///./app.db")
SYNC_ENDPOINT = os.environ.get("SYNC_ENDPOINT", "https://httpbin.org/post")
RECOVERY_INTERVAL = int(os.environ.get("RECOVERY_INTERVAL", "15"))
CLEANER_INTERVAL = int(os.environ.get("CLEANER_INTERVAL", "300"))
IDEMPOTENCY_TTL_HOURS = int(os.environ.get("IDEMPOTENCY_TTL_HOURS", "1"))
RECOVERY_BATCH = int(os.environ.get("RECOVERY_BATCH", "100"))
RECOVERY_STUCK_MINUTES = int(os.environ.get("RECOVERY_STUCK_MINUTES", "5"))
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "10"))

ORDER_RATE_LIMIT = int(os.environ.get("ORDER_RATE_LIMIT", "30"))
ORDER_RATE_WINDOW = int(os.environ.get("ORDER_RATE_WINDOW", "60"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s",
)
logger = logging.getLogger("EnterpriseSyncCore")

# ---------------- Engine ----------------
engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
AsyncSessionLocal = async_sessionmaker(
    bind=engine, class_=AsyncSession, expire_on_commit=False
)


# ---------------- Models ----------------
class Base(DeclarativeBase):
    pass


class OrderModel(Base):
    __tablename__ = "orders"
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    client_name: Mapped[str] = mapped_column(String(100), nullable=False)
    client_email: Mapped[str] = mapped_column(String(254), nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )


class OutboxModel(Base):
    __tablename__ = "sync_outbox"
    order_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, index=True, default="PENDING"
    )
    error_log: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    attempt: Mapped[int] = mapped_column(default=0, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )


class IdempotencyModel(Base):
    __tablename__ = "idempotency_cache"
    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    order_id: Mapped[str] = mapped_column(String(32), nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )


# ---------------- Schemas ----------------
class OrderIn(BaseModel):
    client_name: str = Field(..., min_length=2, max_length=100)
    client_email: EmailStr
    amount: Decimal = Field(
        ...,
        gt=Decimal("0"),
        le=Decimal("9999999999.99"),
        allow_inf_nan=False,
    )


class SyncStatusResponse(BaseModel):
    order_id: str
    status: str
    error_log: Optional[str] = None


# ---------------- HTTP client ----------------
class HttpClientManager:
    def __init__(self) -> None:
        self.client: Optional[httpx.AsyncClient] = None

    def start(self) -> None:
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                timeout=HTTP_TIMEOUT,
                connect=2.0,
                read=HTTP_TIMEOUT,
                write=HTTP_TIMEOUT,
                pool=2.0,
            ),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )

    async def stop(self) -> None:
        if self.client:
            await self.client.aclose()
            self.client = None


http_manager = HttpClientManager()

# ---------------- Background task helpers ----------------
active_tasks: Set[asyncio.Task] = set()
_UNSET = object()


def _sanitize_log(value: str, limit: int = 500) -> str:
    if value is None:
        return ""
    return re.sub(r"[\r\n\t]", " ", str(value))[:limit]


def _handle_task_result(task: asyncio.Task) -> None:
    active_tasks.discard(task)
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.exception("[TASK-FATAL] background task crashed")


def run_background_task(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    active_tasks.add(task)
    task.add_done_callback(_handle_task_result)
    return task


# ---------------- Rate limit ----------------
_rate_store: dict[str, list[float]] = defaultdict(list)


def check_order_rate(ip: str) -> bool:
    now = _time()
    _rate_store[ip] = [t for t in _rate_store[ip] if now - t < ORDER_RATE_WINDOW]
    if len(_rate_store[ip]) >= ORDER_RATE_LIMIT:
        return False
    _rate_store[ip].append(now)
    return True


# ---------------- Sync engine ----------------
class ResilientSyncEngine:
    async def _persist(
        self,
        order_id: str,
        *,
        status: Optional[str] = None,
        error_log=_UNSET,
        attempt: Optional[int] = None,
        touch_updated_at: bool = True,
    ) -> None:
        values = {}
        if status is not None:
            values["status"] = status
        if error_log is not _UNSET:
            values["error_log"] = _sanitize_log(error_log, 500) if error_log else None
        if attempt is not None:
            values["attempt"] = attempt
        if touch_updated_at:
            values["updated_at"] = datetime.now(timezone.utc)
        if not values:
            return
        async with AsyncSessionLocal() as session:
            async with session.begin():
                await session.execute(
                    update(OutboxModel)
                    .where(OutboxModel.order_id == order_id)
                    .values(**values)
                )

    async def push_with_retry(self, order_id: str, max_retries: int = 4) -> bool:
        if http_manager.client is None:
            logger.error("[SYNC] HTTP client not initialized, aborting job=%s", order_id)
            await self._persist(
                order_id,
                status="CRITICAL_FAILED",
                error_log="HTTP client not initialized",
            )
            return False

        delay = 1.0
        for attempt in range(1, max_retries + 1):
            logger.info("[SYNC] job=%s attempt=%s/%s", order_id, attempt, max_retries)
            err: Optional[str] = None
            retryable = False

            try:
                resp = await http_manager.client.post(
                    SYNC_ENDPOINT,
                    json={"order_id": order_id, "attempt": attempt},
                )
                if resp.status_code == 429 or 500 <= resp.status_code < 600:
                    retryable = True
                    raise httpx.HTTPStatusError(
                        f"Remote status {resp.status_code}",
                        request=resp.request,
                        response=resp,
                    )
                resp.raise_for_status()

                await self._persist(
                    order_id,
                    status="SYNCED",
                    error_log=None,
                    attempt=attempt,
                )
                logger.info("[SYNC-SUCCESS] job=%s", order_id)
                return True

            except httpx.HTTPStatusError as e:
                code = e.response.status_code if e.response is not None else 0
                err = f"HTTP {code}"
                retryable = code == 429 or 500 <= code < 600
            except httpx.TransportError as e:
                err = f"transport: {type(e).__name__}"
                retryable = True
            except Exception as e:
                err = f"internal: {type(e).__name__}"
                retryable = False

            if not retryable or attempt == max_retries:
                await self._persist(
                    order_id,
                    status="CRITICAL_FAILED",
                    error_log=err,
                    attempt=attempt,
                )
                logger.error("[SYNC-FAILED] job=%s err=%s", order_id, err)
                return False

            await self._persist(order_id, error_log=err, attempt=attempt)
            await asyncio.sleep(delay)
            delay *= 2
        return False


sync_engine = ResilientSyncEngine()


# ---------------- Daemons ----------------
async def auto_recovery_daemon() -> None:
    while True:
        try:
            await asyncio.sleep(RECOVERY_INTERVAL)
            stuck_bound = datetime.now(timezone.utc) - timedelta(
                minutes=RECOVERY_STUCK_MINUTES
            )

            async with AsyncSessionLocal() as session:
                async with session.begin():
                    subq = (
                        select(OutboxModel.order_id)
                        .where(
                            (OutboxModel.status == "CRITICAL_FAILED")
                            | (
                                (OutboxModel.status == "RECOVERING")
                                & (OutboxModel.updated_at < stuck_bound)
                            )
                        )
                        .order_by(OutboxModel.updated_at)
                        .limit(RECOVERY_BATCH)
                        .scalar_subquery()
                    )
                    stmt = (
                        update(OutboxModel)
                        .where(OutboxModel.order_id.in_(subq))
                        .where(OutboxModel.status != "RECOVERING")
                        .values(
                            status="RECOVERING",
                            attempt=0,
                            updated_at=datetime.now(timezone.utc),
                        )
                        .returning(OutboxModel.order_id)
                    )
                    claimed = (await session.execute(stmt)).scalars().all()

            for order_id in claimed:
                logger.warning("[CRON-HEAL] re-queue job=%s", order_id)
                run_background_task(sync_engine.push_with_retry(order_id))

        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[CRON-ERROR] recovery daemon crash")


async def cache_cleaner_daemon() -> None:
    while True:
        try:
            await asyncio.sleep(CLEANER_INTERVAL)
            bound = datetime.now(timezone.utc) - timedelta(hours=IDEMPOTENCY_TTL_HOURS)
            total = 0
            while True:
                async with AsyncSessionLocal() as session:
                    async with session.begin():
                        subq = (
                            select(IdempotencyModel.key)
                            .where(IdempotencyModel.created_at < bound)
                            .order_by(IdempotencyModel.created_at)
                            .limit(10000)
                            .scalar_subquery()
                        )
                        result = await session.execute(
                            delete(IdempotencyModel).where(
                                IdempotencyModel.key.in_(subq)
                            )
                        )
                        deleted = result.rowcount or 0
                        total += deleted
                        if deleted < 10000:
                            break
            if total:
                logger.info("[GC] cleaned idempotency keys=%s", total)

        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[GC-ERROR] cleaner daemon crash")


# ---------------- Lifespan ----------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    recovery: Optional[asyncio.Task] = None
    cleaner: Optional[asyncio.Task] = None
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        http_manager.start()
        recovery = asyncio.create_task(auto_recovery_daemon())
        cleaner = asyncio.create_task(cache_cleaner_daemon())
        yield
    finally:
        logger.info("[SHUTDOWN] graceful shutdown...")
        if recovery is not None:
            recovery.cancel()
        if cleaner is not None:
            cleaner.cancel()
        await asyncio.gather(
            *[t for t in (recovery, cleaner) if t is not None],
            return_exceptions=True,
        )
        for _ in range(10):
            if not active_tasks:
                break
            await asyncio.gather(*list(active_tasks), return_exceptions=True)
        await http_manager.stop()
        await engine.dispose()
        logger.info("[SHUTDOWN] done")


app = FastAPI(title="Ultimate Resilient Migration Gateway", lifespan=lifespan)


# ---------------- Helpers ----------------
def _canonical_amount(amount: Decimal) -> str:
    return str(amount.quantize(Decimal("0.01")))


def _payload_hash(payload: OrderIn) -> str:
    data = payload.model_dump(mode="json")
    data["amount"] = _canonical_amount(payload.amount)
    raw = json.dumps(data, sort_keys=True).encode()
    return hashlib.sha256(raw).hexdigest()


# ---------------- Endpoints ----------------
@app.post("/api/v1/orders")
async def create_secure_order(
    payload: OrderIn,
    request: Request,
    response: Response,
    x_idempotency_key: Optional[str] = Header(None, max_length=255),
):
    client_ip = request.client.host if request.client else "unknown"
    if not check_order_rate(client_ip):
        raise HTTPException(
            status_code=429,
            detail="Too many requests. Slow down.",
        )

    generated_id = uuid.uuid4().hex
    payload_hash = _payload_hash(payload)
    idem_key = x_idempotency_key

    async with AsyncSessionLocal() as session:
        async with session.begin():
            if idem_key:
                existing = (
                    await session.execute(
                        select(IdempotencyModel).where(
                            IdempotencyModel.key == idem_key
                        )
                    )
                ).scalar_one_or_none()

                if existing is not None:
                    if existing.payload_hash != payload_hash:
                        raise HTTPException(
                            status_code=409,
                            detail="Idempotency key reused with different payload",
                        )
                    logger.info(
                        "[IDEMPOTENT] replay key=%s",
                        _sanitize_log(idem_key, 255),
                    )
                    response.status_code = 200
                    return {
                        "status": "accepted",
                        "order_id": existing.order_id,
                        "message": "Idempotent replay.",
                    }

                session.add(
                    IdempotencyModel(
                        key=idem_key,
                        order_id=generated_id,
                        payload_hash=payload_hash,
                    )
                )

            session.add(
                OrderModel(
                    id=generated_id,
                    client_name=payload.client_name,
                    client_email=payload.client_email,
                    amount=payload.amount,
                )
            )
            session.add(OutboxModel(order_id=generated_id, status="PENDING"))

    logger.info("[DB-COMMIT] order=%s", generated_id)
    run_background_task(sync_engine.push_with_retry(generated_id))
    response.status_code = 201
    return {
        "status": "accepted",
        "order_id": generated_id,
        "message": "Accepted, background sync scheduled.",
    }


@app.get("/api/v1/sync/status/{order_id}", response_model=SyncStatusResponse)
async def check_sync_status(order_id: str):
    async with AsyncSessionLocal() as session:
        row = (
            await session.execute(
                select(OutboxModel).where(OutboxModel.order_id == order_id)
            )
        ).scalar_one_or_none()

    if not row:
        raise HTTPException(status_code=404, detail="Sync job not found")

    return SyncStatusResponse(
        order_id=row.order_id,
        status=row.status,
        error_log=row.error_log,
    )
