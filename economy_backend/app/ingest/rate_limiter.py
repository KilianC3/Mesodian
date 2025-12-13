"""
Thread-safe rate limiters for API calls.

Implements token bucket algorithm for various rate limiting scenarios:
- Requests per hour (COMTRADE, EIA, WTO)
- Requests per minute (UNCTAD, ONS, ILOSTAT)
- Requests per day (EMBER)
"""
import logging
from threading import Lock
from time import time, sleep
from typing import Optional

logger = logging.getLogger(__name__)


class RateLimiter:
    """Thread-safe token bucket rate limiter."""
    
    def __init__(
        self,
        requests_per_period: int,
        period_seconds: int = 3600,
        name: str = "RateLimiter"
    ):
        """
        Initialize rate limiter with token bucket algorithm.
        
        Args:
            requests_per_period: Number of requests allowed per period
            period_seconds: Period duration in seconds (default: 1 hour = 3600)
            name: Name for logging purposes
        """
        self.rate = requests_per_period
        self.period = period_seconds
        self.tokens = float(requests_per_period)
        self.last_update = time()
        self.lock = Lock()
        self.name = name
        
        logger.info(
            f"{self.name}: Initialized with {requests_per_period} requests "
            f"per {period_seconds}s ({requests_per_period * 3600 / period_seconds:.0f} req/hour)"
        )
    
    def acquire(self, tokens: int = 1, blocking: bool = True) -> bool:
        """
        Acquire tokens from the bucket.
        
        Args:
            tokens: Number of tokens to acquire (default: 1)
            blocking: If True, wait for tokens; if False, return immediately
            
        Returns:
            True if tokens acquired, False if non-blocking and insufficient tokens
        """
        with self.lock:
            now = time()
            elapsed = now - self.last_update
            
            # Refill tokens based on elapsed time
            tokens_to_add = elapsed * self.rate / self.period
            self.tokens = min(self.rate, self.tokens + tokens_to_add)
            self.last_update = now
            
            # Check if sufficient tokens available
            if self.tokens < tokens:
                if not blocking:
                    logger.warning(
                        f"{self.name}: Insufficient tokens ({self.tokens:.2f}/{tokens}), "
                        "returning False (non-blocking)"
                    )
                    return False
                
                # Calculate wait time
                deficit = tokens - self.tokens
                sleep_time = deficit * self.period / self.rate
                
                logger.info(
                    f"{self.name}: Rate limit reached, waiting {sleep_time:.2f}s "
                    f"for {deficit:.2f} tokens"
                )
                
                sleep(sleep_time)
                self.tokens = tokens
            
            # Consume tokens
            self.tokens -= tokens
            
            if self.tokens < self.rate * 0.1:
                logger.debug(
                    f"{self.name}: Low on tokens ({self.tokens:.2f}/{self.rate})"
                )
            
            return True
    
    def get_wait_time(self, tokens: int = 1) -> float:
        """
        Get estimated wait time for tokens without acquiring.
        
        Args:
            tokens: Number of tokens needed
            
        Returns:
            Estimated wait time in seconds
        """
        with self.lock:
            now = time()
            elapsed = now - self.last_update
            
            # Calculate current token count
            current_tokens = min(
                self.rate,
                self.tokens + elapsed * self.rate / self.period
            )
            
            if current_tokens >= tokens:
                return 0.0
            
            deficit = tokens - current_tokens
            return deficit * self.period / self.rate


# Pre-configured rate limiters for each source
# Use these singleton instances across the application

# High-frequency sources (per hour)
COMTRADE_LIMITER = RateLimiter(
    requests_per_period=100,
    period_seconds=3600,
    name="COMTRADE"
)

EIA_LIMITER = RateLimiter(
    requests_per_period=1000,
    period_seconds=3600,
    name="EIA"
)

WTO_LIMITER = RateLimiter(
    requests_per_period=1000,
    period_seconds=3600,
    name="WTO"
)

# Medium-frequency sources (per minute)
UNCTAD_LIMITER = RateLimiter(
    requests_per_period=100,
    period_seconds=60,
    name="UNCTAD"
)

ONS_LIMITER = RateLimiter(
    requests_per_period=10,
    period_seconds=60,
    name="ONS"
)

ILOSTAT_LIMITER = RateLimiter(
    requests_per_period=100,
    period_seconds=60,
    name="ILOSTAT"
)

# Low-frequency sources (per day)
EMBER_LIMITER = RateLimiter(
    requests_per_period=1000,
    period_seconds=86400,  # 24 hours
    name="EMBER"
)

# Polite limiters for sources without official limits
WDI_LIMITER = RateLimiter(
    requests_per_period=10,
    period_seconds=60,
    name="WDI"
)


def rate_limit(limiter: Optional[RateLimiter], tokens: int = 1) -> None:
    """
    Convenience function to apply rate limiting.
    
    Args:
        limiter: Rate limiter instance (or None to skip)
        tokens: Number of tokens to acquire
    """
    if limiter is not None:
        limiter.acquire(tokens=tokens, blocking=True)
