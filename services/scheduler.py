"""
Background Scheduler Service - Auto-refresh odds every 5 minutes.
Eliminates UI blocking during game fetching.
"""
import logging
import threading
from datetime import datetime
from typing import Optional, Callable, Dict, Any

logger = logging.getLogger(__name__)


class BackgroundScheduler:
    """
    APScheduler-like background job scheduler.
    Runs odds refresh in background thread without blocking UI.
    """
    
    def __init__(self):
        self._jobs: Dict[str, dict] = {}
        self._timers: Dict[str, threading.Timer] = {}
        self._running = False
        self._lock = threading.Lock()
        self._last_run: Dict[str, datetime] = {}
        self._run_count: Dict[str, int] = {}
        self._errors: Dict[str, list] = {}
    
    def add_job(
        self, 
        func: Callable, 
        job_id: str, 
        interval_seconds: int,
        args: tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
        run_immediately: bool = False
    ) -> bool:
        """
        Add a recurring job to the scheduler.
        
        Args:
            func: Function to call
            job_id: Unique job identifier
            interval_seconds: Interval between runs
            args: Positional arguments for func
            kwargs: Keyword arguments for func
            run_immediately: Run once immediately before scheduling
        """
        if kwargs is None:
            kwargs = {}
        
        with self._lock:
            if job_id in self._jobs:
                logger.warning(f"Job {job_id} already exists, replacing")
                self.remove_job(job_id)
            
            self._jobs[job_id] = {
                'func': func,
                'interval': interval_seconds,
                'args': args,
                'kwargs': kwargs,
                'enabled': True
            }
            self._run_count[job_id] = 0
            self._errors[job_id] = []
            
            logger.info(f"Added job: {job_id} (every {interval_seconds}s)")
            
            if run_immediately:
                self._execute_job(job_id)
            
            if self._running:
                self._schedule_job(job_id)
            
            return True
    
    def remove_job(self, job_id: str) -> bool:
        """Remove a job from the scheduler."""
        with self._lock:
            if job_id in self._timers:
                self._timers[job_id].cancel()
                del self._timers[job_id]
            
            if job_id in self._jobs:
                del self._jobs[job_id]
                logger.info(f"Removed job: {job_id}")
                return True
            
            return False
    
    def start(self) -> None:
        """Start the scheduler."""
        with self._lock:
            if self._running:
                logger.warning("Scheduler already running")
                return
            
            self._running = True
            
            for job_id in self._jobs:
                self._schedule_job(job_id)
            
            logger.info(f"Scheduler started with {len(self._jobs)} jobs")
    
    def stop(self) -> None:
        """Stop the scheduler."""
        with self._lock:
            self._running = False
            
            for job_id, timer in list(self._timers.items()):
                timer.cancel()
            
            self._timers.clear()
            logger.info("Scheduler stopped")
    
    def pause_job(self, job_id: str) -> bool:
        """Pause a specific job."""
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id]['enabled'] = False
                if job_id in self._timers:
                    self._timers[job_id].cancel()
                logger.info(f"Paused job: {job_id}")
                return True
            return False
    
    def resume_job(self, job_id: str) -> bool:
        """Resume a paused job."""
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id]['enabled'] = True
                if self._running:
                    self._schedule_job(job_id)
                logger.info(f"Resumed job: {job_id}")
                return True
            return False
    
    def _schedule_job(self, job_id: str) -> None:
        """Schedule the next run of a job."""
        if job_id not in self._jobs or not self._jobs[job_id]['enabled']:
            return
        
        interval = self._jobs[job_id]['interval']
        
        timer = threading.Timer(interval, self._run_job, args=[job_id])
        timer.daemon = True
        timer.start()
        
        self._timers[job_id] = timer
    
    def _run_job(self, job_id: str) -> None:
        """Execute a job and schedule the next run."""
        self._execute_job(job_id)
        
        if self._running and job_id in self._jobs and self._jobs[job_id]['enabled']:
            self._schedule_job(job_id)
    
    def _execute_job(self, job_id: str) -> None:
        """Execute a single job."""
        if job_id not in self._jobs:
            return
        
        job = self._jobs[job_id]
        
        try:
            logger.info(f"Running job: {job_id}")
            start = datetime.now()
            
            job['func'](*job['args'], **job['kwargs'])
            
            duration = (datetime.now() - start).total_seconds()
            self._last_run[job_id] = datetime.now()
            self._run_count[job_id] = self._run_count.get(job_id, 0) + 1
            
            logger.info(f"Job {job_id} completed in {duration:.2f}s")
            
        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}")
            self._errors[job_id].append({
                'time': datetime.now().isoformat(),
                'error': str(e)
            })
            if len(self._errors[job_id]) > 10:
                self._errors[job_id] = self._errors[job_id][-10:]
    
    def trigger_job(self, job_id: str) -> bool:
        """Manually trigger a job run."""
        if job_id not in self._jobs:
            return False
        
        thread = threading.Thread(target=self._execute_job, args=[job_id])
        thread.daemon = True
        thread.start()
        return True
    
    def get_job_status(self, job_id: str) -> Optional[dict]:
        """Get status of a specific job."""
        if job_id not in self._jobs:
            return None
        
        job = self._jobs[job_id]
        return {
            'job_id': job_id,
            'enabled': job['enabled'],
            'interval': job['interval'],
            'last_run': self._last_run.get(job_id),
            'run_count': self._run_count.get(job_id, 0),
            'recent_errors': self._errors.get(job_id, [])[-3:]
        }
    
    def get_all_jobs(self) -> list:
        """Get status of all jobs."""
        return [self.get_job_status(job_id) for job_id in self._jobs]
    
    @property
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._running


_scheduler: Optional[BackgroundScheduler] = None


def get_scheduler() -> BackgroundScheduler:
    """Get the global scheduler instance."""
    global _scheduler
    if _scheduler is None:
        _scheduler = BackgroundScheduler()
    return _scheduler


def init_scheduler(app) -> BackgroundScheduler:
    """
    Initialize scheduler with Flask app context.
    Sets up auto-refresh job for odds.
    """
    scheduler = get_scheduler()
    
    def refresh_odds_job():
        """Background job to refresh odds."""
        with app.app_context():
            try:
                from sports_app import fetch_odds_internal, clear_dashboard_cache
                logger.info("Background refresh: Starting odds update")
                fetch_odds_internal()
                clear_dashboard_cache()
                logger.info("Background refresh: Odds updated successfully")
            except Exception as e:
                logger.error(f"Background refresh failed: {e}")
    
    scheduler.add_job(
        func=refresh_odds_job,
        job_id='odds_refresh',
        interval_seconds=300,
        run_immediately=False
    )
    
    scheduler.start()
    logger.info("Background scheduler initialized with 5-minute odds refresh")
    
    return scheduler
