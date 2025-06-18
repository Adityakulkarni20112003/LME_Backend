#!/usr/bin/env python3
"""
Optimized LME Aluminum Price Scraper
Fixed all memory leaks and resource issues
"""

import os
import time
import threading
import pandas as pd
import json
import signal
import sys
import atexit
import queue
import contextlib
import weakref
from datetime import datetime, timedelta
from collections import defaultdict
import logging

# Third-party imports
import psutil
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from flask import Flask, jsonify, send_file, Response, request

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Configuration
CONFIG = {
    'SCRAPING_INTERVAL': 60,  # 60 seconds between scrapes
    'MAX_RETRIES': 3,
    'MAX_CONSECUTIVE_FAILURES': 5,
    'MAX_SSE_CONNECTIONS': 5,
    'MAX_CONNECTIONS_PER_IP': 2,
    'DRIVER_POOL_SIZE': 1,
    'PAGE_LOAD_TIMEOUT': 30,
    'ELEMENT_WAIT_TIMEOUT': 20,
    'CSV_MAX_ROWS': 10000,  # Rotate CSV after 10k rows
}

# Ensure the 'scraped_csv' directory exists
csv_dir = "scraped_csv"
os.makedirs(csv_dir, exist_ok=True)
csv_path = os.path.join(csv_dir, "3_months_LME_scrap.csv")

# Initialize CSV file with headers if it doesn't exist
if not os.path.exists(csv_path):
    pd.DataFrame(columns=["Value", "Time Span", "Rate of Change", "Timestamp"]).to_csv(csv_path, index=False)

# Global data storage
latest_data = {
    "Value": None,
    "Time span": None,
    "Rate of Change": None,
    "Timestamp": None,
    "error": None
}

# Threading objects
new_data_event = threading.Event()
shutdown_event = threading.Event()

# SSE connection tracking
active_sse_connections = weakref.WeakSet()
connection_counter = defaultdict(int)

# Scraping constants
URL = "https://in.investing.com/commodities/aluminum"
XPATH_PRICE = "//div[@data-test='instrument-price-last']"
XPATH_CHANGE_VALUE = "//span[@data-test='instrument-price-change']"
XPATH_CHANGE_PERCENT = "//span[@data-test='instrument-price-change-percent']"
XPATH_TIME = "//time[@data-test='trading-time-label']"

class WebDriverPool:
    """Thread-safe WebDriver pool to prevent memory leaks"""
    
    def __init__(self, pool_size=1):
        self.pool = queue.Queue(maxsize=pool_size)
        self.pool_size = pool_size
        self.active_drivers = set()
        self.lock = threading.Lock()
        self._initialize_pool()
        
    def _initialize_pool(self):
        """Initialize the driver pool"""
        for _ in range(self.pool_size):
            driver = self._create_fresh_driver()
            if driver:
                self.pool.put(driver)
    
    def _create_fresh_driver(self):
        """Create a new WebDriver instance"""
        try:
            options = self._get_browser_options()
            driver = webdriver.Chrome(options=options)
            driver.set_page_load_timeout(CONFIG['PAGE_LOAD_TIMEOUT'])
            
            with self.lock:
                self.active_drivers.add(driver)
            
            logger.info("Created new WebDriver instance")
            return driver
        except Exception as e:
            logger.error(f"Failed to create WebDriver: {e}")
            return None
    
    def _get_browser_options(self):
        """Get optimized Chrome options"""
        options = webdriver.ChromeOptions()
        
        # Basic options
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-logging")
        options.add_argument("--log-level=3")
        
        # Resource-saving options
        options.add_argument("--disable-background-timer-throttling")
        options.add_argument("--disable-backgrounding-occluded-windows")
        options.add_argument("--disable-renderer-backgrounding")
        options.add_argument("--disable-features=TranslateUI,VizDisplayCompositor")
        options.add_argument("--disable-default-apps")
        options.add_argument("--disable-sync")
        options.add_argument("--disable-plugins")
        options.add_argument("--disable-images")
        # options.add_argument("--disable-javascript")  # Commented out as it might be needed for dynamic content
        options.add_argument("--memory-pressure-off")
        options.add_argument("--max_old_space_size=128")
        
        # User agent
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # Check if running in Docker/Render environment
        if os.environ.get('PUPPETEER_EXECUTABLE_PATH'):
            chrome_path = os.environ.get('PUPPETEER_EXECUTABLE_PATH')
            options.binary_location = chrome_path
            logger.info(f"Using Chrome binary at: {chrome_path}")
        
        return options
    
    @contextlib.contextmanager
    def get_driver(self):
        """Context manager to get a driver from the pool"""
        driver = None
        try:
            # Get driver from pool with timeout
            driver = self.pool.get(timeout=30)
            
            # Check if driver is still alive
            if not self._is_driver_alive(driver):
                logger.warning("Driver is dead, creating new one")
                self._cleanup_driver(driver)
                driver = self._create_fresh_driver()
                
            if driver:
                yield driver
            else:
                yield None
                
        except queue.Empty:
            logger.error("No driver available in pool")
            yield None
        except Exception as e:
            logger.error(f"Error getting driver from pool: {e}")
            yield None
        finally:
            # Return driver to pool if it's still alive
            if driver and self._is_driver_alive(driver):
                try:
                    self.pool.put(driver, timeout=5)
                except queue.Full:
                    # Pool is full, cleanup this driver
                    self._cleanup_driver(driver)
            elif driver:
                self._cleanup_driver(driver)
    
    def _is_driver_alive(self, driver):
        """Check if driver is still responsive"""
        try:
            driver.current_url
            return True
        except:
            return False
    
    def _cleanup_driver(self, driver):
        """Safely cleanup a driver"""
        try:
            with self.lock:
                self.active_drivers.discard(driver)
            driver.quit()
            time.sleep(1)  # Give it time to cleanup
        except Exception as e:
            logger.error(f"Error cleaning up driver: {e}")
    
    def cleanup_all(self):
        """Cleanup all drivers in the pool"""
        logger.info("Cleaning up all drivers...")
        
        # Clean up pooled drivers
        while not self.pool.empty():
            try:
                driver = self.pool.get_nowait()
                self._cleanup_driver(driver)
            except queue.Empty:
                break
            except Exception as e:
                logger.error(f"Error cleaning pooled driver: {e}")
        
        # Clean up active drivers
        with self.lock:
            active_copy = self.active_drivers.copy()
            
        for driver in active_copy:
            self._cleanup_driver(driver)
        
        # Force kill any remaining Chrome processes
        self._force_kill_chrome_processes()
    
    def _force_kill_chrome_processes(self):
        """Force kill any remaining Chrome processes"""
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                if 'chrome' in proc.info['name'].lower():
                    # Check if it's our Chrome process
                    cmdline = proc.info['cmdline'] or []
                    if any('--enable-automation' in cmd for cmd in cmdline):
                        try:
                            proc.kill()
                            logger.info(f"Killed Chrome process {proc.info['pid']}")
                        except:
                            pass
        except Exception as e:
            logger.error(f"Error force killing Chrome processes: {e}")

# Global driver pool
driver_pool = WebDriverPool(pool_size=CONFIG['DRIVER_POOL_SIZE'])

def scrape_data():
    """Scrape data using pooled driver"""
    global latest_data
    
    with driver_pool.get_driver() as driver:
        if not driver:
            latest_data["error"] = "Failed to get driver from pool"
            return False
            
        try:
            # Navigate to the page
            driver.get(URL)
            
            # Wait for main price element to be visible
            wait = WebDriverWait(driver, CONFIG['ELEMENT_WAIT_TIMEOUT'])
            wait.until(EC.visibility_of_element_located((By.XPATH, XPATH_PRICE)))
            
            # Extract data
            value = driver.find_element(By.XPATH, XPATH_PRICE).text
            rate_change_value = driver.find_element(By.XPATH, XPATH_CHANGE_VALUE).text
            rate_change_percent = driver.find_element(By.XPATH, XPATH_CHANGE_PERCENT).text
            time_span = driver.find_element(By.XPATH, XPATH_TIME).text
            
            # Combine absolute & percentage change
            rate_change = f"{rate_change_value} ({rate_change_percent})"
            
            # Current timestamp
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Update the latest data
            latest_data = {
                "Value": value,
                "Time span": time_span,
                "Rate of Change": rate_change,
                "Timestamp": timestamp,
                "error": None
            }
            
            # Save to CSV for historical records
            save_to_csv(value, time_span, rate_change, timestamp)
            
            # Notify SSE clients
            new_data_event.set()
            threading.Timer(0.1, lambda: new_data_event.clear()).start()
            
            logger.info(f"Data scraped successfully: {value} | {rate_change} | {time_span}")
            return True
        
        except TimeoutException:
            error_msg = "Timeout waiting for page elements"
            logger.error(error_msg)
            latest_data["error"] = error_msg
            return False
        except Exception as e:
            error_msg = f"Error during scraping: {str(e)}"
            logger.error(error_msg)
            latest_data["error"] = error_msg
            return False

def save_to_csv(value, time_span, rate_change, timestamp):
    """Save data to CSV with rotation"""
    try:
        # Check if CSV needs rotation
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            if len(df) >= CONFIG['CSV_MAX_ROWS']:
                # Rotate CSV file
                backup_path = csv_path.replace('.csv', f'_backup_{int(time.time())}.csv')
                os.rename(csv_path, backup_path)
                logger.info(f"Rotated CSV file to {backup_path}")
        
        # Save new data
        data = pd.DataFrame([[value, time_span, rate_change, timestamp]], 
                           columns=["Value", "Time Span", "Rate of Change", "Timestamp"])
        data.to_csv(csv_path, mode="a", index=False, 
                   header=not os.path.exists(csv_path))
        
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}")

def monitor_system_resources():
    """Monitor system resources and return status"""
    try:
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        
        # Count Chrome processes
        chrome_count = sum(1 for p in psutil.process_iter(['name']) 
                          if p.info['name'] and 'chrome' in p.info['name'].lower())
        
        # Count SSE connections
        sse_count = len(active_sse_connections)
        
        logger.info(f"Resources - CPU: {cpu_percent}% | RAM: {memory.percent}% | "
                   f"Chrome: {chrome_count} | SSE: {sse_count}")
        
        # Check if resources are too high
        if cpu_percent > 80 or memory.percent > 85 or chrome_count > 5:
            logger.warning("🚨 HIGH RESOURCE USAGE DETECTED!")
            return False
        
        return True
    except Exception as e:
        logger.error(f"Error monitoring resources: {e}")
        return True

def continuous_scraping():
    """Main scraping loop with proper error handling"""
    consecutive_failures = 0
    last_resource_check = time.time()
    
    logger.info("Starting continuous scraping...")
    
    while not shutdown_event.is_set():
        try:
            # Check system resources every 5 minutes
            if time.time() - last_resource_check > 300:
                if not monitor_system_resources():
                    # High resource usage, take a break
                    logger.warning("High resource usage, sleeping for 5 minutes")
                    time.sleep(300)
                last_resource_check = time.time()
            
            # Attempt to scrape data
            success = False
            for retry in range(CONFIG['MAX_RETRIES']):
                if shutdown_event.is_set():
                    break
                    
                success = scrape_data()
                if success:
                    consecutive_failures = 0
                    break
                else:
                    logger.warning(f"Scraping failed, retry {retry + 1}/{CONFIG['MAX_RETRIES']}")
                    time.sleep(10)  # Wait before retry
            
            # Handle consecutive failures
            if not success:
                consecutive_failures += 1
                logger.error(f"Scraping failed after {CONFIG['MAX_RETRIES']} retries. "
                           f"Consecutive failures: {consecutive_failures}")
                
                if consecutive_failures >= CONFIG['MAX_CONSECUTIVE_FAILURES']:
                    logger.critical("Too many consecutive failures, doing aggressive cleanup")
                    driver_pool.cleanup_all()
                    time.sleep(300)  # Wait 5 minutes
                    consecutive_failures = 0
            
            # Wait for next scrape
            if not shutdown_event.wait(timeout=CONFIG['SCRAPING_INTERVAL']):
                continue  # Continue if not shutting down
            else:
                break  # Shutdown requested
                
        except Exception as e:
            logger.error(f"Critical error in scraping thread: {e}")
            consecutive_failures += 1
            time.sleep(60)
    
    logger.info("Scraping thread stopped")

# Flask Routes
@app.route('/data')
def get_data():
    """Return the latest scraped data as JSON"""
    try:
        if latest_data["Value"] is not None:
            return jsonify({
                "success": True,
                "data": latest_data,
                "connections": len(active_sse_connections)
            })
        
        # Try to get latest data from CSV
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            if not df.empty:
                csv_latest = df.iloc[-1].to_dict()
                return jsonify({
                    "success": True,
                    "data": csv_latest,
                    "note": "Using latest available data from CSV",
                    "connections": len(active_sse_connections)
                })
        
        return jsonify({
            "success": False,
            "error": latest_data.get("error", "No data available")
        })
        
    except Exception as e:
        logger.error(f"Error in /data endpoint: {e}")
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500

@app.route('/stream')
def stream():
    """SSE endpoint with connection limits and proper cleanup"""
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    
    # Check connection limits
    if len(active_sse_connections) >= CONFIG['MAX_SSE_CONNECTIONS']:
        return "Too many active connections", 429
    
    if connection_counter[client_ip] >= CONFIG['MAX_CONNECTIONS_PER_IP']:
        return "Too many connections from your IP", 429
    
    def generate():
        connection_id = threading.get_ident()
        active_sse_connections.add(connection_id)
        connection_counter[client_ip] += 1
        last_heartbeat = time.time()
        
        logger.info(f"New SSE connection from {client_ip}, total: {len(active_sse_connections)}")
        
        try:
            while not shutdown_event.is_set():
                try:
                    # Check connection timeout
                    if time.time() - last_heartbeat > 600:  # 10 minutes timeout
                        logger.info("SSE connection timed out")
                        break
                    
                    # Wait for new data or timeout
                    if new_data_event.wait(timeout=60):
                        if latest_data["Value"] is not None:
                            data_str = json.dumps(latest_data)
                            yield f"data: {data_str}\n\n"
                            last_heartbeat = time.time()
                    else:
                        # Send heartbeat
                        heartbeat_data = {
                            'heartbeat': True, 
                            'timestamp': datetime.now().isoformat(),
                            'connections': len(active_sse_connections)
                        }
                        yield f"data: {json.dumps(heartbeat_data)}\n\n"
                        last_heartbeat = time.time()
                        
                except GeneratorExit:
                    break
                except Exception as e:
                    logger.error(f"Error in SSE stream: {e}")
                    break
                    
        finally:
            # Cleanup connection
            active_sse_connections.discard(connection_id)
            connection_counter[client_ip] -= 1
            if connection_counter[client_ip] <= 0:
                del connection_counter[client_ip]
            logger.info(f"SSE connection closed, remaining: {len(active_sse_connections)}")
    
    return Response(generate(), mimetype="text/event-stream",
                   headers={
                       'Cache-Control': 'no-cache',
                       'Connection': 'keep-alive',
                       'Access-Control-Allow-Origin': '*'
                   })

@app.route('/download')
def download_csv():
    """Download the complete CSV file"""
    try:
        if os.path.exists(csv_path):
            return send_file(csv_path, as_attachment=True, 
                           download_name=f"lme_data_{datetime.now().strftime('%Y%m%d')}.csv")
        else:
            return jsonify({
                "success": False,
                "error": "CSV file not found"
            }), 404
    except Exception as e:
        logger.error(f"Error in /download endpoint: {e}")
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500

@app.route('/status')
def status():
    """System status endpoint"""
    try:
        cpu_percent = psutil.cpu_percent()
        memory = psutil.virtual_memory()
        chrome_count = sum(1 for p in psutil.process_iter(['name']) 
                          if p.info['name'] and 'chrome' in p.info['name'].lower())
        
        return jsonify({
            "success": True,
            "status": {
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "chrome_processes": chrome_count,
                "active_connections": len(active_sse_connections),
                "last_scrape": latest_data.get("Timestamp"),
                "error": latest_data.get("error")
            }
        })
    except Exception as e:
        logger.error(f"Error in /status endpoint: {e}")
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500

def cleanup_on_exit():
    """Cleanup function called on exit"""
    logger.info("Performing cleanup on exit...")
    shutdown_event.set()
    
    # Cleanup driver pool
    driver_pool.cleanup_all()
    
    # Force kill any remaining Chrome processes
    try:
        for proc in psutil.process_iter(['pid', 'name']):
            if proc.info['name'] and 'chrome' in proc.info['name'].lower():
                try:
                    proc.kill()
                except:
                    pass
    except:
        pass
    
    logger.info("Cleanup completed")

def signal_handler(sig, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {sig}, shutting down gracefully...")
    cleanup_on_exit()
    sys.exit(0)

# Register cleanup handlers
atexit.register(cleanup_on_exit)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    # Register cleanup handlers
    atexit.register(cleanup_on_exit)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start resource monitoring thread
    threading.Thread(target=monitor_system_resources, daemon=True).start()
    
    # Start background scraping thread
    logger.info("Starting background scraping thread...")
    scraping_thread = threading.Thread(target=continuous_scraping, daemon=True)
    scraping_thread.start()
    
    # Get port from environment variable or use default
    port = int(os.environ.get("PORT", 5003))
    
    # Run the Flask app
    app.run(host='0.0.0.0', port=port, threaded=True)