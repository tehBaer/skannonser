"""
Simple web dashboard to view database statistics.
Run with: python monitor_dashboard.py
Then open: http://localhost:8000
"""
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import sys
import os
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from main.database.db import PropertyDatabase
except ImportError:
    from database.db import PropertyDatabase


class DashboardHandler(BaseHTTPRequestHandler):
    """Simple HTTP request handler for dashboard."""
    
    def do_GET(self):
        """Handle GET requests."""
        if self.path == '/':
            self.serve_html()
        elif self.path == '/api/stats':
            self.serve_stats()
        else:
            self.send_error(404)
    
    def serve_html(self):
        """Serve the dashboard HTML."""
        html = """
<!DOCTYPE html>
<html>
<head>
    <title>Property Scraper Dashboard</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }
        h1 {
            color: #333;
            border-bottom: 3px solid #0066cc;
            padding-bottom: 10px;
        }
        .container {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }
        .card {
            background: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .card h2 {
            margin-top: 0;
            color: #0066cc;
            font-size: 1.2em;
            text-transform: uppercase;
        }
        .stat {
            display: flex;
            justify-content: space-between;
            padding: 10px 0;
            border-bottom: 1px solid #eee;
        }
        .stat:last-child {
            border-bottom: none;
        }
        .stat-label {
            color: #666;
        }
        .stat-value {
            font-weight: bold;
            color: #333;
        }
        .stat-value.active {
            color: #00aa00;
        }
        .stat-value.inactive {
            color: #cc0000;
        }
        .stat-value.pending {
            color: #ff9900;
        }
        .info {
            background: #e3f2fd;
            padding: 15px;
            border-radius: 5px;
            margin-top: 20px;
            border-left: 4px solid #0066cc;
        }
        .timestamp {
            text-align: center;
            color: #999;
            margin-top: 20px;
            font-size: 0.9em;
        }
        .refresh-btn {
            background: #0066cc;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 5px;
            cursor: pointer;
            font-size: 1em;
            margin-top: 20px;
        }
        .refresh-btn:hover {
            background: #0052a3;
        }
        .db-path {
            font-family: monospace;
            font-size: 0.9em;
            background: #f5f5f5;
            padding: 10px;
            border-radius: 3px;
            margin-top: 10px;
            word-break: break-all;
        }
    </style>
</head>
<body>
    <h1>📊 Property Scraper Dashboard</h1>
    
    <div id="db-info" class="info">
        <strong>Database:</strong>
        <div class="db-path" id="db-path">Loading...</div>
    </div>
    
    <div class="container" id="stats-container">
        <div class="card">
            <h2>⏳ Loading...</h2>
        </div>
    </div>
    
    <div style="text-align: center;">
        <button class="refresh-btn" onclick="loadStats()">🔄 Refresh</button>
    </div>
    
    <div class="timestamp" id="timestamp"></div>
    
    <script>
        async function loadStats() {
            try {
                const response = await fetch('/api/stats');
                const data = await response.json();
                
                // Update database path
                document.getElementById('db-path').textContent = data.db_path;
                
                // Create cards for each table
                const container = document.getElementById('stats-container');
                container.innerHTML = '';
                
                for (const [table, stats] of Object.entries(data.tables)) {
                    const card = document.createElement('div');
                    card.className = 'card';
                    
                    const emoji = table === 'eiendom' ? '🏠' : 
                                  table === 'leie' ? '🔑' : '💼';
                    
                    card.innerHTML = `
                        <h2>${emoji} ${table}</h2>
                        <div class="stat">
                            <span class="stat-label">Total Listings</span>
                            <span class="stat-value">${stats.total}</span>
                        </div>
                        <div class="stat">
                            <span class="stat-label">Active</span>
                            <span class="stat-value active">${stats.active}</span>
                        </div>
                        <div class="stat">
                            <span class="stat-label">Inactive</span>
                            <span class="stat-value inactive">${stats.inactive}</span>
                        </div>
                        <div class="stat">
                            <span class="stat-label">Not Exported</span>
                            <span class="stat-value pending">${stats.not_exported}</span>
                        </div>
                    `;
                    
                    container.appendChild(card);
                }
                
                // Update timestamp
                const now = new Date().toLocaleString();
                document.getElementById('timestamp').textContent = `Last updated: ${now}`;
                
            } catch (error) {
                console.error('Error loading stats:', error);
                document.getElementById('stats-container').innerHTML = 
                    '<div class="card"><h2>❌ Error loading data</h2></div>';
            }
        }
        
        // Load stats on page load
        loadStats();
        
        // Auto-refresh every 30 seconds
        setInterval(loadStats, 30000);
    </script>
</body>
</html>
        """
        
        self.send_response(200)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(html.encode('utf-8'))
    
    def serve_stats(self):
        """Serve statistics as JSON."""
        try:
            db = PropertyDatabase()
            
            stats = {
                'db_path': db.db_path,
                'timestamp': datetime.now().isoformat(),
                'tables': {}
            }
            
            for table in ['eiendom', 'leie', 'jobbe']:
                stats['tables'][table] = db.get_stats(table)
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(stats).encode('utf-8'))
            
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            error = {'error': str(e)}
            self.wfile.write(json.dumps(error).encode('utf-8'))
    
    def log_message(self, format, *args):
        """Suppress default logging."""
        pass


def main():
    """Run the dashboard server."""
    port = 8000
    server = HTTPServer(('localhost', port), DashboardHandler)
    
    print(f"\n{'='*60}")
    print(f"Property Scraper Dashboard")
    print(f"{'='*60}")
    print(f"\n✓ Server running at: http://localhost:{port}")
    print(f"\nPress Ctrl+C to stop\n")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n\n{'='*60}")
        print("Server stopped")
        print(f"{'='*60}\n")
        server.server_close()


if __name__ == "__main__":
    main()
