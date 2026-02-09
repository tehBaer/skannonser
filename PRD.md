# Product Requirements Document (PRD)
## Norwegian Classified Ads Scraping & Monitoring System

**Version:** 1.0  
**Last Updated:** February 9, 2026  
**Document Owner:** Product Team  

---

## 1. Executive Summary

### 1.1 Product Overview
This system is an automated web scraping and monitoring platform designed to track Norwegian classified ads across multiple categories: real estate (eiendom), rental properties (flippe), and job listings (jobbe). The platform aggregates data from FINN.no and NAV Arbeidsplassen, processes it through intelligent extraction pipelines, stores it in a SQLite database, and synchronizes with Google Sheets for easy access and management.

### 1.2 Problem Statement
Finding and tracking relevant classified ads across multiple Norwegian platforms is time-consuming and inefficient. Users need:
- Automated collection of listings matching specific criteria
- Centralized data storage and management
- Easy-to-use interface for viewing and analyzing listings
- Location-based intelligence (commute times, proximity to amenities)
- Historical tracking of listings and price changes

### 1.3 Target Users
- Real estate investors and property seekers
- Rental property managers
- Job seekers and recruiters
- Market analysts tracking Norwegian classified ad trends

---

## 2. Product Goals & Success Metrics

### 2.1 Primary Goals
1. **Automation**: Reduce manual effort in tracking classified ads by 95%
2. **Data Quality**: Achieve >90% accuracy in extracted listing data
3. **Timeliness**: Update listings within 1 hour of posting on source platforms
4. **Accessibility**: Provide easy Google Sheets access for non-technical users

### 2.2 Success Metrics
- **Listings tracked**: Number of active listings in database
- **Scrape success rate**: % of URLs successfully processed
- **Data freshness**: Average age of listings in database
- **System uptime**: % of scheduled scrapes completed successfully
- **User engagement**: Frequency of Google Sheets access

---

## 3. Core Features & Functionality

### 3.1 Web Scraping Engine

#### 3.1.1 URL Discovery (`crawl.py`)
**Description**: Automated discovery of listing URLs from search result pages

**Functionality**:
- Multi-page pagination support for FINN.no and NAV platforms
- Regex pattern matching for URL extraction
- Intelligent filtering of duplicate and invalid URLs
- Support for custom search parameters and location polygons

**Technical Details**:
- Uses BeautifulSoup for HTML parsing
- Saves raw HTML for audit/debugging purposes
- Handles both FINN.no and NAV pagination schemes

#### 3.1.2 Data Extraction

##### **Property/Rental Extraction** (`extraction_eiendom.py`, `extraction_rental.py`)
Extracts comprehensive property data:
- **Basic Info**: Finnkode (unique ID), address, postal code, URL
- **Pricing**: Purchase price or monthly rent, deposit (rentals)
- **Property Details**: Multiple area measurements (primary area, usable area, gross area, outdoor area)
- **Status**: Availability status, listing state

##### **Job Listing Extraction** (`extraction_jobs_FINN.py`, `extraction_jobs_NAV.py`)
Extracts job posting data:
- **Position Info**: Job title, company name, industry
- **Application Details**: Deadline, number of positions
- **Content**: Full job description text
- **Source-specific metadata**: Different parsing for FINN vs NAV

**Error Handling**:
- Robust try-catch for individual listing failures
- Failed URL logging with error details
- Differentiation between planned properties vs errors
- Retry capabilities

### 3.2 Data Storage & Management

#### 3.2.1 Database Layer (`database/db.py`)
**Technology**: SQLite with pandas integration

**Database Schema**:

**Eiendom (Property) Table**:
```sql
- id (PRIMARY KEY)
- finnkode (UNIQUE, NOT NULL)
- tilgjengelighet (availability status)
- adresse (address)
- postnummer (postal code)
- pris (price)
- url
- areal (area in sqm)
- pris_kvm (price per sqm)
- scraped_at (timestamp)
- updated_at (timestamp)
- is_active (boolean)
- exported_to_sheets (boolean)
```

**Features**:
- Automatic duplicate detection via unique finnkode
- Timestamp tracking for insertion and updates
- Active/inactive status management
- Export tracking to prevent duplicate syncs
- Property deletion and override capabilities
- Bulk operations for efficiency

#### 3.2.2 Property Overrides (`database/overrides.py`)
**Purpose**: Manual intervention system for data corrections

**Capabilities**:
- Override individual field values
- Track override history
- Priority system (manual overrides take precedence)
- Audit trail of all modifications

### 3.3 Post-Processing & Enrichment

#### 3.3.1 Data Transformation (`post_process.py`)
**Features**:
- Price per square meter calculation
- Data type normalization
- Missing data imputation strategies
- Outlier detection and flagging

#### 3.3.2 Location Intelligence (`location_features.py`)
**Extensible Framework** for calculating location-based features:

**Implemented Features**:
- **Commute Calculator**: Transit time to specified addresses using Google Maps API
- **Distance to Amenities**: Walking/driving distance to grocery stores, transit stops
- **Geocoding**: Address to lat/long conversion
- **Route Analysis**: Multi-modal transport calculations

**Configuration System**:
- Feature registry pattern for easy additions
- Per-feature configuration options
- Caching to minimize API calls
- Fallback strategies for API failures

**Example Use Cases**:
```python
# Calculate commute time to workplace
commute_time = calculate_commute(property_address, work_address, mode="transit")

# Find nearest grocery store walking time
grocery_distance = calculate_nearest_poi(property_address, poi_type="grocery")
```

### 3.4 Google Sheets Integration

#### 3.4.1 Sync Operations (`sync/`)
**Bidirectional sync** between SQLite database and Google Sheets:

**Add New Listings** (`add_new_to_sheet.py`):
- Identifies un-exported listings in database
- Formats data for sheet insertion
- Batch uploads to minimize API calls
- Marks records as exported

**Update Existing Rows** (`update_rows_in_sheet.py`):
- Detects data changes in database
- Updates corresponding sheet rows
- Preserves manual sheet edits where configured
- Conflict resolution strategies

**Refresh from Sheet** (`refresh_listings.py`):
- Pull manual changes from sheets back to database
- Sync override values
- Handle deletions marked in sheets

**Technical Features**:
- OAuth2 authentication (token.json)
- Service account support
- Rate limiting and retry logic
- Sheet format preservation

### 3.5 Automation & Scheduling

#### 3.5.1 Task Scheduler (`tools/scheduler.py`)
**Purpose**: Orchestrate regular scraping runs

**Capabilities**:
- Configurable task schedules
- Task chaining (scrape → process → sync)
- Error notification
- Execution logging
- Cron-compatible for system scheduling

**Supported Tasks**:
- `eiendom`: Property listings scraper
- `rental`: Rental property scraper
- `jobs`: Job listings scraper

**Typical Schedule**:
```
# Every 6 hours
0 */6 * * * python scheduler.py --task eiendom --sync
```

#### 3.5.2 Runner Scripts (`runners/`)
**Task-specific entry points**:
- `run_eiendom_db.py`: Real estate scraper
- `run_rental.py`: Rental scraper
- `run_jobs_FINN.py`: FINN job scraper
- `run_jobs_NAV.py`: NAV job scraper

**Shared Functionality** (`run_helper.py`):
- Virtual environment validation
- Dependency checking
- Configuration loading

### 3.6 Monitoring & Observability

#### 3.6.1 Web Dashboard (`monitoring/monitor_dashboard.py`)
**Purpose**: Real-time system health monitoring

**Dashboard Features**:
- **Statistics Overview**: 
  - Total listings (active/inactive)
  - Listings added today/this week
  - Export status
  - Database size
- **Recent Activity**: Latest scrapes and additions
- **Error Tracking**: Failed URLs and error patterns
- **Category Breakdown**: Stats per listing type

**Access**: HTTP server on localhost:8000

**Technology**: Lightweight Python HTTP server with JSON API

#### 3.6.2 Test & Validation (`monitoring/test_setup.py`)
**Purpose**: Validate system configuration and dependencies

**Checks**:
- Database connectivity
- Google Sheets API credentials
- Google Maps API key
- Required Python packages
- File system permissions
- Network connectivity to source sites

### 3.7 Configuration Management

#### 3.7.1 Central Configuration (`config/config.py`)
**Contains**:
- API keys (Google Maps, etc.)
- Database paths
- Default parameters
- Feature flags

#### 3.7.2 Search Filters (`config/filters.py`)
**Purpose**: Define scraping criteria

**Filter Options**:
- Geographic boundaries (polygon coordinates)
- Property types (apartment, house, etc.)
- Price ranges
- Area size requirements
- Lifecycle status (new, sold, etc.)

---

## 4. Data Flow Architecture

### 4.1 High-Level Flow
```
1. URL Discovery (crawl.py)
   ↓
2. HTML Extraction & Storage
   ↓
3. Data Parsing (extractors/)
   ↓
4. Database Storage (database/db.py)
   ↓
5. Post-Processing (post_process.py)
   ↓
6. Feature Enrichment (location_features.py)
   ↓
7. Google Sheets Sync (sync/)
   ↓
8. Monitoring Dashboard (monitoring/)
```

### 4.2 Directory Structure
```
data/
  ├── eiendom/          # Property listings
  │   ├── 0_URLs.csv    # Discovered URLs
  │   ├── A_live.csv    # Active listings
  │   ├── AB_processed.csv  # Enriched data
  │   ├── html_crawled/ # Search result pages
  │   └── html_extracted/ # Individual listing HTML
  ├── flippe/           # Rental listings
  └── jobbe/            # Job listings

main/
  ├── config/           # Configuration & credentials
  ├── database/         # SQLite database layer
  ├── extractors/       # HTML parsing logic
  ├── runners/          # Task entry points
  ├── sync/             # Google Sheets integration
  ├── monitoring/       # Dashboard & health checks
  └── tools/            # Utilities & scheduler
```

---

## 5. Technical Requirements

### 5.1 Dependencies
**Core Libraries**:
- `beautifulsoup4`: HTML parsing
- `pandas`: Data manipulation
- `requests`: HTTP requests
- `sqlite3`: Database (standard library)
- `google-api-python-client`: Sheets integration
- `geopy`: Geocoding and distance calculations

**API Requirements**:
- Google Maps API key (for location features)
- Google Sheets API credentials (OAuth2 or service account)

### 5.2 System Requirements
- **Python**: 3.8+
- **Disk Space**: ~500MB for database and HTML cache
- **Network**: Stable internet connection for scraping
- **OS**: macOS, Linux, or Windows (WSL recommended)

### 5.3 Performance Requirements
- **Scrape Speed**: 1-2 seconds per listing
- **Database Query Time**: <100ms for typical queries
- **Sheet Sync Time**: <30 seconds for 100 new listings
- **Dashboard Load Time**: <2 seconds

### 5.4 Reliability Requirements
- **Error Recovery**: Graceful handling of network failures
- **Data Integrity**: ACID compliance for database operations
- **Idempotency**: Safe to re-run scrapes without duplicates

---

## 6. User Stories

### 6.1 Real Estate Investor
> "As a real estate investor, I want to automatically track all new property listings in my target areas so that I can be first to identify good investment opportunities."

**Acceptance Criteria**:
- System checks for new listings every 6 hours
- New listings appear in Google Sheet within 1 hour
- Price per sqm is calculated automatically
- Duplicate listings are filtered out

### 6.2 Rental Property Manager
> "As a property manager, I want to monitor rental prices in specific neighborhoods to price my units competitively."

**Acceptance Criteria**:
- Can define custom geographic boundaries
- Historical price tracking over time
- Neighborhood-level aggregations
- Export to CSV for analysis

### 6.3 Job Seeker
> "As a job seeker, I want to track all tech job postings with application deadlines so I never miss an opportunity."

**Acceptance Criteria**:
- Automatic extraction of deadlines
- Company and industry categorization
- Full job description text searchable
- Links directly to application pages

### 6.4 Data Analyst
> "As an analyst, I want API access to historical listing data to build market trend models."

**Acceptance Criteria**:
- SQLite database with documented schema
- CSV export functionality
- Timestamp tracking for all records
- Data quality flags and error logs

---

## 7. Feature Roadmap

### 7.1 Current State (✅ Implemented)
- ✅ Web scraping for FINN.no (properties, rentals, jobs)
- ✅ Web scraping for NAV jobs platform
- ✅ SQLite database storage
- ✅ Google Sheets bidirectional sync
- ✅ Location-based features (commute, distance)
- ✅ Web monitoring dashboard
- ✅ Scheduled scraping
- ✅ Error tracking and logging

### 7.2 Phase 2 (🚧 Partially Implemented)
- 🚧 Complete database migration for all listing types (only eiendom fully migrated)
- 🚧 Enhanced location features (currently framework exists but limited usage)
- 🚧 Price change tracking
- 🚧 Automated alerts for matching listings

### 7.3 Phase 3 (📋 Planned)
- 📋 REST API for external access
- 📋 Machine learning price predictions
- 📋 Image download and analysis
- 📋 Email/SMS notifications
- 📋 Multi-user support with permissions
- 📋 Advanced filtering and saved searches
- 📋 Mobile app or responsive web interface
- 📋 Data visualization and analytics dashboard

### 7.4 Future Considerations
- Expansion to other Norwegian platforms (Hybel.no, Tise, etc.)
- International market support
- Real-time websocket updates
- Blockchain-based listing verification
- Integration with property management software

---

## 8. Non-Functional Requirements

### 8.1 Scalability
- Support for 10,000+ active listings per category
- Horizontal scaling via multiple scraper instances
- Database partitioning for historical data

### 8.2 Security
- API keys stored in config files (excluded from version control)
- OAuth2 authentication for Google services
- No storage of personal user data
- Rate limiting to respect source site policies

### 8.3 Maintainability
- Modular architecture with clear separation of concerns
- Consistent coding standards and documentation
- Comprehensive error logging
- Automated testing (TODO: currently minimal)

### 8.4 Compliance
- **Robots.txt Compliance**: Respect crawling rules
- **Rate Limiting**: Avoid overwhelming source sites
- **Data Retention**: GDPR considerations for any personal data
- **Terms of Service**: Adherence to FINN.no and NAV usage policies

### 8.5 Usability
- Non-technical users can access data via Google Sheets
- Clear dashboard for system status
- Intuitive sheet layouts with descriptive column names
- Norwegian language support in UI elements

---

## 9. Known Limitations & Technical Debt

### 9.1 Current Limitations
1. **CSV Migration Incomplete**: Rental and jobs still use legacy CSV storage
2. **Limited Testing**: No automated test suite
3. **Manual Configuration**: API keys hardcoded in config.py
4. **Single-User System**: No authentication or multi-user support
5. **No Real-Time Updates**: Polling-based, not event-driven
6. **HTML Storage**: Large storage footprint from saved HTML
7. **Error Recovery**: Manual intervention needed for persistent scraping failures

### 9.2 Technical Debt
1. **Mixed Import Patterns**: Inconsistent try/except for relative imports
2. **Hardcoded Paths**: Some absolute paths in configuration
3. **Limited Location Feature Usage**: Framework built but underutilized
4. **Google API Key Exposed**: Should use environment variables
5. **No Logging Framework**: Print statements instead of proper logging
6. **Duplicate Code**: Extraction logic shares patterns that could be consolidated

### 9.3 Risk Mitigation
| Risk | Impact | Mitigation |
|------|--------|------------|
| Source site structure changes | High | Regular monitoring, version HTML storage |
| API rate limits exceeded | Medium | Implement exponential backoff, caching |
| Google Sheets quota exhaustion | Medium | Batch operations, monitor usage |
| Database corruption | High | Regular backups, transaction management |
| Hardcoded credentials leaked | Critical | Move to environment variables immediately |

---

## 10. Success Criteria & KPIs

### 10.1 Launch Criteria
- [ ] All three categories (eiendom, rental, jobs) using database storage
- [ ] 99% scrape success rate over 1-week period
- [ ] Google Sheets sync working bidirectionally without data loss
- [ ] Dashboard accessible and showing accurate metrics
- [ ] Scheduler running for 30 days without manual intervention
- [ ] Documentation complete for setup and usage

### 10.2 Key Performance Indicators
| KPI | Target | Measurement |
|-----|--------|-------------|
| Scrape Success Rate | >95% | Successful extractions / Total URLs |
| Data Freshness | <6 hours | Average age of active listings |
| System Uptime | >99% | Successful scheduled runs / Total runs |
| Sheet Sync Latency | <5 minutes | Time from DB insert to Sheet update |
| Error Rate | <2% | Failed URLs / Total URLs processed |
| Dashboard Response | <3 seconds | Time to load dashboard |

---

## 11. Open Questions & Decisions

### 11.1 Pending Decisions
1. **Authentication Strategy**: OAuth2 vs Service Account for production?
2. **Data Retention Policy**: How long to keep inactive listings?
3. **Notification System**: Which channels (email, SMS, Slack)?
4. **API Design**: RESTful vs GraphQL for external access?
5. **Deployment**: Cloud (AWS, GCP) vs self-hosted?

### 11.2 Areas for Exploration
1. **Legal Compliance**: Formal legal review of scraping practices
2. **Image Analysis**: OCR and computer vision for listing images
3. **NLP Applications**: Sentiment analysis of job descriptions
4. **Competitive Analysis**: Benchmarking against similar tools
5. **Monetization**: Freemium model, API access pricing, B2B licensing

---

## 12. Appendix

### 12.1 Glossary
- **Finnkode**: Unique identifier for FINN.no listings
- **BRA**: Bruksareal (usable area in Norwegian property terms)
- **Eiendom**: Real estate / property
- **Flippe**: Rental / leased property
- **Jobbe**: Job / employment
- **MVV**: Market value calculation (referenced in temp scripts)

### 12.2 Related Documentation
- `main/config/requirements.txt`: Python dependencies
- `main/config/setup.py`: Installation instructions
- Database schema: See `database/db.py` table definitions
- API documentation: Google Sheets API, Google Maps API

### 12.3 Contact & Ownership
- **Project Repository**: `/Users/tehbaer/kode/skannonser`
- **Database Location**: `main/database/properties.db`
- **Credentials**: `main/config/credentials.json`, `main/config/token.json`

---

## Document Change Log
| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-02-09 | GitHub Copilot | Initial PRD creation based on codebase analysis |

---

**END OF DOCUMENT**
