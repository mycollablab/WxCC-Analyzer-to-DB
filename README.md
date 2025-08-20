# Webex Contact Center Analyzer (GraphQL) to SQL (SQLite) Data Extractors

This project provides two implementations of a data extraction tool for Webex Contact Center Reporting/Analyzer - one in Python and one in Node.js. Both scripts connect to the Webex Contact Center SearchAPI to extract comprehensive contact center data and store it in SQLite databases. It is based on the Cisco Java app published for shipping Analyzer data to Power BI and has been simplified for easier understanding: https://github.com/WebexSamples/webex-contact-center-api-samples/tree/main/reporting-samples/graphql-powerbi-sample

## Files

| File | Language | Description |
|------|----------|-------------|
| `wxcc_graphql_sqlite.py` | Python | Python implementation |
| `wxcc_graphql_sqlite.js` | Node.js | Node.js implementation |

## Purpose

Both scripts perform the same core functions:

1. **Connect to Webex Contact Center SearchAPI** using OAuth2 authentication
2. **Extract comprehensive contact center data** including:
   - Task records (contact interactions)
   - Task activities (detailed activity logs)
   - Agent sessions (login/logout sessions)
   - Agent activities (agent-specific activities)
   - Task aggregations (performance metrics)
3. **Store data in SQLite database** with structured tables and raw JSON preservation
4. **Provide detailed logging** and error handling

## Prerequisites

### For Python Version (`wxcc_graphql_sqlite.py`)
- Python 3.7+
- Required packages: `requests`, `sqlite3` (built-in)
- Install with: `pip install requests`

### For Node.js Version (`wxcc_graphql_sqlite.js`)
- Node.js 14.0+
- Required packages: `axios`, `sqlite3`
- Install with: `npm install axios sqlite3`

### For Both Versions
- Webex Contact Center access token
- Access to Webex CC SearchAPI

## Configuration

### Python Version
Edit `wxcc_graphql_sqlite.py` and update these lines:
```python
ACCESS_TOKEN = 'YOUR_ACCESS_TOKEN_HERE'
CONFIG = {
    'base_url': 'https://api.wxcc-us1.cisco.com',  # Your data center URL
    'access_token': ACCESS_TOKEN,
    'org_id': ACCESS_TOKEN.split("_")[-1],  # add org ID if not part of the token
    'db_path': 'webex_cc_data.db',
    'days_back': 7  # Number of days to extract
}
```

### Node.js Version
Edit `wxcc_graphql_sqlite.js` and update these lines:
```javascript
const ACCESS_TOKEN = 'YOUR_ACCESS_TOKEN_HERE';
const CONFIG = {
    base_url: 'https://api.wxcc-us1.cisco.com',  // Your data center URL
    access_token: ACCESS_TOKEN,
    org_id: ACCESS_TOKEN.split("_").pop(),  // add org ID if not part of the token
    db_path: 'webex_cc_data.db',
    days_back: 7  // Number of days to extract
};
```

## Usage

### Python Version
```bash
python wxcc_graphql_sqlite.py
```

### Node.js Version
```bash
node wxcc_graphql_sqlite.js
```

## Process Flow

Both scripts follow the same systematic process:

### 1. Initialization
- Validate configuration
- Initialize GraphQL client with authentication
- Create/connect to SQLite database
- Create database tables if they don't exist

### 2. Data Extraction (Sequential)

#### Step 1: Task Data
- Execute GraphQL query for `taskDetails`
- Extract task records with activity details
- Store in `tasks` and `task_activities` tables
- Log progress and record counts

#### Step 2: Agent Sessions
- Execute GraphQL query for `agentSession`
- Extract agent session records with channel info
- Store in `agent_sessions` table
- Extract and store agent activities in `agent_activities` table
- Log progress and record counts

#### Step 3: Task Aggregations
- Execute GraphQL query with aggregation parameters
- Extract performance metrics and statistics
- Store in `task_aggregations` table
- Log progress and record counts

### 3. Database Storage
- **Tasks Table**: Main task records with raw JSON
- **Task Activities Table**: Detailed activity logs (40+ fields)
- **Agent Sessions Table**: Session records with channel info
- **Agent Activities Table**: Agent-specific activity details
- **Task Aggregations Table**: Performance metrics and summaries

### 4. Error Handling
- Network connectivity issues
- GraphQL query errors
- Database operation failures
- Configuration validation
- Comprehensive logging at each step

## Database Schema

Both scripts create identical database structures:

```sql
-- Main task records
CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_data TEXT
);

-- Detailed task activities
CREATE TABLE task_activities (
    id TEXT PRIMARY KEY,
    task_id TEXT,
    is_active BOOLEAN,
    created_time INTEGER,
    ended_time INTEGER,
    agent_id TEXT,
    agent_name TEXT,
    -- ... 40+ additional fields
    raw_data TEXT,
    FOREIGN KEY (task_id) REFERENCES tasks (id)
);

-- Agent sessions
CREATE TABLE agent_sessions (
    agent_session_id TEXT PRIMARY KEY,
    agent_id TEXT,
    agent_name TEXT,
    user_login_id TEXT,
    site_id TEXT,
    site_name TEXT,
    team_id TEXT,
    team_name TEXT,
    channel_id TEXT,
    channel_type TEXT,
    agent_phone_number TEXT,
    sub_channel_type TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_data TEXT
);

-- Agent activities
CREATE TABLE agent_activities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_session_id TEXT,
    agent_id TEXT,
    start_time INTEGER,
    end_time INTEGER,
    duration INTEGER,
    state TEXT,
    -- ... additional fields
    raw_data TEXT,
    FOREIGN KEY (agent_session_id) REFERENCES agent_sessions (agent_session_id)
);

-- Task aggregations
CREATE TABLE task_aggregations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query_name TEXT,
    aggregation_name TEXT,
    aggregation_value REAL,
    time_start INTEGER,
    time_end INTEGER,
    group_by_field TEXT,
    group_by_value TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Output

Both scripts produce:
- **SQLite database file** (`webex_cc_data.db` by default)
- **Console logging** with timestamps and progress information
- **Error messages** for troubleshooting
- **Success confirmation** with record counts

## Key Differences

| Aspect | Python Version | Node.js Version |
|--------|----------------|-----------------|
| **Dependencies** | `requests` library | `axios`, `sqlite3` |
| **Async Handling** | Synchronous | Async/await |
| **Error Handling** | Try/catch blocks | Promise rejection |
| **Database** | Built-in `sqlite3` | `sqlite3` npm package |
| **HTTP Client** | `requests` | `axios` |

## Troubleshooting

### Common Issues

1. **400 Bad Request Error**
   - Verify access token is valid
   - Check data center URL is correct
   - Ensure proper GraphQL query structure

2. **Authentication Errors**
   - Validate OAuth2 token format
   - Check token expiration
   - Verify organization ID extraction

3. **Database Errors**
   - Check file permissions
   - Ensure sufficient disk space
   - Delete existing database file if schema conflicts

4. **Network Errors**
   - Verify internet connectivity
   - Check firewall settings
   - Validate API endpoint accessibility

### Debug Mode

**Python:**
```python
logging.basicConfig(level=logging.DEBUG)
```

**Node.js:**
```javascript
// Add debug logging
function log(level, message) {
    if (level === 'debug' || process.env.DEBUG) {
        console.log(`${timestamp} - ${level.toUpperCase()} - ${message}`);
    }
}
```

## Performance Notes

- **Data Volume**: Both scripts handle large datasets efficiently
- **Memory Usage**: Node.js version may use more memory due to async operations
- **Speed**: Python version may be slightly faster for large datasets
- **Reliability**: Both versions include comprehensive error handling

## Security Considerations

- Store access tokens securely
- Use environment variables for sensitive data
- Validate all input data
- Implement proper error handling to avoid information leakage

## Support

For issues:
1. Check the troubleshooting section
2. Verify your configuration
3. Review console output for error details
4. Check Webex CC API documentation for token/endpoint issues 
