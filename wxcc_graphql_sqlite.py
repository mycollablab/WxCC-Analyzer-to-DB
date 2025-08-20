#!/usr/bin/env python3
"""
Webex Contact Center GraphQL to SQLite Database Script

This script executes GraphQL queries against the Webex Contact Center Search API
and stores the results in a SQLite database.
"""

import sqlite3
import requests
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration - UPDATE THESE VALUES
ACCESS_TOKEN = 'ACCESS_TOKEN_HERE'
CONFIG = {
    'base_url': 'https://api.wxcc-us1.cisco.com',  # Change to your data center URL
    'access_token': ACCESS_TOKEN,      # Your OAuth2 access token
    'org_id': ACCESS_TOKEN.split("_")[-1],                  # Your organization ID
    'db_path': 'webex_cc_data.db',                 # SQLite database file path
    'days_back': 7                                 # Number of days to extract data for
}

class WebexCCGraphQLClient:
    def __init__(self, base_url: str, access_token: str, org_id: str):
        """
        Initialize the Webex Contact Center GraphQL client
        
        Args:
            base_url: The Webex CC API base URL (e.g., https://api.wxcc-us1.cisco.com)
            access_token: OAuth2 access token
            org_id: Organization ID
        """
        self.base_url = base_url.rstrip('/')
        self.access_token = access_token
        self.org_id = org_id
        self.search_url = f"{self.base_url}/search"
        self.headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
    def execute_query(self, query: str, variables: Optional[Dict] = None) -> Dict:
        """
        Execute a GraphQL query against the Webex CC Search API
        
        Args:
            query: GraphQL query string
            variables: Optional variables for the query
            
        Returns:
            Response data as dictionary
        """
        payload = {
            'query': query,
            'variables': variables or {}
        }
        
        logger.info(f"Executing GraphQL query: {query[:100]}...")
        
        try:
            response = requests.post(
                self.search_url,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            
            if 'errors' in result:
                logger.error(f"GraphQL errors: {result['errors']}")
                raise Exception(f"GraphQL query failed: {result['errors']}")
                
            return result.get('data', {})
            
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

class SQLiteManager:
    def __init__(self, db_path: str):
        """
        Initialize SQLite database manager
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.create_tables()
        
    def create_tables(self):
        """Create database tables for storing Webex CC data"""
        cursor = self.conn.cursor()
        
        # Tasks table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                raw_data TEXT
            )
        ''')
        
        # Task Activities table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS task_activities (
                id TEXT PRIMARY KEY,
                task_id TEXT,
                is_active BOOLEAN,
                created_time INTEGER,
                ended_time INTEGER,
                agent_id TEXT,
                agent_name TEXT,
                agent_phone_number TEXT,
                agent_session_id TEXT,
                agent_channel_id TEXT,
                entrypoint_id TEXT,
                entrypoint_name TEXT,
                queue_id TEXT,
                queue_name TEXT,
                site_id TEXT,
                site_name TEXT,
                team_id TEXT,
                team_name TEXT,
                transfer_type TEXT,
                activity_type TEXT,
                activity_name TEXT,
                event_name TEXT,
                previous_state TEXT,
                next_state TEXT,
                consult_ep_id TEXT,
                consult_ep_name TEXT,
                child_contact_id TEXT,
                child_contact_type TEXT,
                duration INTEGER,
                destination_agent_phone_number TEXT,
                destination_agent_id TEXT,
                destination_agent_name TEXT,
                destination_agent_session_id TEXT,
                destination_agent_channel_id TEXT,
                destination_agent_team_id TEXT,
                destination_agent_team_name TEXT,
                destination_queue_name TEXT,
                destination_queue_id TEXT,
                termination_reason TEXT,
                ivr_script_id TEXT,
                ivr_script_name TEXT,
                ivr_script_tag_id TEXT,
                ivr_script_tag_name TEXT,
                last_activity_time INTEGER,
                skills_assigned_in TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                raw_data TEXT,
                FOREIGN KEY (task_id) REFERENCES tasks (id)
            )
        ''')
        
        # Agent Sessions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS agent_sessions (
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
            )
        ''')
        
        # Agent Activities table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS agent_activities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_session_id TEXT,
                agent_id TEXT,
                start_time INTEGER,
                end_time INTEGER,
                duration INTEGER,
                state TEXT,
                idle_code_id TEXT,
                idle_code_name TEXT,
                task_id TEXT,
                queue_id TEXT,
                queue_name TEXT,
                wrapup_code_id TEXT,
                wrapup_code_name TEXT,
                is_outdial BOOLEAN,
                outbound_type TEXT,
                is_current_activity BOOLEAN,
                is_login_activity BOOLEAN,
                is_logout_activity BOOLEAN,
                changed_by_id TEXT,
                changed_by_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                raw_data TEXT,
                FOREIGN KEY (agent_session_id) REFERENCES agent_sessions (agent_session_id)
            )
        ''')
        
        # Task Aggregations table (for aggregated results)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS task_aggregations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query_name TEXT,
                aggregation_name TEXT,
                aggregation_value REAL,
                group_by_field TEXT,
                group_by_value TEXT,
                time_period_start INTEGER,
                time_period_end INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.conn.commit()
        logger.info("Database tables created/verified")
        
    def insert_tasks(self, tasks_data: List[Dict]):
        """Insert task data into the database"""
        cursor = self.conn.cursor()
        
        for task in tasks_data:
            # Insert the main task record
            cursor.execute('''
                INSERT OR REPLACE INTO tasks (id, raw_data)
                VALUES (?, ?)
            ''', (
                task.get('id'),
                json.dumps(task)
            ))
            
            # Insert task activities
            if 'activities' in task and 'nodes' in task['activities']:
                for activity in task['activities']['nodes']:
                    cursor.execute('''
                        INSERT OR REPLACE INTO task_activities (
                            id, task_id, is_active, created_time, ended_time, agent_id, agent_name,
                            agent_phone_number, agent_session_id, agent_channel_id, entrypoint_id,
                            entrypoint_name, queue_id, queue_name, site_id, site_name, team_id,
                            team_name, transfer_type, activity_type, activity_name, event_name,
                            previous_state, next_state, consult_ep_id, consult_ep_name,
                            child_contact_id, child_contact_type, duration,
                            destination_agent_phone_number, destination_agent_id, destination_agent_name,
                            destination_agent_session_id, destination_agent_channel_id,
                            destination_agent_team_id, destination_agent_team_name,
                            destination_queue_name, destination_queue_id, termination_reason,
                            ivr_script_id, ivr_script_name, ivr_script_tag_id, ivr_script_tag_name,
                            last_activity_time, skills_assigned_in, created_at, raw_data
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        activity.get('id'),
                        task.get('id'),
                        activity.get('isActive'),
                        activity.get('createdTime'),
                        activity.get('endedTime'),
                        activity.get('agentId'),
                        activity.get('agentName'),
                        activity.get('agentPhoneNumber'),
                        activity.get('agentSessionId'),
                        activity.get('agentChannelId'),
                        activity.get('entrypointId'),
                        activity.get('entrypointName'),
                        activity.get('queueId'),
                        activity.get('queueName'),
                        activity.get('siteId'),
                        activity.get('siteName'),
                        activity.get('teamId'),
                        activity.get('teamName'),
                        activity.get('transferType'),
                        activity.get('activityType'),
                        activity.get('activityName'),
                        activity.get('eventName'),
                        activity.get('previousState'),
                        activity.get('nextState'),
                        activity.get('consultEpId'),
                        activity.get('consultEpName'),
                        activity.get('childContactId'),
                        activity.get('childContactType'),
                        activity.get('duration'),
                        activity.get('destinationAgentPhoneNumber'),
                        activity.get('destinationAgentId'),
                        activity.get('destinationAgentName'),
                        activity.get('destinationAgentSessionId'),
                        activity.get('destinationAgentChannelId'),
                        activity.get('destinationAgentTeamId'),
                        activity.get('destinationAgentTeamName'),
                        activity.get('destinationQueueName'),
                        activity.get('destinationQueueId'),
                        activity.get('terminationReason'),
                        activity.get('ivrScriptId'),
                        activity.get('ivrScriptName'),
                        activity.get('ivrScriptTagId'),
                        activity.get('ivrScriptTagName'),
                        activity.get('lastActivityTime'),
                        activity.get('skillsAssignedIn'),
                        None,  # created_at will use DEFAULT CURRENT_TIMESTAMP
                        json.dumps(activity)
                    ))
        
        self.conn.commit()
        logger.info(f"Inserted {len(tasks_data)} task records")
        
    def insert_agent_sessions(self, sessions_data: List[Dict]):
        """Insert agent session data into the database"""
        cursor = self.conn.cursor()
        
        for session in sessions_data:
            # Get the first channel info if it's a list
            channel_info = session.get('channelInfo', [])
            if isinstance(channel_info, list) and len(channel_info) > 0:
                channel_info = channel_info[0]
            elif not isinstance(channel_info, dict):
                channel_info = {}
            
            cursor.execute('''
                INSERT OR REPLACE INTO agent_sessions (
                    agent_session_id, agent_id, agent_name, user_login_id, site_id, site_name,
                    team_id, team_name, channel_id, channel_type, agent_phone_number, sub_channel_type, raw_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                session.get('agentSessionId'),
                session.get('agentId'),
                session.get('agentName'),
                session.get('userLoginId'),
                session.get('siteId'),
                session.get('siteName'),
                session.get('teamId'),
                session.get('teamName'),
                channel_info.get('channelId'),
                channel_info.get('channelType'),
                channel_info.get('agentPhoneNumber'),
                channel_info.get('subChannelType'),
                json.dumps(session)
            ))
            
            # Insert activities if present
            if channel_info.get('activities', {}).get('nodes'):
                activities = channel_info['activities']['nodes']
                self.insert_agent_activities(session.get('agentSessionId'), activities)
        
        self.conn.commit()
        logger.info(f"Inserted {len(sessions_data)} agent session records")
        
    def insert_agent_activities(self, agent_session_id: str, activities_data: List[Dict]):
        """Insert agent activity data into the database"""
        cursor = self.conn.cursor()
        
        for activity in activities_data:
            cursor.execute('''
                INSERT INTO agent_activities (
                    agent_session_id, agent_id, start_time, end_time, duration, state,
                    idle_code_id, idle_code_name, task_id, queue_id, queue_name,
                    wrapup_code_id, wrapup_code_name, is_outdial, outbound_type,
                    is_current_activity, is_login_activity, is_logout_activity,
                    changed_by_id, changed_by_name, raw_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                agent_session_id,
                activity.get('agentId'),
                activity.get('startTime'),
                activity.get('endTime'),
                activity.get('duration'),
                activity.get('state'),
                activity.get('idleCode', {}).get('id') if activity.get('idleCode') else None,
                activity.get('idleCode', {}).get('name') if activity.get('idleCode') else None,
                activity.get('taskId'),
                activity.get('queue', {}).get('id') if activity.get('queue') else None,
                activity.get('queue', {}).get('name') if activity.get('queue') else None,
                activity.get('wrapupCode', {}).get('id') if activity.get('wrapupCode') else None,
                activity.get('wrapupCode', {}).get('name') if activity.get('wrapupCode') else None,
                activity.get('isOutdial'),
                activity.get('outboundType'),
                activity.get('isCurrentActivity'),
                activity.get('isLoginActivity'),
                activity.get('isLogoutActivity'),
                activity.get('changedById'),
                activity.get('changedByName'),
                json.dumps(activity)
            ))
        
        self.conn.commit()
        
    def insert_aggregations(self, query_name: str, aggregations: List[Dict], 
                          time_start: int, time_end: int, group_by_data: Dict = None):
        """Insert aggregation results into the database"""
        cursor = self.conn.cursor()
        
        for agg in aggregations:
            cursor.execute('''
                INSERT INTO task_aggregations (
                    query_name, aggregation_name, aggregation_value,
                    group_by_field, group_by_value, time_period_start, time_period_end
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                query_name,
                agg.get('name'),
                agg.get('value'),
                group_by_data.get('field') if group_by_data else None,
                group_by_data.get('value') if group_by_data else None,
                time_start,
                time_end
            ))
        
        self.conn.commit()
        
    def close(self):
        """Close database connection"""
        self.conn.close()

class WebexCCDataExtractor:
    def __init__(self, client: WebexCCGraphQLClient, db_manager: SQLiteManager):
        self.client = client
        self.db = db_manager
        
    def get_time_range(self, days_back: int = 7) -> tuple:
        """Get time range in epoch milliseconds"""
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days_back)
        
        end_epoch = int(end_time.timestamp() * 1000)
        start_epoch = int(start_time.timestamp() * 1000)
        
        return start_epoch, end_epoch
        
    def extract_tasks(self, days_back: int = 7):
        """Extract task data from Webex CC"""
        start_time, end_time = self.get_time_range(days_back)
        
        query = """
        {
            taskDetails(from: %d, to: %d) {
                tasks {
                    id
                    activities {
                        totalCount
                        nodes {
                            id
                            isActive
                            createdTime
                            endedTime
                            agentId
                            agentName
                            agentPhoneNumber
                            agentSessionId
                            agentChannelId
                            entrypointId
                            entrypointName
                            queueId
                            queueName
                            siteId
                            siteName
                            teamId
                            teamName
                            transferType
                            activityType
                            activityName
                            eventName
                            previousState
                            nextState
                            consultEpId
                            consultEpName
                            childContactId
                            childContactType
                            duration
                            destinationAgentPhoneNumber
                            destinationAgentId
                            destinationAgentName
                            destinationAgentSessionId
                            destinationAgentChannelId
                            destinationAgentTeamId
                            destinationAgentTeamName
                            destinationQueueName
                            destinationQueueId
                            terminationReason
                            ivrScriptId
                            ivrScriptName
                            ivrScriptTagId
                            ivrScriptTagName
                            lastActivityTime
                            skillsAssignedIn
                        }
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
        """ % (start_time, end_time)
        
        result = self.client.execute_query(query)
        
        if 'taskDetails' in result and 'tasks' in result['taskDetails']:
            tasks = result['taskDetails']['tasks']
            logger.info(f"Retrieved {len(tasks)} tasks")
            
            if tasks:
                self.db.insert_tasks(tasks)
                
        return len(tasks) if 'taskDetails' in result and 'tasks' in result['taskDetails'] else 0
        
    def extract_agent_sessions(self, days_back: int = 7):
        """Extract agent session data from Webex CC"""
        start_time, end_time = self.get_time_range(days_back)
        
        query = """
        {
            agentSession(from: %d, to: %d) {
                agentSessions {
                    agentSessionId
                    agentId
                    agentName
                    userLoginId
                    siteId
                    siteName
                    teamId
                    teamName
                    channelInfo {
                        channelId
                        channelType
                        agentPhoneNumber
                        subChannelType
                        activities {
                            nodes {
                                id 
                                startTime
                                endTime
                                duration
                                state
                                idleCode {
                                    id
                                    name
                                }
                                taskId
                                queue {
                                    id
                                    name
                                }
                                wrapupCode {
                                    id
                                    name
                                }
                                isOutdial
                                outboundType
                                isCurrentActivity
                                isLoginActivity
                                isLogoutActivity
                                changedById
                                changedByName
                            }
                        }
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
        """ % (start_time, end_time)
        
        result = self.client.execute_query(query)
        
        if 'agentSession' in result and 'agentSessions' in result['agentSession']:
            sessions = result['agentSession']['agentSessions']
            logger.info(f"Retrieved {len(sessions)} agent sessions")
            
            if sessions:
                self.db.insert_agent_sessions(sessions)
                
        return len(sessions) if 'agentSession' in result and 'agentSessions' in result['agentSession'] else 0
        
    def extract_task_aggregations(self, days_back: int = 7):
        """Extract task aggregation data from Webex CC"""
        start_time, end_time = self.get_time_range(days_back)
        
        query = """
        {
            taskDetails(
                from: %d,
                to: %d,
                filter: {
                    and: [
                        { direction: { equals: "inbound" } }
                        { channelType: { equals: telephony } }
                        { owner: { notequals: { id: null } } }
                    ]
                },
                aggregations: [
                    { field: "id", type: count, name: "Total Contacts Handled" }
                    { field: "connectedDuration", type: average, name: "Average Talk Time" }
                    { field: "holdDuration", type: max, name: "Maximum Hold Time" }
                    { field: "totalDuration", type: average, name: "Average Handle Time" }
                ]
            ) {
                tasks {
                    owner {
                        name
                        id
                    }
                    aggregation {
                        name
                        value
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
        """ % (start_time, end_time)
        
        result = self.client.execute_query(query)
        
        if 'taskDetails' in result and 'tasks' in result['taskDetails']:
            tasks = result['taskDetails']['tasks']
            
            for task in tasks:
                if 'aggregation' in task:
                    group_by_data = {
                        'field': 'owner_id',
                        'value': task.get('owner', {}).get('id')
                    }
                    self.db.insert_aggregations(
                        'task_statistics_by_agent',
                        task['aggregation'],
                        start_time,
                        end_time,
                        group_by_data
                    )
                    
            logger.info(f"Inserted aggregations for {len(tasks)} agents")
            
        return len(tasks) if 'taskDetails' in result and 'tasks' in result['taskDetails'] else 0

def main():
    """Main execution function"""
    try:
        # Initialize clients
        logger.info("Initializing Webex CC GraphQL client and SQLite database")
        client = WebexCCGraphQLClient(
            CONFIG['base_url'],
            CONFIG['access_token'],
            CONFIG['org_id']
        )
        
        db_manager = SQLiteManager(CONFIG['db_path'])
        extractor = WebexCCDataExtractor(client, db_manager)
        
        # Extract data
        logger.info(f"Starting data extraction for last {CONFIG['days_back']} days")
        
        # Extract tasks
        logger.info("Extracting task data...")
        task_count = extractor.extract_tasks(CONFIG['days_back'])
        logger.info(f"Extracted {task_count} tasks")
        
        # Extract agent sessions
        logger.info("Extracting agent session data...")
        session_count = extractor.extract_agent_sessions(CONFIG['days_back'])
        logger.info(f"Extracted {session_count} agent sessions")
        
        # Extract aggregations
        logger.info("Extracting task aggregations...")
        agg_count = extractor.extract_task_aggregations(CONFIG['days_back'])
        logger.info(f"Extracted aggregations for {agg_count} agents")
        
        logger.info(f"Data extraction completed successfully!")
        logger.info(f"Database saved to: {CONFIG['db_path']}")
        
    except Exception as e:
        logger.error(f"Data extraction failed: {e}")
        raise
    finally:
        if 'db_manager' in locals():
            db_manager.close()

if __name__ == "__main__":
    main()
