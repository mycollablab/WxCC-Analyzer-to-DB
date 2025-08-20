#!/usr/bin/env node
/**
 * Webex Contact Center GraphQL to SQLite Database Script (Node.js)
 * 
 * This script executes GraphQL queries against the Webex Contact Center Search API
 * and stores the results in a SQLite database.
 */

const sqlite3 = require('sqlite3').verbose();
const axios = require('axios');
const fs = require('fs');
const path = require('path');

// Configuration - UPDATE THESE VALUES
const ACCESS_TOKEN = 'ACCESS_TOKEN_HERE';
const CONFIG = {
    base_url: 'https://api.wxcc-us1.cisco.com',  // Change to your data center URL
    access_token: ACCESS_TOKEN,      // Your OAuth2 access token
    org_id: ACCESS_TOKEN.split("_").pop(),                  // Your organization ID
    db_path: 'webex_cc_data.db',                 // SQLite database file path
    days_back: 7                                 // Number of days to extract data for
};

// Simple logging
function log(level, message) {
    const timestamp = new Date().toISOString().replace('T', ' ').substr(0, 19);
    console.log(`${timestamp} - ${level.toUpperCase()} - ${message}`);
}

class WebexCCGraphQLClient {
    constructor(baseUrl, accessToken, orgId) {
        this.baseUrl = baseUrl.replace(/\/$/, '');
        this.accessToken = accessToken;
        this.orgId = orgId;
        this.searchUrl = `${this.baseUrl}/search`;
        this.headers = {
            'Authorization': `Bearer ${accessToken}`,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        };
    }

    async executeQuery(query, variables = null) {
        const payload = {
            query: query,
            variables: variables || {}
        };

        log('info', `Executing GraphQL query: ${query.substring(0, 100)}...`);

        try {
            const response = await axios.post(this.searchUrl, payload, {
                headers: this.headers,
                timeout: 30000
            });

            const result = response.data;

            if (result.errors) {
                log('error', `GraphQL errors: ${JSON.stringify(result.errors)}`);
                throw new Error(`GraphQL query failed: ${JSON.stringify(result.errors)}`);
            }

            return result.data || {};
        } catch (error) {
            if (error.response) {
                log('error', `HTTP request failed: ${error.response.status} ${error.response.statusText} for url: ${error.config.url}`);
            } else {
                log('error', `Query execution failed: ${error.message}`);
            }
            throw error;
        }
    }
}

class SQLiteManager {
    constructor(dbPath) {
        this.dbPath = dbPath;
        this.db = null;
    }

    async initialize() {
        return new Promise((resolve, reject) => {
            this.db = new sqlite3.Database(this.dbPath, (err) => {
                if (err) {
                    reject(err);
                } else {
                    this.createTables().then(resolve).catch(reject);
                }
            });
        });
    }

    async createTables() {
        const createTablesQueries = [
            // Tasks table
            `CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                raw_data TEXT
            )`,
            
            // Task Activities table
            `CREATE TABLE IF NOT EXISTS task_activities (
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
            )`,
            
            // Agent Sessions table
            `CREATE TABLE IF NOT EXISTS agent_sessions (
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
            )`,
            
            // Agent Activities table
            `CREATE TABLE IF NOT EXISTS agent_activities (
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
            )`,
            
            // Task Aggregations table
            `CREATE TABLE IF NOT EXISTS task_aggregations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query_name TEXT,
                aggregation_name TEXT,
                aggregation_value REAL,
                time_start INTEGER,
                time_end INTEGER,
                group_by_field TEXT,
                group_by_value TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`
        ];

        for (const query of createTablesQueries) {
            await this.runQuery(query);
        }
        
        log('info', 'Database tables created/verified');
    }

    async runQuery(sql, params = []) {
        return new Promise((resolve, reject) => {
            this.db.run(sql, params, function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this);
                }
            });
        });
    }

    async insertTasks(tasksData) {
        for (const task of tasksData) {
            // Insert the main task record
            await this.runQuery(
                'INSERT OR REPLACE INTO tasks (id, raw_data) VALUES (?, ?)',
                [task.id, JSON.stringify(task)]
            );

            // Insert task activities
            if (task.activities && task.activities.nodes) {
                for (const activity of task.activities.nodes) {
                    await this.runQuery(`
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
                    `, [
                        activity.id,
                        task.id,
                        activity.isActive,
                        activity.createdTime,
                        activity.endedTime,
                        activity.agentId,
                        activity.agentName,
                        activity.agentPhoneNumber,
                        activity.agentSessionId,
                        activity.agentChannelId,
                        activity.entrypointId,
                        activity.entrypointName,
                        activity.queueId,
                        activity.queueName,
                        activity.siteId,
                        activity.siteName,
                        activity.teamId,
                        activity.teamName,
                        activity.transferType,
                        activity.activityType,
                        activity.activityName,
                        activity.eventName,
                        activity.previousState,
                        activity.nextState,
                        activity.consultEpId,
                        activity.consultEpName,
                        activity.childContactId,
                        activity.childContactType,
                        activity.duration,
                        activity.destinationAgentPhoneNumber,
                        activity.destinationAgentId,
                        activity.destinationAgentName,
                        activity.destinationAgentSessionId,
                        activity.destinationAgentChannelId,
                        activity.destinationAgentTeamId,
                        activity.destinationAgentTeamName,
                        activity.destinationQueueName,
                        activity.destinationQueueId,
                        activity.terminationReason,
                        activity.ivrScriptId,
                        activity.ivrScriptName,
                        activity.ivrScriptTagId,
                        activity.ivrScriptTagName,
                        activity.lastActivityTime,
                        activity.skillsAssignedIn,
                        null, // created_at will use DEFAULT CURRENT_TIMESTAMP
                        JSON.stringify(activity)
                    ]);
                }
            }
        }
        
        log('info', `Inserted ${tasksData.length} task records`);
    }

    async insertAgentSessions(sessionsData) {
        for (const session of sessionsData) {
            // Get the first channel info if it's a list
            let channelInfo = session.channelInfo || [];
            if (Array.isArray(channelInfo) && channelInfo.length > 0) {
                channelInfo = channelInfo[0];
            } else if (!channelInfo || typeof channelInfo !== 'object') {
                channelInfo = {};
            }

            await this.runQuery(`
                INSERT OR REPLACE INTO agent_sessions (
                    agent_session_id, agent_id, agent_name, user_login_id, site_id, site_name,
                    team_id, team_name, channel_id, channel_type, agent_phone_number, sub_channel_type, raw_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `, [
                session.agentSessionId,
                session.agentId,
                session.agentName,
                session.userLoginId,
                session.siteId,
                session.siteName,
                session.teamId,
                session.teamName,
                channelInfo.channelId,
                channelInfo.channelType,
                channelInfo.agentPhoneNumber,
                channelInfo.subChannelType,
                JSON.stringify(session)
            ]);

            // Insert activities if present
            if (channelInfo.activities && channelInfo.activities.nodes) {
                await this.insertAgentActivities(session.agentSessionId, channelInfo.activities.nodes);
            }
        }
        
        log('info', `Inserted ${sessionsData.length} agent session records`);
    }

    async insertAgentActivities(agentSessionId, activitiesData) {
        for (const activity of activitiesData) {
            await this.runQuery(`
                INSERT INTO agent_activities (
                    agent_session_id, agent_id, start_time, end_time, duration, state,
                    idle_code_id, idle_code_name, task_id, queue_id, queue_name,
                    wrapup_code_id, wrapup_code_name, is_outdial, outbound_type,
                    is_current_activity, is_login_activity, is_logout_activity,
                    changed_by_id, changed_by_name, raw_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `, [
                agentSessionId,
                activity.agentId,
                activity.startTime,
                activity.endTime,
                activity.duration,
                activity.state,
                activity.idleCode?.id,
                activity.idleCode?.name,
                activity.taskId,
                activity.queue?.id,
                activity.queue?.name,
                activity.wrapupCode?.id,
                activity.wrapupCode?.name,
                activity.isOutdial,
                activity.outboundType,
                activity.isCurrentActivity,
                activity.isLoginActivity,
                activity.isLogoutActivity,
                activity.changedById,
                activity.changedByName,
                JSON.stringify(activity)
            ]);
        }
    }

    async insertAggregations(queryName, aggregations, timeStart, timeEnd, groupByData = null) {
        for (const aggregation of aggregations) {
            await this.runQuery(`
                INSERT INTO task_aggregations (
                    query_name, aggregation_name, aggregation_value, time_start, time_end,
                    group_by_field, group_by_value
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            `, [
                queryName,
                aggregation.name,
                aggregation.value,
                timeStart,
                timeEnd,
                groupByData?.field || null,
                groupByData?.value || null
            ]);
        }
    }

    close() {
        if (this.db) {
            this.db.close();
        }
    }
}

class WebexCCDataExtractor {
    constructor(client, dbManager) {
        this.client = client;
        this.db = dbManager;
    }

    getTimeRange(daysBack = 7) {
        const endTime = new Date();
        const startTime = new Date(endTime.getTime() - (daysBack * 24 * 60 * 60 * 1000));
        
        const endEpoch = Math.floor(endTime.getTime());
        const startEpoch = Math.floor(startTime.getTime());
        
        return [startEpoch, endEpoch];
    }

    async extractTasks(daysBack = 7) {
        const [startTime, endTime] = this.getTimeRange(daysBack);
        
        const query = `
        {
            taskDetails(from: ${startTime}, to: ${endTime}) {
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
        `;

        const result = await this.client.executeQuery(query);
        
        if (result.taskDetails && result.taskDetails.tasks) {
            const tasks = result.taskDetails.tasks;
            log('info', `Retrieved ${tasks.length} tasks`);
            
            if (tasks.length > 0) {
                await this.db.insertTasks(tasks);
            }
        }
        
        return result.taskDetails?.tasks?.length || 0;
    }

    async extractAgentSessions(daysBack = 7) {
        const [startTime, endTime] = this.getTimeRange(daysBack);
        
        const query = `
        {
            agentSession(from: ${startTime}, to: ${endTime}) {
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
        `;

        const result = await this.client.executeQuery(query);
        
        if (result.agentSession && result.agentSession.agentSessions) {
            const sessions = result.agentSession.agentSessions;
            log('info', `Retrieved ${sessions.length} agent sessions`);
            
            if (sessions.length > 0) {
                await this.db.insertAgentSessions(sessions);
            }
        }
        
        return result.agentSession?.agentSessions?.length || 0;
    }

    async extractTaskAggregations(daysBack = 7) {
        const [startTime, endTime] = this.getTimeRange(daysBack);
        
        const query = `
        {
            taskDetails(
                from: ${startTime},
                to: ${endTime},
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
        `;

        const result = await this.client.executeQuery(query);
        
        if (result.taskDetails && result.taskDetails.tasks) {
            const tasks = result.taskDetails.tasks;
            
            for (const task of tasks) {
                if (task.aggregation) {
                    const groupByData = {
                        field: 'owner_id',
                        value: task.owner?.id
                    };
                    await this.db.insertAggregations(
                        'task_statistics_by_agent',
                        [task.aggregation],
                        startTime,
                        endTime,
                        groupByData
                    );
                }
            }
            
            log('info', `Inserted aggregations for ${tasks.length} agents`);
        }
        
        return result.taskDetails?.tasks?.length || 0;
    }
}

async function main() {
    let dbManager = null;
    
    try {
        // Initialize clients
        log('info', 'Initializing Webex CC GraphQL client and SQLite database');
        const client = new WebexCCGraphQLClient(
            CONFIG.base_url,
            CONFIG.access_token,
            CONFIG.org_id
        );
        
        dbManager = new SQLiteManager(CONFIG.db_path);
        await dbManager.initialize();
        
        const extractor = new WebexCCDataExtractor(client, dbManager);
        
        // Extract data
        log('info', `Starting data extraction for last ${CONFIG.days_back} days`);
        
        // Extract tasks
        log('info', 'Extracting task data...');
        const taskCount = await extractor.extractTasks(CONFIG.days_back);
        log('info', `Extracted ${taskCount} tasks`);
        
        // Extract agent sessions
        log('info', 'Extracting agent session data...');
        const sessionCount = await extractor.extractAgentSessions(CONFIG.days_back);
        log('info', `Extracted ${sessionCount} agent sessions`);
        
        // Extract aggregations
        log('info', 'Extracting task aggregations...');
        const aggCount = await extractor.extractTaskAggregations(CONFIG.days_back);
        log('info', `Extracted aggregations for ${aggCount} agents`);
        
        log('info', 'Data extraction completed successfully!');
        log('info', `Database saved to: ${CONFIG.db_path}`);
        
    } catch (error) {
        log('error', `Data extraction failed: ${error.message}`);
        throw error;
    } finally {
        if (dbManager) {
            dbManager.close();
        }
    }
}

// Run the script
if (require.main === module) {
    main().catch(error => {
        console.error('Script failed:', error);
        process.exit(1);
    });
}

module.exports = {
    WebexCCGraphQLClient,
    SQLiteManager,
    WebexCCDataExtractor,
    CONFIG
};
