from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["nimbus_events"]

collection = db["user_activity_logs"]

normalize_stage = {
    "$addFields": {
        "user_id": {
            "$ifNull": ["$member_id", {"$ifNull": ["$userId", "$userID"]}]
        },
        "customer_id_clean": {
            "$toString": {
                "$ifNull": ["$customer_id", "$customerId"]
            }
        },
        "timestamp_clean": {
            "$cond": {
                "if": {"$eq": [{"$type": "$timestamp"}, "string"]},
                "then": {"$toDate": "$timestamp"},
                "else": "$timestamp"
            }
        },
        "session_duration_sec": {
            "$ifNull": ["$session_duration_sec", 0]
        }
    }
}


#Q1
pipeline_q1 = [
    normalize_stage,

    {
        "$addFields": {
            "week": {"$week": "$timestamp_clean"}
        }
    },

    {
        "$group": {
            "_id": {
                "user": "$user_id",
                "week": "$week"
            },
            "sessions": {"$sum": 1},
            "durations": {"$push": "$session_duration_sec"}
        }
    },

    {
        "$group": {
            "_id": "$_id.user",
            "avg_sessions_per_week": {"$avg": "$sessions"},
            "all_durations": {"$push": "$durations"}
        }
    },

    {
        "$project": {
            "avg_sessions_per_week": 1,
            "p25": {"$percentile": {"input": "$all_durations", "p": [0.25]}},
            "p50": {"$percentile": {"input": "$all_durations", "p": [0.5]}},
            "p75": {"$percentile": {"input": "$all_durations", "p": [0.75]}}
        }
    }
]

result_q1 = list(collection.aggregate(pipeline_q1))

#Q2
pipeline_q2 = [
    normalize_stage,

    {
        "$match": {
            "feature": {"$exists": True}
        }
    },

    {
        "$addFields": {
            "date": {
                "$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp_clean"}
            }
        }
    },

    {
        "$group": {
            "_id": {
                "feature": "$feature",
                "date": "$date"
            },
            "users": {"$addToSet": "$user_id"}
        }
    },

    {
        "$project": {
            "feature": "$_id.feature",
            "date": "$_id.date",
            "DAU": {"$size": "$users"}
        }
    }
]

result_q2 = list(collection.aggregate(pipeline_q2))


#Q3
pipeline_q3 = [
    {
        "$match": {
            "event_type": {
                "$in": [
                    "signup",
                    "first_login",
                    "workspace_created",
                    "first_project",
                    "invited_teammate"
                ]
            }
        }
    },

    normalize_stage,

    {
        "$group": {
            "_id": "$user_id",
            "events": {
                "$push": {
                    "event": "$event_type",
                    "time": "$timestamp_clean"
                }
            }
        }
    }
]

result_q3 = list(collection.aggregate(pipeline_q3))


#Q4
pipeline_q4 = [
    normalize_stage,

    {
        "$group": {
            "_id": "$user_id",
            "sessions": {"$sum": 1},
            "feature_clicks": {
                "$sum": {
                    "$cond": [{"$eq": ["$event_type", "feature_click"]}, 1, 0]
                }
            },
            "total_duration": {"$sum": "$session_duration_sec"}
        }
    },

    {
        "$addFields": {
            "engagement_score": {
                "$add": [
                    "$sessions",
                    {"$multiply": ["$feature_clicks", 2]},
                    {"$divide": ["$total_duration", 100]}
                ]
            }
        }
    },

    {
        "$sort": {"engagement_score": -1}
    },

    {
        "$limit": 20
    }
]

result_q4 = list(collection.aggregate(pipeline_q4))

