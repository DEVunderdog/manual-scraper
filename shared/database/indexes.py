from pymongo import ASCENDING, DESCENDING, IndexModel

IndexDefinition = dict[str, list[IndexModel]]


def get_all_indexes() -> IndexDefinition:
    return {
        **get_api_indexes(),
        **get_worker_indexes(),
        **get_shared_indexes(),
    }


def get_api_indexes() -> IndexDefinition:
    return {
        "tasks": [
            IndexModel(
                [("user_id", ASCENDING), ("status", ASCENDING)],
                name="tasks_user_status_idx",
            ),
            IndexModel(
                [("user_id", ASCENDING), ("created_at", DESCENDING)],
                name="tasks_user_created_idx",
            ),
            IndexModel(
                [("status", ASCENDING), ("created_at", DESCENDING)],
                name="tasks_status_created_idx",
            ),
            IndexModel(
                [("celery_task_id", ASCENDING)],
                name="tasks_celery_task_id_idx",
                sparse=True,
            ),
            IndexModel(
                [("site", ASCENDING), ("status", ASCENDING)],
                name="tasks_site_status_idx",
            ),
            IndexModel(
                [
                    ("status", ASCENDING),
                    ("priority", DESCENDING),
                    ("created_at", ASCENDING),
                ],
                name="tasks_queue_order_idx",
            ),
        ],
        "api_keys": [
            IndexModel(
                [("key_credential", ASCENDING)],
                name="api_keys_credential_idx",
                unique=True,
            ),
            IndexModel(
                [("user_id", ASCENDING), ("is_active", ASCENDING)],
                name="api_keys_user_active_idx",
            ),
            IndexModel(
                [("key_id", ASCENDING)],
                name="api_keys_key_id_idx",
            ),
        ],
        "users": [
            IndexModel(
                [("email", ASCENDING)],
                name="users_email_idx",
                unique=True,
            ),
            IndexModel(
                [("is_active", ASCENDING), ("role", ASCENDING)],
                name="users_active_role_idx",
            ),
        ],
        "encryption_keys": [
            IndexModel(
                [("is_active", ASCENDING)],
                name="encryption_keys_active_idx",
            ),
            IndexModel(
                [("expired_at", ASCENDING)],
                name="encryption_keys_expiry_idx",
            ),
        ],
        "settings": [
            IndexModel(
                [("key", ASCENDING)],
                name="settings_key_idx",
                unique=True,
            ),
        ],
        "activity_logs": [
            IndexModel(
                [("created_at", DESCENDING)],
                name="activity_logs_created_idx",
            ),
            IndexModel(
                [("user_email", ASCENDING), ("created_at", DESCENDING)],
                name="activity_logs_user_created_idx",
            ),
            IndexModel(
                [("activity_type", ASCENDING), ("created_at", DESCENDING)],
                name="activity_logs_type_created_idx",
            ),
            IndexModel(
                [("created_at", ASCENDING)],
                name="activity_logs_ttl_idx",
                expireAfterSeconds=365 * 24 * 60 * 60,  # 365 days TTL
            ),
        ],
    }


def get_worker_indexes() -> IndexDefinition:
    return {
        "scrape_results": [
            IndexModel(
                [("task_id", ASCENDING)],
                name="scrape_results_task_id_idx",
                unique=True,
            ),
            IndexModel(
                [("site", ASCENDING), ("created_at", DESCENDING)],
                name="scrape_results_site_created_idx",
            ),
            IndexModel(
                [("site", ASCENDING), ("url", ASCENDING)],
                name="scrape_results_site_url_idx",
            ),
        ],
        "scrape_errors": [
            IndexModel(
                [("task_id", ASCENDING), ("created_at", DESCENDING)],
                name="scrape_errors_task_created_idx",
            ),
            IndexModel(
                [("site", ASCENDING), ("created_at", DESCENDING)],
                name="scrape_errors_site_created_idx",
            ),
            IndexModel(
                [("task_id", ASCENDING), ("retry_count", ASCENDING)],
                name="scrape_errors_task_retry_idx",
            ),
        ],
    }


def get_shared_indexes() -> IndexDefinition:
    return {
        "scrapers": [
            IndexModel(
                [("site_id", ASCENDING)],
                name="scrapers_site_id_idx",
                unique=True,
            ),
            IndexModel(
                [("status", ASCENDING)],
                name="scrapers_status_idx",
            ),
            IndexModel(
                [("metadata.tags", ASCENDING)],
                name="scrapers_tags_idx",
                sparse=True,
            ),
        ],
    }
