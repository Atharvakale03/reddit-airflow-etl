import logging
import os
import sqlite3
from datetime import datetime
from typing import Dict, Any, List

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

import praw

logger = logging.getLogger(__name__)

DB_PATH = "/opt/airflow/data/reddit_mentions.db"


@dag(
    dag_id="reddit_mentions",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["reddit", "etl", "free"],
)
def reddit_mentions():

    # ---------------------------------------------------------
    # Create SQLite table
    # ---------------------------------------------------------
    @task
    def create_table():

        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reddit_mentions (
                id TEXT PRIMARY KEY,
                title TEXT,
                author TEXT,
                subreddit TEXT,
                score INTEGER,
                num_comments INTEGER,
                created_utc INTEGER,
                fetched_at TEXT,
                selftext TEXT,
                permalink TEXT,
                priority TEXT
            )
        """)

        conn.commit()
        cursor.close()
        conn.close()

        logger.info("SQLite table created successfully.")

    # ---------------------------------------------------------
    # Fetch Reddit posts
    # ---------------------------------------------------------
    @task
    def fetch_posts() -> List[Dict[str, Any]]:

        client_id = os.getenv("REDDIT_CLIENT_ID")
        client_secret = os.getenv("REDDIT_CLIENT_SECRET")

        if not client_id or not client_secret:
            logger.warning("Reddit API credentials are missing.")
            return []

        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent="airflow_reddit_etl",
            check_for_async=False,
        )

        posts = []

        for submission in reddit.subreddit("dataengineering").new(limit=20):

            posts.append({
                "id": submission.id,
                "title": submission.title,
                "author": str(submission.author),
                "subreddit": str(submission.subreddit),
                "score": submission.score,
                "num_comments": submission.num_comments,
                "created_utc": int(submission.created_utc),
                "fetched_at": datetime.now().isoformat(),
                "selftext": submission.selftext or "",
                "permalink": f"https://reddit.com{submission.permalink}",
            })

        logger.info("Fetched %s Reddit posts.", len(posts))

        return posts

    # ---------------------------------------------------------
    # Check whether posts exist
    # ---------------------------------------------------------
    @task.branch
    def check_posts(posts):

        if posts:
            return "categorize"
        else:
            return "no_posts"

    # ---------------------------------------------------------
    # No posts
    # ---------------------------------------------------------
    @task
    def no_posts():

        logger.info("No Reddit posts found.")

    # ---------------------------------------------------------
    # Categorize posts
    # ---------------------------------------------------------
    @task
    def categorize(post: Dict[str, Any]) -> Dict[str, Any]:

        text = (
            post["title"] + " " + post["selftext"]
        ).lower()

        if any(
            keyword in text
            for keyword in ["error", "issue", "help", "problem"]
        ):
            return {
                "id": post["id"],
                "priority": "high",
                "should_respond": True,
            }

        elif any(
            keyword in text
            for keyword in ["how", "guide", "tutorial"]
        ):
            return {
                "id": post["id"],
                "priority": "medium",
                "should_respond": True,
            }

        return {
            "id": post["id"],
            "priority": "low",
            "should_respond": False,
        }

    # ---------------------------------------------------------
    # Filter useful posts
    # ---------------------------------------------------------
    @task
    def filter_posts(
        posts: List[Dict[str, Any]],
        categorizations: List[Dict[str, Any]],
    ):

        result = []

        for post, category in zip(posts, categorizations):

            if category["should_respond"]:

                post["priority"] = category["priority"]

                result.append(post)

        if not result:
            raise AirflowSkipException(
                "No useful Reddit posts found."
            )

        logger.info(
            "Filtered %s useful posts.",
            len(result)
        )

        return result

    # ---------------------------------------------------------
    # Store posts in SQLite
    # ---------------------------------------------------------
    @task
    def store_posts(posts):

        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        for post in posts:

            cursor.execute(
                """
                INSERT OR REPLACE INTO reddit_mentions (
                    id,
                    title,
                    author,
                    subreddit,
                    score,
                    num_comments,
                    created_utc,
                    fetched_at,
                    selftext,
                    permalink,
                    priority
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    post["id"],
                    post["title"],
                    post["author"],
                    post["subreddit"],
                    post["score"],
                    post["num_comments"],
                    post["created_utc"],
                    post["fetched_at"],
                    post["selftext"],
                    post["permalink"],
                    post.get("priority", "unknown"),
                ),
            )

        conn.commit()

        cursor.close()
        conn.close()

        logger.info(
            "Stored %s posts in SQLite.",
            len(posts)
        )

    # ---------------------------------------------------------
    # Slack notification
    # ---------------------------------------------------------
    @task
    def send_slack(posts):

        webhook_url = os.getenv("SLACK_WEBHOOK_URL")

        if not webhook_url:
            logger.info(
                "SLACK_WEBHOOK_URL not configured. "
                "Skipping Slack notification."
            )
            return

        import requests

        for post in posts:

            message = (
                f"🔔 {post['priority'].upper()} priority Reddit post\n\n"
                f"Title: {post['title']}\n"
                f"Subreddit: r/{post['subreddit']}\n"
                f"Score: {post['score']}\n"
                f"Comments: {post['num_comments']}\n"
                f"Link: {post['permalink']}"
            )

            try:

                response = requests.post(
                    webhook_url,
                    json={"text": message},
                    timeout=10,
                )

                response.raise_for_status()

                logger.info(
                    "Slack notification sent for %s",
                    post["id"]
                )

            except Exception as error:

                logger.error(
                    "Slack notification failed: %s",
                    error
                )


    # ---------------------------------------------------------
    # DAG workflow
    # ---------------------------------------------------------

    table = create_table()

    posts = fetch_posts()

    branch = check_posts(posts)

    no_post_task = no_posts()

    categorized = categorize.expand(post=posts)

    filtered = filter_posts(
        posts,
        categorized
    )

    store = store_posts(filtered)

    slack = send_slack(filtered)

    table >> posts >> branch

    branch >> no_post_task

    branch >> categorized

    categorized >> filtered

    filtered >> store

    store >> slack


reddit_mentions_dag = reddit_mentions()
