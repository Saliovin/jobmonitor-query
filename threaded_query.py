import requests
import concurrent.futures
import threading
import sqlite3
import os
import configparser
import logger


def write_to_db():
    logger.info("Start writing to SQLite.")
    conn = sqlite3.connect(f"{path}{os.sep}{config['default']['db']}")
    cursor = conn.cursor()
    sql_create_table = ''' CREATE TABLE IF NOT EXISTS jobs (
                                                    job_id text,
                                                    id integer,
                                                    app_name text,
                                                    state text,
                                                    date_created text
                                                ); '''
    sql_insert = ''' INSERT INTO jobs(job_id,id,app_name,state,date_created)
                          VALUES(:job_id,:id,:app_name,:state,:date_created); '''
    cursor.execute(sql_create_table)
    cursor.executemany(sql_insert, jobs)
    conn.commit()
    conn.close()
    logger.info("End writing to SQLite.")


def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session


def get_job(job_id):
    session = get_session()
    logger.info(f"Getting job {job_id}.")
    with session.get(f"{config['default']['api_url']}{job_id}/") as response:
        global jobs
        jobs += response.json()


def get_all_jobs(job_ids, threads):
    logger.info("Start getting jobs.")
    with concurrent.futures.ThreadPoolExecutor(max_workers=int(threads)) as executor:
        executor.map(get_job, job_ids)
    logger.info("End getting jobs.")


def main():
    with open(config['default']['text_file']) as file:
        logger.info("Reading text file.")
        job_ids = file.read().splitlines()
    get_all_jobs(job_ids, config['default']['threads'])
    write_to_db()


if __name__ == "__main__":
    config = configparser.ConfigParser()
    path = os.path.abspath(os.path.dirname(__file__))
    config.read(f"{path}{os.sep}config.ini")
    thread_local = threading.local()
    logger = logger.ini_logger(__name__)
    jobs = []

    main()
