import os
import time
from urllib import request

import boto3
from prefect import task, Flow, Parameter
import prefect
import praw
import datetime
from urllib.parse import urlparse

from prefect.executors import LocalDaskExecutor
from prefect.schedules import IntervalSchedule
from prefect.utilities.edges import unmapped
from slugify import slugify
from prefect.tasks.control_flow import FilterTask
from .imgur_wrapper import ImgurWrapper
from PIL import Image


def get_suffix(imagename):
    return imagename[imagename.rfind("."):]


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=2))
def get_submissions(subreddit, how_many=50):
    """
    Pull the hot wallpaper entries from reddit
    """
    reddit = praw.Reddit(
        client_id=prefect.context.secrets['reddit_client_id'],
        client_secret=prefect.context.secrets['reddit_client_secret'],
        user_agent=prefect.context.secrets["reddit_user_agent"]
    )
    subreddit = reddit.subreddit(subreddit)

    return [submission for submission in subreddit.hot(limit=how_many)]

@task
def check_duplicates(submissions, keys):
    """
    Takes the submissions and the keys to filter out existing ones in the s3 bucket
    """
    return [submission for submission in submissions if submission.name not in keys]


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=2))
def pull_keys(bucket):
    """
    Pulls all keys in the wallpapers bucket
    """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    return [key.key for key in bucket.objects.all()]


@task
def get_file_data(submission):
    """
    Takes submission and returns a list of (url, name) pairs.
    This task should probably be broken up into someething that sorts the different image download types
    into different path branches.
    """
    logger = prefect.context.get("logger")  # Prefect comes with a logger in the context
    image_suffixes = (".jpg", ".png", ".gif")
    url = urlparse(submission.url)
    data_list = []

    if url.path.endswith(image_suffixes):
        # Plain ol images urls
        name = f"{slugify(submission.title)}-{submission.name}{get_suffix(submission.url)}"
        logger.info(f"Normal image found: {name}")
        data_list.append((name, submission.url))
    elif ImgurWrapper.is_imgur(url):
        # Now create the imgur client
        logger.info("Found imgur url")
        imgur_wrapper = ImgurWrapper(prefect.context.secrets['imgur_client_id'], prefect.context.secrets['imgur_client_secret'])

        # Gets the image url or a series of them from a gallery on imgur
        images = imgur_wrapper.get_image_list(url)
        for image in images:
            # Title is blank back up to reddit title
            if image.title is None:
                name = f"{slugify(submission.title)}-{image.id}{get_suffix(image.link)}"
            else:
                name = f"{slugify(image.title)}-{image.id}{get_suffix(image.link)}"
            data_list.append((name, submission.url))
    else:
        # New formats that will come later
        logger.info(f"Found unknown url: {submission.url}")
    return data_list


filter_blank = FilterTask(filter_func=lambda x: bool(x))

@task
def download_image(save_data, min_height=900, min_width=1400):
    """
    Take the name and url to save the files and filter for minimum
    """
    logger = prefect.context.get("logger")
    (name, url) = save_data[0] # strangely prefect sends a list of 1 tuple
    try:
        logger.info(f"Downloading {url}")
        request.urlretrieve(url, name)
    except Exception as e:
        logger.error(e)
        return
    else:
        with open(name, "rb") as f:
            img = Image.open(f)
            (width, height) = img.size

            if width < min_width or height < min_height:
                logger.info("File saved then removed due to size:" + name)
                os.remove(name)
        return name

@task
def send_to_s3_and_remove(bucket, folder, name):
    """
    Take the downloaded file and push up to s3 in the images directory then remove local copy
    """
    s3 = boto3.resource('s3')
    s3.Object(bucket, f"{folder}/{name}").put(Body=open(name, 'rb'))

    os.remove(name)

@task
def store_submissions(bucket, submission):
    """
    Used to store the submission in a bucket for checking
    """
    s3 = boto3.resource('s3')
    s3.Object(bucket, submission.name).put(Body=str(submission))


schedule = IntervalSchedule(interval=datetime.timedelta(minutes=15))
with Flow("Reddit wallpaper fetcher", executor=LocalDaskExecutor(), schedule=schedule) as flow:
    subreddit = Parameter("subreddit", default="wallpapers")
    bucket = Parameter("bucket", default="prefect-wallpapers")
    s3_image_folder = Parameter("s3_image_folder", default="images")

    submissions = get_submissions(subreddit)
    existing_keys = pull_keys(bucket)
    unique = check_duplicates(submissions, existing_keys)
    image_data = filter_blank(get_file_data.map(unique))

    # download, filter and then send to s3
    downloaded = filter_blank(download_image.map(image_data))
    send_to_s3_and_remove.map(unmapped(bucket), unmapped(s3_image_folder), downloaded)

    # finally add the submission data to the s3 bucket
    final = store_submissions.map(unmapped(bucket), unique)


flow.register("Testing")
# flow.visualize()
# start = time.time()
# state = flow.run()
# print(f"Flow took: {time.time()-start} seconds")


